package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

type sourceHealth struct {
	Name        string     `json:"name"`
	Pending     int        `json:"pending_tasks"`
	LastSuccess *time.Time `json:"last_success_at"`
}
type health struct {
	Version   int            `json:"schema_version"`
	Generated time.Time      `json:"generated_at"`
	Errors    []string       `json:"probe_errors"`
	Poll      int            `json:"poll_interval_seconds"`
	Cycle     *time.Time     `json:"last_cycle_at"`
	Sources   []sourceHealth `json:"sources"`
	Proxy     struct {
		Enabled bool     `json:"enabled"`
		Auth    bool     `json:"auth_blocked"`
		Budgets []string `json:"budget_guards"`
	} `json:"proxy"`
}
type progress struct {
	Since   time.Time `json:"since"`
	Success string    `json:"success"`
}
type incident struct {
	Since       time.Time `json:"since"`
	LastAttempt time.Time `json:"last_attempt"`
	Accepted    bool      `json:"accepted"`
	Summary     string    `json:"summary"`
}
type state struct {
	Version          int                 `json:"schema_version"`
	Incidents        map[string]incident `json:"incidents"`
	Progress         map[string]progress `json:"progress"`
	UnavailableSince *time.Time          `json:"unavailable_since"`
	TestAttempted    bool                `json:"test_attempted"`
	TestAccepted     bool                `json:"test_accepted"`
	LastCheck        time.Time           `json:"last_check_at"`
	ProbeOK          bool                `json:"probe_ok"`
	LastSend         string              `json:"last_send_outcome"`
}
type notice struct {
	Title string
	Body  string
}

const notificationDelay = 10 * time.Minute

func freshState() state {
	return state{Version: 1, Incidents: map[string]incident{}, Progress: map[string]progress{}}
}

func evaluate(s *state, h *health, probeOK bool, now time.Time) map[string]string {
	active := map[string]string{}
	if !probeOK {
		if s.UnavailableSince == nil {
			t := now
			s.UnavailableSince = &t
		}
		// Preserve known faults until a complete, fresh observation can clear them.
		for key, previous := range s.Incidents {
			if key != "monitor_unavailable" {
				active[key] = previous.Summary
			}
		}
		if now.Sub(*s.UnavailableSince) >= 3*time.Minute {
			active["monitor_unavailable"] = "监控连续 3 分钟无法取得完整健康状态，请检查服务、网络或访问凭证。"
		}
		return active
	}
	s.UnavailableSince = nil
	if h.Proxy.Enabled && h.Proxy.Auth {
		active["proxy_auth"] = "付费代理认证熔断：请检查快代理白名单、出口 IP 和认证配置；确认修复后再解除熔断。"
	}
	labels := map[string]string{"hourly": "每小时", "daily": "每日", "no_success": "连续无成功请求"}
	for _, budget := range h.Proxy.Budgets {
		if label, ok := labels[budget]; ok {
			active["budget_"+budget] = "付费代理" + label + "提取已触发预算保护，请检查消耗及抓取情况。"
		}
	}
	latest := h.Cycle
	seen := map[string]bool{}
	for _, source := range h.Sources {
		seen[source.Name] = true
		if source.LastSuccess != nil && (latest == nil || source.LastSuccess.After(*latest)) {
			latest = source.LastSuccess
		}
		if source.Pending == 0 {
			delete(s.Progress, source.Name)
			continue
		}
		stamp := ""
		if source.LastSuccess != nil {
			stamp = source.LastSuccess.Format(time.RFC3339Nano)
		}
		p, exists := s.Progress[source.Name]
		if !exists || p.Success != stamp {
			p = progress{Since: now, Success: stamp}
		}
		s.Progress[source.Name] = p
		if now.Sub(p.Since) >= 10*time.Minute && (source.LastSuccess == nil || now.Sub(*source.LastSuccess) >= 10*time.Minute) {
			active["source_"+source.Name] = fmt.Sprintf("%s 有 %d 个待抓任务，已连续至少 10 分钟未观察到成功抓取。", source.Name, source.Pending)
		}
	}
	for name := range s.Progress {
		if name != "_crawler" && !seen[name] {
			delete(s.Progress, name)
		}
	}
	// Even empty queues finish crawl cycles. This catches a dead crawler without
	// treating a normal poll-interval sleep as an interruption.
	grace := time.Duration(h.Poll*2+300) * time.Second
	if grace < 15*time.Minute {
		grace = 15 * time.Minute
	}
	if latest == nil {
		p, exists := s.Progress["_crawler"]
		if !exists {
			p = progress{Since: now}
			s.Progress["_crawler"] = p
		}
		if now.Sub(p.Since) >= grace {
			active["crawler_stalled"] = "长时间没有抓取周期完成或成功抓取记录，请检查抓取进程。"
		}
	} else {
		delete(s.Progress, "_crawler")
		if now.Sub(*latest) >= grace {
			active["crawler_stalled"] = "长时间没有抓取周期完成或成功抓取记录，请检查抓取进程。"
		}
	}
	return active
}

func prepare(s *state, active map[string]string, now time.Time) *notice {
	var alerts, recovered []string
	keys := make([]string, 0, len(active))
	for key := range active {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := s.Incidents[key]
		if v.Since.IsZero() {
			v.Since = now
		}
		v.Summary = active[key]
		if now.Sub(v.Since) >= notificationDelay && (v.LastAttempt.IsZero() || now.Sub(v.LastAttempt) >= time.Hour) {
			alerts = append(alerts, "- "+v.Summary)
			v.LastAttempt = now
			v.Accepted = false
		}
		s.Incidents[key] = v
	}
	keys = keys[:0]
	for key := range s.Incidents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := active[key]; ok {
			continue
		}
		previous := s.Incidents[key]
		if !previous.LastAttempt.IsZero() {
			recovered = append(recovered, "- "+previous.Summary)
		}
		delete(s.Incidents, key)
	}
	if len(alerts)+len(recovered) == 0 {
		return nil
	}
	title := "AlphaPulse 抓取异常"
	if len(alerts) == 0 {
		title = "AlphaPulse 抓取已恢复"
	}
	body := "检查时间：" + now.In(time.FixedZone("UTC+8", 8*3600)).Format("2006-01-02 15:04:05") + "（UTC+8）\n\n"
	if len(alerts) > 0 {
		body += "当前异常：\n" + strings.Join(alerts, "\n") + "\n\n"
	}
	if len(recovered) > 0 {
		body += "以下告警条件已解除：\n" + strings.Join(recovered, "\n") + "\n\n"
	}
	body += "此消息来自群晖上的独立监控。告警条件持续 10 分钟后才通知；相同异常每小时最多提醒一次，不会自动重启服务或解除代理熔断。"
	return &notice{Title: title, Body: body}
}

func client() *http.Client {
	return &http.Client{Timeout: 25 * time.Second,
		Transport:     &http.Transport{Proxy: nil, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second},
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
}

func secret(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("credential_unavailable")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(b) > 65536 {
		return "", errors.New("credential_unavailable")
	}
	v := strings.TrimSpace(string(b))
	if v == "" || strings.ContainsAny(v, "\r\n\x00") {
		return "", errors.New("credential_invalid")
	}
	return v, nil
}

func fetch(c *http.Client, endpoint, credential string, now time.Time) (*health, error) {
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.New("request_invalid")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "alphapulse-agent/watchdog-v1")
	req.Header.Set("Authorization", credential)
	res, err := c.Do(req)
	if err != nil {
		return nil, errors.New("probe_transport")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("probe_http_%d", res.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, 65537))
	if err != nil || len(b) > 65536 {
		return nil, errors.New("probe_body")
	}
	var h health
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil {
		return nil, errors.New("probe_json_invalid")
	}
	for _, field := range []string{"schema_version", "generated_at", "probe_errors", "poll_interval_seconds", "last_cycle_at", "sources", "proxy"} {
		if _, exists := fields[field]; !exists {
			return nil, errors.New("probe_missing_" + field)
		}
	}
	var proxyFields map[string]json.RawMessage
	if json.Unmarshal(fields["proxy"], &proxyFields) != nil {
		return nil, errors.New("probe_incomplete")
	}
	for _, field := range []string{"enabled", "auth_blocked", "budget_guards"} {
		if _, exists := proxyFields[field]; !exists {
			return nil, errors.New("probe_incomplete")
		}
	}
	if json.Unmarshal(b, &h) != nil || h.Version != 1 || h.Generated.IsZero() || len(h.Sources) == 0 || len(h.Errors) > 0 {
		for _, category := range []string{"state_store_unavailable", "fetch_store_unavailable", "proxy_store_unavailable", "cycle_store_unavailable", "unsupported_state_backend", "raw_monitoring_disabled"} {
			for _, observed := range h.Errors {
				if observed == category {
					return nil, errors.New("probe_component_" + category)
				}
			}
		}
		return nil, errors.New("probe_incomplete")
	}
	if now.Sub(h.Generated) > 3*time.Minute || h.Generated.Sub(now) > time.Minute {
		return nil, errors.New("probe_stale")
	}
	if h.Poll < 1 || h.Poll > 86400 {
		return nil, errors.New("probe_invalid")
	}
	allowed := map[string]bool{"guba": true, "tgb": true, "jiuyan": true, "hupu": true, "bilibili": true, "xueqiu": true}
	seen := map[string]bool{}
	for _, source := range h.Sources {
		if !allowed[source.Name] || seen[source.Name] || source.Pending < 0 || (source.LastSuccess != nil && source.LastSuccess.Sub(now) > time.Minute) {
			return nil, errors.New("probe_invalid")
		}
		seen[source.Name] = true
	}
	if h.Cycle != nil && h.Cycle.Sub(now) > time.Minute {
		return nil, errors.New("probe_invalid")
	}
	return &h, nil
}

var keyPattern = regexp.MustCompile(`^SCT[A-Za-z0-9]+$`)

func send(c *http.Client, keyPath string, n notice) bool {
	key, err := secret(keyPath)
	if err != nil || !keyPattern.MatchString(key) {
		return false
	}
	body := url.Values{"title": {n.Title}, "desp": {n.Body}}.Encode()
	req, err := http.NewRequest(http.MethodPost, "https://sctapi.ftqq.com/"+key+".send", strings.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := c.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return false
	}
	b, err := io.ReadAll(io.LimitReader(res.Body, 65537))
	if err != nil || len(b) > 65536 {
		return false
	}
	var result struct {
		Code *int `json:"code"`
	}
	return json.Unmarshal(b, &result) == nil && result.Code != nil && *result.Code == 0
}

func save(path string, s state) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return errors.New("state_encode")
	}
	f, err := os.OpenFile(path+".new", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return errors.New("state_open")
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return errors.New("state_write")
	}
	if os.Rename(path+".new", path) != nil {
		return errors.New("state_replace")
	}
	return nil
}

func load(path string) (state, error) {
	s := freshState()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil || len(b) > 1024*1024 || json.Unmarshal(b, &s) != nil || s.Version != 1 || s.Incidents == nil || s.Progress == nil {
		return s, errors.New("state_invalid")
	}
	return s, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Println(`{"ok":false,"error":"watchdog_operation_failed"}`)
		os.Exit(1)
	}
}

func run() error {
	endpoint := flag.String("endpoint", "https://alphapulse-api.sanae.edu.kg/api/crawl-health", "Health endpoint")
	authFile := flag.String("authorization-file", "/volume1/alphapulse-agent/cloudflare-authorization.token", "Existing access credential file")
	keyFile := flag.String("sendkey-file", "/volume1/stock-alert/sendkey", "Existing ServerChan key file")
	statePath := flag.String("state-file", "/volume1/alphapulse-watchdog/state.json", "Watchdog state")
	pidPath := flag.String("pid-file", "/volume1/alphapulse-watchdog/watchdog.pid", "Daemon process ID")
	once := flag.Bool("once", false, "One check")
	dry := flag.Bool("dry-run", false, "Read health only; no state writes or sends")
	test := flag.Bool("test-notification", false, "One idempotent, clearly labeled notification")
	flag.Parse()
	if *endpoint != "https://alphapulse-api.sanae.edu.kg/api/crawl-health" {
		return errors.New("endpoint_not_allowed")
	}
	if *dry && *test {
		return errors.New("flags_invalid")
	}
	c := client()
	if *dry {
		auth, err := secret(*authFile)
		if err != nil {
			return err
		}
		_, err = fetch(c, *endpoint, auth, time.Now().UTC())
		result := map[string]any{"ok": err == nil, "probe_ok": err == nil}
		if err != nil {
			result["category"] = err.Error()
		}
		b, _ := json.Marshal(result)
		fmt.Println(string(b))
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*statePath), 0700); err != nil {
		return errors.New("state_dir")
	}
	lock, err := os.OpenFile(*statePath+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return errors.New("lock_open")
	}
	defer lock.Close()
	if err = lockFile(lock); err != nil {
		return errors.New("already_running")
	}
	s, err := load(*statePath)
	if err != nil {
		return err
	}
	if !*once && !*test {
		if err = os.WriteFile(*pidPath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600); err != nil {
			return errors.New("pid_write")
		}
	}
	if *test {
		if s.TestAttempted {
			return errors.New("test_already_attempted")
		}
		s.TestAttempted = true
		if err = save(*statePath, s); err != nil {
			return err
		}
		s.TestAccepted = send(c, *keyFile, notice{Title: "AlphaPulse 抓取监控已接入（测试）", Body: "这是一条测试通知，不代表当前发生故障。\n\n群晖独立监控每分钟检查一次：代理认证熔断、提取预算达到上限、有待抓任务且连续 10 分钟无成功抓取、连续 3 分钟无法读取健康状态。上述告警条件持续 10 分钟后才通知，提前恢复则静默取消。相同异常一小时内去重，仅已通知的异常解除后通知恢复。\n\n请在聊天中确认手机是否实际收到。"})
		if err = save(*statePath, s); err != nil {
			return err
		}
		fmt.Printf("{\"ok\":%t,\"test_accepted\":%t}\n", s.TestAccepted, s.TestAccepted)
		if !s.TestAccepted {
			return errors.New("send_unconfirmed")
		}
		return nil
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(stop)
	for {
		now := time.Now().UTC()
		auth, authErr := secret(*authFile)
		var h *health
		var probeErr error
		if authErr == nil {
			h, probeErr = fetch(c, *endpoint, auth, now)
		} else {
			probeErr = authErr
		}
		ok := probeErr == nil
		active := evaluate(&s, h, ok, now)
		n := prepare(&s, active, now)
		s.LastCheck = now
		s.ProbeOK = ok
		// Save the attempt marker before sending. A timeout or crash must not
		// automatically replay the same POST on the next minute or restart.
		if n != nil {
			s.LastSend = "attempt_started"
		}
		if err = save(*statePath, s); err != nil {
			return err
		}
		if n != nil {
			accepted := send(c, *keyFile, *n)
			s.LastSend = "unconfirmed"
			if accepted {
				s.LastSend = "accepted"
				for key, v := range s.Incidents {
					if v.LastAttempt.Equal(now) {
						v.Accepted = true
						s.Incidents[key] = v
					}
				}
			}
			if err = save(*statePath, s); err != nil {
				return err
			}
		}
		result := map[string]any{"ok": true, "probe_ok": ok, "checked_at": now, "active_incidents": len(active), "notification_attempted": n != nil}
		b, _ := json.Marshal(result)
		if *once || n != nil {
			fmt.Println(string(bytes.TrimSpace(b)))
		}
		if *once {
			return nil
		}
		timer := time.NewTimer(time.Minute)
		select {
		case <-stop:
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
