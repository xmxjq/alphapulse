package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"
)

var version = "dev"

type stringList []string

func (values *stringList) String() string {
	return strings.Join(*values, ",")
}

func (values *stringList) Set(value string) error {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return errors.New("value cannot be empty")
	}
	*values = append(*values, value)
	return nil
}

type valueList []string

func (values *valueList) String() string {
	return strings.Join(*values, ",")
}

func (values *valueList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("value cannot be empty")
	}
	*values = append(*values, value)
	return nil
}

type config struct {
	serverURL               string
	agentID                 string
	token                   string
	cloudflareAuthorization string
	cloudflareClientID      string
	cloudflareClientSecret  string
	allowedHosts            map[string]struct{}
	maxConcurrency          int
	pollWaitSeconds         int
	heartbeatInterval       time.Duration
	requestIntervalMin      time.Duration
	requestIntervalMax      time.Duration
	activeSchedule          *dailySchedule
}

type requestPacer struct {
	mu      sync.Mutex
	minimum time.Duration
	maximum time.Duration
	nextAt  time.Time
}

type dailyWindow struct {
	spec        string
	startMinute int
	endMinute   int
	index       int
}

type resolvedWindow struct {
	start time.Time
	end   time.Time
}

type dailySchedule struct {
	location *time.Location
	windows  []dailyWindow
	jitter   time.Duration
	seed     string
}

type agentInfo struct {
	AgentID        string   `json:"agent_id"`
	Version        string   `json:"version"`
	OS             string   `json:"os"`
	Arch           string   `json:"arch"`
	Capabilities   []string `json:"capabilities"`
	MaxConcurrency int      `json:"max_concurrency"`
}

type leaseRequest struct {
	agentInfo
	WaitSeconds int `json:"wait_seconds"`
}

type leasedJob struct {
	JobID            string            `json:"job_id"`
	LeaseID          string            `json:"lease_id"`
	Source           string            `json:"source"`
	Capability       string            `json:"capability"`
	Method           string            `json:"method"`
	URL              string            `json:"url"`
	Headers          map[string]string `json:"headers"`
	BodyBase64       *string           `json:"body_base64"`
	TimeoutSeconds   int               `json:"timeout_seconds"`
	MaxResponseBytes int64             `json:"max_response_bytes"`
	LeaseExpiresAt   string            `json:"lease_expires_at"`
}

type completedJob struct {
	LeaseID    string            `json:"lease_id"`
	StatusCode int               `json:"status_code"`
	FinalURL   string            `json:"final_url"`
	Headers    map[string]string `json:"headers"`
	BodyBase64 string            `json:"body_base64"`
	DurationMS int64             `json:"duration_ms"`
}

type failedJob struct {
	LeaseID      string `json:"lease_id"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

type fetchResult struct {
	statusCode int
	finalURL   string
	headers    map[string]string
	body       []byte
	durationMS int64
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	info := agentInfo{
		AgentID:        cfg.agentID,
		Version:        version,
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		Capabilities:   capabilitiesForHosts(cfg.allowedHosts),
		MaxConcurrency: cfg.maxConcurrency,
	}
	pacer := &requestPacer{
		minimum: cfg.requestIntervalMin,
		maximum: cfg.requestIntervalMax,
	}
	apiClient := &http.Client{Timeout: 90 * time.Second}
	if cfg.activeSchedule == nil || cfg.activeSchedule.activeAt(time.Now()) {
		if err := heartbeat(ctx, apiClient, cfg, info); err != nil {
			log.Printf("initial heartbeat failed: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		heartbeatLoop(ctx, apiClient, cfg, info)
	}()
	for worker := 0; worker < cfg.maxConcurrency; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerLoop(ctx, workerID, apiClient, cfg, info, pacer)
		}(worker + 1)
	}
	scheduleDescription := "always"
	if cfg.activeSchedule != nil {
		scheduleDescription = cfg.activeSchedule.String()
	}
	log.Printf(
		"alphapulse-agent %s started id=%s platform=%s/%s workers=%d request_interval=%s..%s active_schedule=%q",
		version,
		cfg.agentID,
		runtime.GOOS,
		runtime.GOARCH,
		cfg.maxConcurrency,
		cfg.requestIntervalMin,
		cfg.requestIntervalMax,
		scheduleDescription,
	)
	<-ctx.Done()
	wg.Wait()
}

func capabilitiesForHosts(allowedHosts map[string]struct{}) []string {
	capabilities := []string{"http"}
	for host := range allowedHosts {
		capabilities = append(capabilities, "http-host:"+normalizeHost(host))
	}
	sort.Strings(capabilities)
	return capabilities
}

func parseConfig() (config, error) {
	var allowedHosts stringList
	var activeWindows valueList
	serverURL := flag.String("server", "", "AlphaPulse server base URL")
	agentID := flag.String("id", "", "Stable agent id")
	tokenFile := flag.String("token-file", "", "File containing the AlphaPulse agent token")
	cfAuthorizationFile := flag.String(
		"cloudflare-authorization-file",
		"",
		"Optional file containing the Cloudflare Authorization header value",
	)
	cfClientIDFile := flag.String(
		"cf-access-client-id-file",
		"",
		"Optional file containing a Cloudflare Access client id",
	)
	cfClientSecretFile := flag.String(
		"cf-access-client-secret-file",
		"",
		"Optional file containing a Cloudflare Access client secret",
	)
	maxConcurrency := flag.Int("max-concurrency", 1, "Number of concurrent fetch workers")
	pollWait := flag.Int("poll-wait", 20, "Server-side long poll duration in seconds")
	heartbeatSeconds := flag.Int("heartbeat-interval", 30, "Heartbeat interval in seconds")
	requestIntervalMin := flag.Duration(
		"request-interval-min",
		0,
		"Minimum delay between target requests, for example 30s",
	)
	requestIntervalMax := flag.Duration(
		"request-interval-max",
		0,
		"Maximum delay between target requests, for example 60s",
	)
	activeTimezone := flag.String(
		"active-timezone",
		"Local",
		"IANA timezone used by active windows, for example Asia/Shanghai",
	)
	activeWindowJitter := flag.Duration(
		"active-window-jitter",
		0,
		"Maximum daily inward jitter applied to each active-window boundary",
	)
	flag.Var(&allowedHosts, "allow-host", "Allowed target hostname; may be repeated")
	flag.Var(
		&activeWindows,
		"active-window",
		"Daily local-time fetch window in HH:MM-HH:MM form; may be repeated",
	)
	flag.Parse()

	if strings.TrimSpace(*serverURL) == "" {
		return config{}, errors.New("--server is required")
	}
	if strings.TrimSpace(*agentID) == "" {
		return config{}, errors.New("--id is required")
	}
	if *tokenFile == "" {
		return config{}, errors.New("--token-file is required")
	}
	if *maxConcurrency < 1 || *maxConcurrency > 64 {
		return config{}, errors.New("--max-concurrency must be between 1 and 64")
	}
	if *pollWait < 0 || *pollWait > 30 {
		return config{}, errors.New("--poll-wait must be between 0 and 30")
	}
	if *heartbeatSeconds < 10 {
		return config{}, errors.New("--heartbeat-interval must be at least 10 seconds")
	}
	if *requestIntervalMin < 0 || *requestIntervalMax < 0 {
		return config{}, errors.New("request intervals cannot be negative")
	}
	if *requestIntervalMax == 0 && *requestIntervalMin > 0 {
		*requestIntervalMax = *requestIntervalMin
	}
	if *requestIntervalMax < *requestIntervalMin {
		return config{}, errors.New("--request-interval-max must be >= --request-interval-min")
	}
	if *requestIntervalMax > 0 && *maxConcurrency != 1 {
		return config{}, errors.New(
			"--max-concurrency must be 1 when request interval pacing is enabled",
		)
	}
	activeSchedule, err := newDailySchedule(
		activeWindows,
		*activeTimezone,
		*activeWindowJitter,
		strings.TrimSpace(*agentID),
	)
	if err != nil {
		return config{}, err
	}
	token, err := readSecret(*tokenFile)
	if err != nil {
		return config{}, fmt.Errorf("read agent token: %w", err)
	}
	cfAuthorization, err := readOptionalSecret(*cfAuthorizationFile)
	if err != nil {
		return config{}, fmt.Errorf("read Cloudflare authorization: %w", err)
	}
	cfClientID, err := readOptionalSecret(*cfClientIDFile)
	if err != nil {
		return config{}, fmt.Errorf("read Cloudflare client id: %w", err)
	}
	cfClientSecret, err := readOptionalSecret(*cfClientSecretFile)
	if err != nil {
		return config{}, fmt.Errorf("read Cloudflare client secret: %w", err)
	}
	if len(allowedHosts) == 0 {
		allowedHosts = []string{
			"guba.eastmoney.com",
			"emappdata.eastmoney.com",
			"push2.eastmoney.com",
			"www.tgb.cn",
			"app.jiuyangongshe.com",
			"bbs.hupu.com",
		}
	}
	allowed := make(map[string]struct{}, len(allowedHosts))
	for _, host := range allowedHosts {
		allowed[normalizeHost(host)] = struct{}{}
	}
	return config{
		serverURL:               strings.TrimRight(*serverURL, "/"),
		agentID:                 strings.TrimSpace(*agentID),
		token:                   token,
		cloudflareAuthorization: cfAuthorization,
		cloudflareClientID:      cfClientID,
		cloudflareClientSecret:  cfClientSecret,
		allowedHosts:            allowed,
		maxConcurrency:          *maxConcurrency,
		pollWaitSeconds:         *pollWait,
		heartbeatInterval:       time.Duration(*heartbeatSeconds) * time.Second,
		requestIntervalMin:      *requestIntervalMin,
		requestIntervalMax:      *requestIntervalMax,
		activeSchedule:          activeSchedule,
	}, nil
}

func newDailySchedule(
	specs []string,
	timezone string,
	jitter time.Duration,
	seed string,
) (*dailySchedule, error) {
	if len(specs) == 0 {
		if jitter != 0 {
			return nil, errors.New("--active-window-jitter requires --active-window")
		}
		return nil, nil
	}
	if jitter < 0 {
		return nil, errors.New("--active-window-jitter cannot be negative")
	}
	timezone = strings.TrimSpace(timezone)
	if timezone == "" {
		return nil, errors.New("--active-timezone cannot be empty")
	}
	location := time.Local
	if timezone != "Local" {
		var err error
		location, err = time.LoadLocation(timezone)
		if err != nil {
			return nil, fmt.Errorf("load active timezone %q: %w", timezone, err)
		}
	}
	windows := make([]dailyWindow, 0, len(specs))
	for index, spec := range specs {
		window, err := parseDailyWindow(spec, index)
		if err != nil {
			return nil, err
		}
		duration := windowDuration(window)
		if jitter*2 >= duration {
			return nil, fmt.Errorf(
				"--active-window-jitter must be less than half of window %q",
				window.spec,
			)
		}
		windows = append(windows, window)
	}
	return &dailySchedule{
		location: location,
		windows:  windows,
		jitter:   jitter,
		seed:     seed,
	}, nil
}

func parseDailyWindow(spec string, index int) (dailyWindow, error) {
	spec = strings.TrimSpace(spec)
	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return dailyWindow{}, fmt.Errorf(
			"invalid --active-window %q; expected HH:MM-HH:MM",
			spec,
		)
	}
	startMinute, err := parseClockMinute(parts[0])
	if err != nil {
		return dailyWindow{}, fmt.Errorf("invalid --active-window %q: %w", spec, err)
	}
	endMinute, err := parseClockMinute(parts[1])
	if err != nil {
		return dailyWindow{}, fmt.Errorf("invalid --active-window %q: %w", spec, err)
	}
	if startMinute == endMinute {
		return dailyWindow{}, fmt.Errorf(
			"invalid --active-window %q: start and end must differ",
			spec,
		)
	}
	return dailyWindow{
		spec:        spec,
		startMinute: startMinute,
		endMinute:   endMinute,
		index:       index,
	}, nil
}

func parseClockMinute(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) != 5 || raw[2] != ':' {
		return 0, errors.New("time must use HH:MM")
	}
	hour, hourErr := strconv.Atoi(raw[:2])
	minute, minuteErr := strconv.Atoi(raw[3:])
	if hourErr != nil || minuteErr != nil {
		return 0, errors.New("time must use HH:MM")
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, errors.New("time is outside 00:00-23:59")
	}
	return hour*60 + minute, nil
}

func windowDuration(window dailyWindow) time.Duration {
	endMinute := window.endMinute
	if endMinute <= window.startMinute {
		endMinute += 24 * 60
	}
	return time.Duration(endMinute-window.startMinute) * time.Minute
}

func (schedule *dailySchedule) String() string {
	specs := make([]string, 0, len(schedule.windows))
	for _, window := range schedule.windows {
		specs = append(specs, window.spec)
	}
	return fmt.Sprintf(
		"%s timezone=%s jitter<=%s",
		strings.Join(specs, ","),
		schedule.location.String(),
		schedule.jitter,
	)
}

func (schedule *dailySchedule) activeAt(now time.Time) bool {
	now = now.In(schedule.location)
	day := localMidnight(now, schedule.location)
	for _, offset := range []int{-1, 0} {
		windowDay := day.AddDate(0, 0, offset)
		for _, window := range schedule.windows {
			resolved := schedule.resolve(windowDay, window)
			if !now.Before(resolved.start) && now.Before(resolved.end) {
				return true
			}
		}
	}
	return false
}

func (schedule *dailySchedule) nextStart(now time.Time) time.Time {
	now = now.In(schedule.location)
	day := localMidnight(now, schedule.location)
	var next time.Time
	for offset := 0; offset <= 2; offset++ {
		windowDay := day.AddDate(0, 0, offset)
		for _, window := range schedule.windows {
			start := schedule.resolve(windowDay, window).start
			if !start.After(now) {
				continue
			}
			if next.IsZero() || start.Before(next) {
				next = start
			}
		}
	}
	return next
}

func (schedule *dailySchedule) resolve(
	day time.Time,
	window dailyWindow,
) resolvedWindow {
	start := clockOnDay(day, window.startMinute)
	endDay := day
	if window.endMinute <= window.startMinute {
		endDay = endDay.AddDate(0, 0, 1)
	}
	end := clockOnDay(endDay, window.endMinute)
	start = start.Add(schedule.boundaryJitter(day, window.index, "start"))
	end = end.Add(-schedule.boundaryJitter(day, window.index, "end"))
	return resolvedWindow{start: start, end: end}
}

func clockOnDay(day time.Time, minuteOfDay int) time.Time {
	return time.Date(
		day.Year(),
		day.Month(),
		day.Day(),
		minuteOfDay/60,
		minuteOfDay%60,
		0,
		0,
		day.Location(),
	)
}

func (schedule *dailySchedule) boundaryJitter(
	day time.Time,
	windowIndex int,
	boundary string,
) time.Duration {
	if schedule.jitter <= 0 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = fmt.Fprintf(
		hash,
		"%s|%s|%d|%s",
		schedule.seed,
		day.Format("2006-01-02"),
		windowIndex,
		boundary,
	)
	return time.Duration(hash.Sum64() % (uint64(schedule.jitter) + 1))
}

func localMidnight(now time.Time, location *time.Location) time.Time {
	local := now.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
}

func readSecret(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", errors.New("secret file is empty")
	}
	return value, nil
}

func readOptionalSecret(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	return readSecret(path)
}

func heartbeatLoop(
	ctx context.Context,
	client *http.Client,
	cfg config,
	info agentInfo,
) {
	ticker := time.NewTicker(cfg.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cfg.activeSchedule != nil &&
				!cfg.activeSchedule.activeAt(time.Now()) {
				continue
			}
			if err := heartbeat(ctx, client, cfg, info); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
		}
	}
}

func heartbeat(
	ctx context.Context,
	client *http.Client,
	cfg config,
	info agentInfo,
) error {
	status, _, err := apiJSON(
		ctx,
		client,
		cfg,
		http.MethodPost,
		"/api/agent/v1/heartbeat",
		info,
	)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("heartbeat returned HTTP %d", status)
	}
	return nil
}

func workerLoop(
	ctx context.Context,
	workerID int,
	apiClient *http.Client,
	cfg config,
	info agentInfo,
	pacer *requestPacer,
) {
	for ctx.Err() == nil {
		if cfg.activeSchedule != nil &&
			!cfg.activeSchedule.activeAt(time.Now()) {
			next := cfg.activeSchedule.nextStart(time.Now())
			if workerID == 1 {
				log.Printf(
					"outside active schedule; next fetch window starts at %s",
					next.Format(time.RFC3339),
				)
			}
			if next.IsZero() {
				sleepContext(ctx, time.Minute)
			} else {
				sleepContext(ctx, time.Until(next))
			}
			continue
		}
		job, err := lease(ctx, apiClient, cfg, info)
		if err != nil {
			log.Printf("worker=%d lease failed: %v", workerID, err)
			sleepContext(ctx, 3*time.Second)
			continue
		}
		if job == nil {
			continue
		}
		if err := pacer.wait(ctx); err != nil {
			return
		}
		result, err := executeJob(ctx, cfg, *job)
		if err != nil {
			log.Printf("worker=%d job=%s failed: %v", workerID, job.JobID, err)
			if reportErr := reportFailure(ctx, apiClient, cfg, *job, err); reportErr != nil {
				log.Printf("worker=%d job=%s failure report failed: %v", workerID, job.JobID, reportErr)
			}
			continue
		}
		if err := reportCompletion(ctx, apiClient, cfg, *job, result); err != nil {
			log.Printf("worker=%d job=%s completion report failed: %v", workerID, job.JobID, err)
		}
	}
}

func (pacer *requestPacer) wait(ctx context.Context) error {
	if pacer.maximum <= 0 {
		return nil
	}
	pacer.mu.Lock()
	now := time.Now()
	startAt := now
	if pacer.nextAt.After(startAt) {
		startAt = pacer.nextAt
	}
	interval := pacer.minimum
	if spread := pacer.maximum - pacer.minimum; spread > 0 {
		interval += time.Duration(rand.Int63n(int64(spread) + 1))
	}
	pacer.nextAt = startAt.Add(interval)
	pacer.mu.Unlock()

	delay := time.Until(startAt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func lease(
	ctx context.Context,
	client *http.Client,
	cfg config,
	info agentInfo,
) (*leasedJob, error) {
	status, body, err := apiJSON(
		ctx,
		client,
		cfg,
		http.MethodPost,
		"/api/agent/v1/jobs/lease",
		leaseRequest{agentInfo: info, WaitSeconds: cfg.pollWaitSeconds},
	)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("lease returned HTTP %d: %s", status, limitedText(body))
	}
	var job leasedJob
	if err := json.Unmarshal(body, &job); err != nil {
		return nil, fmt.Errorf("decode lease response: %w", err)
	}
	return &job, nil
}

func executeJob(ctx context.Context, cfg config, job leasedJob) (fetchResult, error) {
	if job.Capability != "http" {
		return fetchResult{}, fmt.Errorf("unsupported capability %q", job.Capability)
	}
	target, err := url.Parse(job.URL)
	if err != nil {
		return fetchResult{}, fmt.Errorf("parse target URL: %w", err)
	}
	if err := validateTarget(target, cfg.allowedHosts); err != nil {
		return fetchResult{}, err
	}
	var requestBody []byte
	if job.BodyBase64 != nil {
		requestBody, err = base64.StdEncoding.DecodeString(*job.BodyBase64)
		if err != nil {
			return fetchResult{}, fmt.Errorf("decode request body: %w", err)
		}
	}
	timeout := time.Duration(job.TimeoutSeconds) * time.Second
	if timeout <= 0 || timeout > 5*time.Minute {
		return fetchResult{}, errors.New("invalid request timeout")
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		requestCtx,
		job.Method,
		target.String(),
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return fetchResult{}, fmt.Errorf("build target request: %w", err)
	}
	copyAllowedHeaders(req.Header, job.Headers, job.Source)

	transport := &http.Transport{
		Proxy:                 nil,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          20,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		DialContext:           safeDialer(cfg.allowedHosts),
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(next *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many redirects")
			}
			if err := validateTarget(next.URL, cfg.allowedHosts); err != nil {
				return err
			}
			copyAllowedHeaders(next.Header, job.Headers, job.Source)
			return nil
		},
	}
	started := time.Now()
	response, err := client.Do(req)
	if err != nil {
		return fetchResult{}, fmt.Errorf("target request failed: %w", err)
	}
	defer response.Body.Close()
	maxBytes := job.MaxResponseBytes
	if maxBytes < 1024 || maxBytes > 50_000_000 {
		return fetchResult{}, errors.New("invalid response size limit")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return fetchResult{}, fmt.Errorf("read target response: %w", err)
	}
	if int64(len(body)) > maxBytes {
		return fetchResult{}, fmt.Errorf("target response exceeds %d bytes", maxBytes)
	}
	return fetchResult{
		statusCode: response.StatusCode,
		finalURL:   response.Request.URL.String(),
		headers:    responseHeaders(response.Header),
		body:       body,
		durationMS: time.Since(started).Milliseconds(),
	}, nil
}

func reportCompletion(
	ctx context.Context,
	client *http.Client,
	cfg config,
	job leasedJob,
	result fetchResult,
) error {
	payload := completedJob{
		LeaseID:    job.LeaseID,
		StatusCode: result.statusCode,
		FinalURL:   result.finalURL,
		Headers:    result.headers,
		BodyBase64: base64.StdEncoding.EncodeToString(result.body),
		DurationMS: result.durationMS,
	}
	path := fmt.Sprintf("/api/agent/v1/jobs/%s/complete", url.PathEscape(job.JobID))
	status, body, err := apiJSON(ctx, client, cfg, http.MethodPost, path, payload)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("complete returned HTTP %d: %s", status, limitedText(body))
	}
	return nil
}

func reportFailure(
	ctx context.Context,
	client *http.Client,
	cfg config,
	job leasedJob,
	jobError error,
) error {
	message := jobError.Error()
	if len(message) > 1800 {
		message = message[:1800]
	}
	payload := failedJob{
		LeaseID:      job.LeaseID,
		ErrorMessage: message,
		Retryable:    true,
	}
	path := fmt.Sprintf("/api/agent/v1/jobs/%s/fail", url.PathEscape(job.JobID))
	status, body, err := apiJSON(ctx, client, cfg, http.MethodPost, path, payload)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("fail returned HTTP %d: %s", status, limitedText(body))
	}
	return nil
}

func apiJSON(
	ctx context.Context,
	client *http.Client,
	cfg config,
	method string,
	path string,
	payload any,
) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(
		ctx,
		method,
		cfg.serverURL+path,
		bytes.NewReader(body),
	)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "alphapulse-agent/"+version)
	req.Header.Set("X-AlphaPulse-Agent-ID", cfg.agentID)
	req.Header.Set("X-AlphaPulse-Agent-Token", cfg.token)
	if cfg.cloudflareAuthorization != "" {
		req.Header.Set("Authorization", cfg.cloudflareAuthorization)
	}
	if cfg.cloudflareClientID != "" {
		req.Header.Set("CF-Access-Client-Id", cfg.cloudflareClientID)
	}
	if cfg.cloudflareClientSecret != "" {
		req.Header.Set("CF-Access-Client-Secret", cfg.cloudflareClientSecret)
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1_000_000))
	if err != nil {
		return response.StatusCode, nil, err
	}
	return response.StatusCode, responseBody, nil
}

func safeDialer(allowedHosts map[string]struct{}) func(
	context.Context,
	string,
	string,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	resolver := net.DefaultResolver
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		host = normalizeHost(host)
		if _, ok := allowedHosts[host]; !ok {
			return nil, fmt.Errorf("dial host is not allowed: %s", host)
		}
		ips, err := resolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("resolve target host: %w", err)
		}
		for _, ip := range ips {
			if unsafeIP(ip) {
				continue
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		}
		return nil, errors.New("target host has no safe public address")
	}
}

func validateTarget(target *url.URL, allowedHosts map[string]struct{}) error {
	if target.Scheme != "http" && target.Scheme != "https" {
		return errors.New("only HTTP(S) targets are allowed")
	}
	host := normalizeHost(target.Hostname())
	if host == "" {
		return errors.New("target URL has no hostname")
	}
	if _, ok := allowedHosts[host]; !ok {
		return fmt.Errorf("target host is not allowed: %s", host)
	}
	if target.User != nil {
		return errors.New("target URL credentials are not allowed")
	}
	return nil
}

func normalizeHost(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
}

func unsafeIP(ip net.IP) bool {
	return ip.IsUnspecified() ||
		ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast()
}

func copyAllowedHeaders(
	target http.Header,
	source map[string]string,
	jobSource string,
) {
	allowed := map[string]struct{}{
		"accept":          {},
		"accept-language": {},
		"cache-control":   {},
		"content-type":    {},
		"referer":         {},
		"user-agent":      {},
	}
	if jobSource == "jiuyan" {
		for _, key := range []string{
			"origin",
			"platform",
			"timestamp",
			"token",
			"x-requested-with",
		} {
			allowed[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := allowed[strings.ToLower(key)]; ok {
			target.Set(key, source[key])
		}
	}
}

func responseHeaders(headers http.Header) map[string]string {
	allowed := map[string]struct{}{
		"content-type":   {},
		"content-length": {},
		"last-modified":  {},
		"etag":           {},
		"date":           {},
	}
	result := make(map[string]string)
	for key, values := range headers {
		lower := strings.ToLower(key)
		if _, ok := allowed[lower]; !ok || len(values) == 0 {
			continue
		}
		result[lower] = strings.Join(values, ", ")
	}
	return result
}

func sleepContext(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func limitedText(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 300 {
		return text[:300]
	}
	return text
}
