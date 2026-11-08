package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var epoch = time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)

func healthy() *health {
	return &health{Version: 1, Generated: epoch, Poll: 300, Cycle: &epoch,
		Sources: []sourceHealth{{Name: "guba", Pending: 3, LastSuccess: &epoch}}}
}

func TestSourceStallAndRecovery(t *testing.T) {
	s := freshState()
	h := healthy()
	if len(evaluate(&s, h, true, epoch)) != 0 {
		t.Fatal("fresh source alarm")
	}
	if len(evaluate(&s, h, true, epoch.Add(9*time.Minute))) != 0 {
		t.Fatal("premature alarm")
	}
	active := evaluate(&s, h, true, epoch.Add(10*time.Minute))
	if active["source_guba"] == "" {
		t.Fatal("missing source alarm")
	}
	if prepare(&s, active, epoch.Add(10*time.Minute)) != nil {
		t.Fatal("source confirmation window skipped")
	}
	if prepare(&s, active, epoch.Add(25*time.Minute)) != nil {
		t.Fatal("diagnostic source generated notification")
	}
	if prepare(&s, active, epoch.Add(26*time.Minute)) != nil {
		t.Fatal("duplicate notification")
	}
	stamp := epoch.Add(27 * time.Minute)
	h.Sources[0].LastSuccess = &stamp
	n := prepare(&s, evaluate(&s, h, true, stamp), stamp)
	if n != nil {
		t.Fatal("diagnostic source generated recovery")
	}
	if prepare(&s, evaluate(&s, h, true, stamp), stamp) != nil {
		t.Fatal("duplicate recovery")
	}
}

func TestIdleAndDisabledSource(t *testing.T) {
	s := freshState()
	h := healthy()
	h.Sources[0].Pending = 0
	stamp := epoch.Add(20 * time.Minute)
	h.Cycle = &stamp
	if len(evaluate(&s, h, true, stamp)) != 0 {
		t.Fatal("idle alarm")
	}
	h.Sources = nil
	if len(evaluate(&s, h, true, stamp)) != 0 {
		t.Fatal("disabled source alarm")
	}
}

func TestGuardConfirmationAndSixHourlyRepeat(t *testing.T) {
	s := freshState()
	h := healthy()
	h.Proxy.Enabled = true
	h.Proxy.Auth = true
	h.Proxy.Budgets = []string{"daily"}
	a := evaluate(&s, h, true, epoch)
	if len(a) != 2 {
		t.Fatal("guards missing")
	}
	if prepare(&s, a, epoch) != nil {
		t.Fatal("guard confirmation window skipped")
	}
	if prepare(&s, a, epoch.Add(10*time.Minute)) != nil {
		t.Fatal("old ten-minute threshold still alerts")
	}
	if prepare(&s, a, epoch.Add(15*time.Minute)) == nil {
		t.Fatal("first notification missing")
	}
	if prepare(&s, a, epoch.Add(74*time.Minute)) != nil {
		t.Fatal("early repeat")
	}
	if prepare(&s, a, epoch.Add(375*time.Minute-time.Nanosecond)) != nil {
		t.Fatal("repeat before six hours")
	}
	if prepare(&s, a, epoch.Add(375*time.Minute)) == nil {
		t.Fatal("repeat missing")
	}
}

func TestUnreadableDoesNotRecoverKnownFault(t *testing.T) {
	s := freshState()
	h := healthy()
	h.Proxy.Enabled = true
	h.Proxy.Auth = true
	prepare(&s, evaluate(&s, h, true, epoch), epoch)
	active := evaluate(&s, nil, false, epoch.Add(time.Minute))
	if active["proxy_auth"] == "" {
		t.Fatal("fault falsely recovered")
	}
	if prepare(&s, active, epoch.Add(time.Minute)) != nil {
		t.Fatal("false recovery")
	}
	active = evaluate(&s, nil, false, epoch.Add(4*time.Minute))
	if active["monitor_unavailable"] == "" {
		t.Fatal("missing monitoring outage")
	}
}

func TestProgressResetsWait(t *testing.T) {
	s := freshState()
	h := healthy()
	evaluate(&s, h, true, epoch)
	now := epoch.Add(9 * time.Minute)
	h.Sources[0].LastSuccess = &now
	evaluate(&s, h, true, now)
	if evaluate(&s, h, true, epoch.Add(12*time.Minute))["source_guba"] != "" {
		t.Fatal("progress ignored")
	}
}

func TestUnknownSuccessRequiresObservedBacklog(t *testing.T) {
	s := freshState()
	h := healthy()
	h.Sources[0].LastSuccess = nil
	if evaluate(&s, h, true, epoch)["source_guba"] != "" {
		t.Fatal("no startup grace")
	}
	if evaluate(&s, h, true, epoch.Add(10*time.Minute))["source_guba"] == "" {
		t.Fatal("missing unknown-success alarm")
	}
}

func TestCrawlerWithNoRecords(t *testing.T) {
	s := freshState()
	h := healthy()
	h.Cycle = nil
	h.Sources[0].LastSuccess = nil
	h.Sources[0].Pending = 0
	for i := 0; i < 15; i++ {
		if evaluate(&s, h, true, epoch.Add(time.Duration(i)*time.Minute))["crawler_stalled"] != "" {
			t.Fatal("early crawler alarm")
		}
	}
	if evaluate(&s, h, true, epoch.Add(15*time.Minute))["crawler_stalled"] == "" {
		t.Fatal("missing crawler alarm")
	}
}

func TestStatePersistsAttemptAndRejectsCorruption(t *testing.T) {
	dir, err := os.MkdirTemp("", "alphapulse-watchdog-fixture-")
	if err != nil {
		t.Fatal("fixture failed")
	}
	s := freshState()
	p := filepath.Join(dir, "state.json")
	prepare(&s, map[string]string{"proxy_auth": "test"}, epoch)
	prepare(&s, map[string]string{"proxy_auth": "test"}, epoch.Add(15*time.Minute))
	if save(p, s) != nil {
		t.Fatal("save failed")
	}
	loaded, err := load(p)
	if err != nil {
		t.Fatal("load failed")
	}
	if prepare(&loaded, map[string]string{"proxy_auth": "test"}, epoch.Add(16*time.Minute)) != nil {
		t.Fatal("restart replay")
	}
	if os.WriteFile(p, []byte("not-json"), 0600) != nil {
		t.Fatal("fixture failed")
	}
	if _, err = load(p); err == nil {
		t.Fatal("corrupt state reset silently")
	}
}

func TestBriefIncidentsStaySilent(t *testing.T) {
	for _, key := range []string{"proxy_auth", "budget_hourly", "budget_daily", "budget_no_success", "monitor_unavailable", "crawler_stalled"} {
		t.Run(key, func(t *testing.T) {
			s := freshState()
			active := map[string]string{key: "test"}
			if prepare(&s, active, epoch) != nil || prepare(&s, active, epoch.Add(10*time.Minute)) != nil || prepare(&s, active, epoch.Add(15*time.Minute-time.Nanosecond)) != nil {
				t.Fatal("brief incident alerted")
			}
			if prepare(&s, map[string]string{}, epoch.Add(15*time.Minute)) != nil || len(s.Incidents) != 0 {
				t.Fatal("brief incident recovery was not silent")
			}
			if prepare(&s, active, epoch.Add(16*time.Minute)) != nil {
				t.Fatal("new occurrence inherited old timer")
			}
			if prepare(&s, active, epoch.Add(31*time.Minute)) == nil {
				t.Fatal("persistent occurrence did not alert")
			}
		})
	}
}

func TestPendingTimerSurvivesRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "alphapulse-watchdog-delay-")
	if err != nil {
		t.Fatal("fixture failed")
	}
	s := freshState()
	active := map[string]string{"proxy_auth": "test"}
	prepare(&s, active, epoch)
	p := filepath.Join(dir, "state.json")
	if save(p, s) != nil {
		t.Fatal("save failed")
	}
	loaded, err := load(p)
	if err != nil {
		t.Fatal("load failed")
	}
	if prepare(&loaded, active, epoch.Add(14*time.Minute)) != nil {
		t.Fatal("pending restart alerted early")
	}
	if prepare(&loaded, active, epoch.Add(15*time.Minute)) == nil {
		t.Fatal("pending restart reset the timer")
	}
}

func TestMixedIncidentsHaveIndependentTimers(t *testing.T) {
	s := freshState()
	prepare(&s, map[string]string{"proxy_auth": "auth"}, epoch)
	now := epoch.Add(15 * time.Minute)
	n := prepare(&s, map[string]string{"proxy_auth": "auth", "crawler_stalled": "source"}, now)
	if n == nil || !strings.Contains(n.Body, "- auth") || strings.Contains(n.Body, "- source") {
		t.Fatal("new incident bypassed its own confirmation window")
	}
	n = prepare(&s, map[string]string{}, now.Add(time.Minute))
	if n == nil || !strings.Contains(n.Body, "- auth") || strings.Contains(n.Body, "- source") {
		t.Fatal("silent incident leaked into recovery")
	}
}

func TestFetchRejectsRedirectStaleIncompleteAndSecretBody(t *testing.T) {
	for _, mode := range []string{"ok", "redirect", "stale", "incomplete", "invalid", "future"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "fake-test-credential" {
					t.Error("missing test auth")
				}
				if mode == "redirect" {
					http.Redirect(w, r, "http://127.0.0.1:1/private", 302)
					return
				}
				if mode == "invalid" {
					fmt.Fprint(w, "fake-test-credential")
					return
				}
				h := healthy()
				if mode == "stale" {
					h.Generated = epoch.Add(-time.Hour)
				}
				if mode == "incomplete" {
					h.Errors = []string{"state_store_unavailable"}
				}
				if mode == "future" {
					f := epoch.Add(time.Hour)
					h.Cycle = &f
				}
				json.NewEncoder(w).Encode(h)
			}))
			defer server.Close()
			_, err := fetch(client(), server.URL, "fake-test-credential", epoch)
			if (err == nil) != (mode == "ok") {
				t.Fatalf("unexpected mode result %s", mode)
			}
			if err != nil && strings.Contains(err.Error(), "fake-test-credential") {
				t.Fatal("credential leaked")
			}
		})
	}
}

func TestDiagnosticSourcesNeverNotify(t *testing.T) {
	for _, name := range []string{"guba", "tgb", "jiuyan", "hupu", "bilibili", "xueqiu"} {
		t.Run(name, func(t *testing.T) {
			s := freshState()
			key := "source_" + name
			active := map[string]string{key: "diagnostic"}
			for _, elapsed := range []time.Duration{0, 15 * time.Minute, 6 * time.Hour, 24 * time.Hour} {
				if prepare(&s, active, epoch.Add(elapsed)) != nil {
					t.Fatal("source notification")
				}
			}
			if len(s.Incidents) != 1 || !s.Incidents[key].LastAttempt.IsZero() {
				t.Fatal("diagnostic state not retained silently")
			}
			if prepare(&s, map[string]string{}, epoch.Add(25*time.Hour)) != nil {
				t.Fatal("source recovery notification")
			}
		})
	}
}

func TestUpgradeSilencesLegacySourcesWithoutReplayingSevereFault(t *testing.T) {
	for _, stillActive := range []bool{false, true} {
		s := freshState()
		s.Incidents["source_hupu"] = incident{Since: epoch, LastAttempt: epoch, Accepted: true, Summary: "minor"}
		s.Incidents["proxy_auth"] = incident{Since: epoch, LastAttempt: epoch, Accepted: true, Summary: "severe"}
		active := map[string]string{"proxy_auth": "severe"}
		if stillActive {
			active["source_hupu"] = "minor"
		}
		if prepare(&s, active, epoch.Add(2*time.Hour)) != nil {
			t.Fatal("upgrade replayed notification")
		}
		if v, ok := s.Incidents["source_hupu"]; ok && (!v.LastAttempt.IsZero() || v.Accepted) {
			t.Fatal("legacy source attempt retained")
		}
		n := prepare(&s, active, epoch.Add(6*time.Hour))
		if n == nil || !strings.Contains(n.Body, "- severe") || strings.Contains(n.Body, "- minor") {
			t.Fatal("severe reminder missing or diagnostic leaked")
		}
		n = prepare(&s, map[string]string{}, epoch.Add(7*time.Hour))
		if n == nil || !strings.Contains(n.Title, "恢复") || strings.Contains(n.Body, "- minor") {
			t.Fatal("severe recovery missing or diagnostic leaked")
		}
	}
}

func TestDiagnosticAndUnknownKeysCannotLeakIntoSevereNotice(t *testing.T) {
	s := freshState()
	active := map[string]string{"source_guba": "minor", "unknown": "unknown diagnostic", "budget_daily": "severe"}
	prepare(&s, active, epoch)
	n := prepare(&s, active, epoch.Add(15*time.Minute))
	if n == nil || !strings.Contains(n.Body, "- severe") || strings.Contains(n.Body, "- minor") || strings.Contains(n.Body, "unknown diagnostic") {
		t.Fatal("notification allowlist failed")
	}
	if prepare(&s, active, epoch.Add(time.Hour)) != nil {
		t.Fatal("old hourly repeat survived")
	}
}
