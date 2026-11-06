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
	if prepare(&s, active, epoch.Add(10*time.Minute)) == nil {
		t.Fatal("missing notification")
	}
	if prepare(&s, active, epoch.Add(11*time.Minute)) != nil {
		t.Fatal("duplicate notification")
	}
	stamp := epoch.Add(12 * time.Minute)
	h.Sources[0].LastSuccess = &stamp
	n := prepare(&s, evaluate(&s, h, true, stamp), stamp)
	if n == nil || !strings.Contains(n.Title, "恢复") {
		t.Fatal("missing recovery")
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

func TestImmediateGuardsAndHourlyRepeat(t *testing.T) {
	s := freshState()
	h := healthy()
	h.Proxy.Enabled = true
	h.Proxy.Auth = true
	h.Proxy.Budgets = []string{"daily"}
	a := evaluate(&s, h, true, epoch)
	if len(a) != 2 {
		t.Fatal("guards missing")
	}
	if prepare(&s, a, epoch) == nil {
		t.Fatal("first notification missing")
	}
	if prepare(&s, a, epoch.Add(59*time.Minute)) != nil {
		t.Fatal("early repeat")
	}
	if prepare(&s, a, epoch.Add(time.Hour)) == nil {
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
	if save(p, s) != nil {
		t.Fatal("save failed")
	}
	loaded, err := load(p)
	if err != nil {
		t.Fatal("load failed")
	}
	if prepare(&loaded, map[string]string{"proxy_auth": "test"}, epoch.Add(time.Minute)) != nil {
		t.Fatal("restart replay")
	}
	if os.WriteFile(p, []byte("not-json"), 0600) != nil {
		t.Fatal("fixture failed")
	}
	if _, err = load(p); err == nil {
		t.Fatal("corrupt state reset silently")
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
