package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestValidateTarget(t *testing.T) {
	allowed := map[string]struct{}{
		"guba.eastmoney.com":    {},
		"www.tgb.cn":            {},
		"app.jiuyangongshe.com": {},
		"bbs.hupu.com":          {},
	}
	good, _ := url.Parse("https://guba.eastmoney.com/list,600519.html")
	if err := validateTarget(good, allowed); err != nil {
		t.Fatalf("expected allowed URL: %v", err)
	}
	tgb, _ := url.Parse("https://www.tgb.cn/zongban/1/1")
	if err := validateTarget(tgb, allowed); err != nil {
		t.Fatalf("expected allowed TGB URL: %v", err)
	}
	jiuyan, _ := url.Parse(
		"https://app.jiuyangongshe.com/jystock-app/api/v2/article/detail",
	)
	if err := validateTarget(jiuyan, allowed); err != nil {
		t.Fatalf("expected allowed Jiuyan URL: %v", err)
	}
	hupu, _ := url.Parse("https://bbs.hupu.com/641433410.html")
	if err := validateTarget(hupu, allowed); err != nil {
		t.Fatalf("expected allowed Hupu URL: %v", err)
	}
	bad, _ := url.Parse("http://127.0.0.1/private")
	if err := validateTarget(bad, allowed); err == nil {
		t.Fatal("expected disallowed host")
	}
}

func TestUnsafeIP(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "10.0.0.1", "169.254.1.1", "::1"} {
		if !unsafeIP(net.ParseIP(raw)) {
			t.Fatalf("expected %s to be unsafe", raw)
		}
	}
	if unsafeIP(net.ParseIP("1.1.1.1")) {
		t.Fatal("expected public address to be safe")
	}
}

func TestCopyAllowedHeaders(t *testing.T) {
	headers := make(http.Header)
	copyAllowedHeaders(headers, map[string]string{
		"User-Agent":    "test",
		"Accept":        "text/html",
		"Cookie":        "secret",
		"Authorization": "secret",
		"Token":         "source-token",
	}, "guba")
	if headers.Get("User-Agent") != "test" || headers.Get("Accept") != "text/html" {
		t.Fatal("expected safe headers to be copied")
	}
	if headers.Get("Cookie") != "" ||
		headers.Get("Authorization") != "" ||
		headers.Get("Token") != "" {
		t.Fatal("sensitive headers must not be copied")
	}
}

func TestCopyAllowedJiuyanHeaders(t *testing.T) {
	headers := make(http.Header)
	copyAllowedHeaders(headers, map[string]string{
		"Origin":           "https://www.jiuyangongshe.com",
		"Platform":         "3",
		"Timestamp":        "123456",
		"Token":            "request-signature",
		"X-Requested-With": "XMLHttpRequest",
		"Cookie":           "secret",
		"Authorization":    "secret",
	}, "jiuyan")

	for _, key := range []string{
		"Origin",
		"Platform",
		"Timestamp",
		"Token",
		"X-Requested-With",
	} {
		if headers.Get(key) == "" {
			t.Fatalf("expected Jiuyan header %s", key)
		}
	}
	if headers.Get("Cookie") != "" || headers.Get("Authorization") != "" {
		t.Fatal("sensitive headers must not be copied")
	}
}

func TestRequestPacerSpacesRequests(t *testing.T) {
	pacer := &requestPacer{
		minimum: 20 * time.Millisecond,
		maximum: 20 * time.Millisecond,
	}
	ctx := context.Background()
	if err := pacer.wait(ctx); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := pacer.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond {
		t.Fatalf("second request was not paced: %s", elapsed)
	}
}

func TestDailyScheduleWithMultipleWindows(t *testing.T) {
	schedule, err := newDailySchedule(
		[]string{"08:30-12:00", "14:00-18:00", "20:00-23:00"},
		"Asia/Shanghai",
		0,
		"home-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	location, _ := time.LoadLocation("Asia/Shanghai")
	tests := []struct {
		hour   int
		minute int
		active bool
	}{
		{8, 29, false},
		{8, 30, true},
		{11, 59, true},
		{12, 0, false},
		{15, 0, true},
		{19, 0, false},
		{22, 59, true},
		{23, 0, false},
	}
	for _, test := range tests {
		now := time.Date(2026, time.July, 29, test.hour, test.minute, 0, 0, location)
		if got := schedule.activeAt(now); got != test.active {
			t.Fatalf(
				"activeAt(%s) = %v, want %v",
				now.Format(time.RFC3339),
				got,
				test.active,
			)
		}
	}
}

func TestDailyScheduleSupportsOvernightWindow(t *testing.T) {
	schedule, err := newDailySchedule(
		[]string{"22:00-02:00"},
		"UTC",
		0,
		"night-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, hour := range []int{22, 23, 0, 1} {
		now := time.Date(2026, time.July, 29, hour, 30, 0, 0, time.UTC)
		if !schedule.activeAt(now) {
			t.Fatalf("expected %s to be active", now.Format(time.RFC3339))
		}
	}
	if schedule.activeAt(time.Date(2026, time.July, 29, 2, 0, 0, 0, time.UTC)) {
		t.Fatal("expected end boundary to be inactive")
	}
}

func TestDailyScheduleJitterIsStableAndInward(t *testing.T) {
	schedule, err := newDailySchedule(
		[]string{"08:30-12:00"},
		"Asia/Shanghai",
		20*time.Minute,
		"home-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	location, _ := time.LoadLocation("Asia/Shanghai")
	day := time.Date(2026, time.July, 29, 0, 0, 0, 0, location)
	first := schedule.resolve(day, schedule.windows[0])
	second := schedule.resolve(day, schedule.windows[0])
	baseStart := day.Add(8*time.Hour + 30*time.Minute)
	baseEnd := day.Add(12 * time.Hour)
	if first != second {
		t.Fatal("daily jitter must be stable across calls")
	}
	if first.start.Before(baseStart) ||
		first.start.After(baseStart.Add(20*time.Minute)) {
		t.Fatalf("start jitter is outside configured bounds: %s", first.start)
	}
	if first.end.After(baseEnd) ||
		first.end.Before(baseEnd.Add(-20*time.Minute)) {
		t.Fatalf("end jitter is outside configured bounds: %s", first.end)
	}
}

func TestDailyScheduleNextStart(t *testing.T) {
	schedule, err := newDailySchedule(
		[]string{"08:30-12:00", "14:00-18:00"},
		"UTC",
		0,
		"home-1",
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 29, 12, 30, 0, 0, time.UTC)
	want := time.Date(2026, time.July, 29, 14, 0, 0, 0, time.UTC)
	if got := schedule.nextStart(now); !got.Equal(want) {
		t.Fatalf("nextStart() = %s, want %s", got, want)
	}
}

func TestDailyScheduleRejectsUnsafeJitter(t *testing.T) {
	if _, err := newDailySchedule(
		[]string{"08:00-09:00"},
		"UTC",
		30*time.Minute,
		"home-1",
	); err == nil {
		t.Fatal("expected jitter covering the entire window to be rejected")
	}
}

func TestParseClockMinuteRequiresHHMM(t *testing.T) {
	for _, raw := range []string{"8:30", "08:3", "08:30:00", "24:00"} {
		if _, err := parseClockMinute(raw); err == nil {
			t.Fatalf("expected %q to be rejected", raw)
		}
	}
	if got, err := parseClockMinute("08:30"); err != nil || got != 8*60+30 {
		t.Fatalf("parseClockMinute() = %d, %v", got, err)
	}
}
