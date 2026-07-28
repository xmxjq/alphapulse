package main

import (
	"net"
	"net/http"
	"net/url"
	"testing"
)

func TestValidateTarget(t *testing.T) {
	allowed := map[string]struct{}{"guba.eastmoney.com": {}}
	good, _ := url.Parse("https://guba.eastmoney.com/list,600519.html")
	if err := validateTarget(good, allowed); err != nil {
		t.Fatalf("expected allowed URL: %v", err)
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
	})
	if headers.Get("User-Agent") != "test" || headers.Get("Accept") != "text/html" {
		t.Fatal("expected safe headers to be copied")
	}
	if headers.Get("Cookie") != "" || headers.Get("Authorization") != "" {
		t.Fatal("sensitive headers must not be copied")
	}
}
