#!/usr/bin/env python3
"""Generate an xray client config from proxy share links.

Takes one or more vless:// / vmess:// / trojan:// share links and emits a
complete xray config: one HTTP-proxy inbound per link (ports counting up from
--start-port), one outbound per link, and routing rules pairing them — the
layout expected by the compose "xray" service and the crawler's static_list
proxy provider (docs/xray-proxy.md).

Usage:
    python3 scripts/xray_config_from_links.py 'vless://...' 'trojan://...' \
        > xray/config.json

The matching [crawl.static_proxies] urls list is printed to stderr.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from urllib.parse import parse_qs, unquote, urlsplit


def _first(query: dict[str, list[str]], key: str, default: str = "") -> str:
    values = query.get(key)
    return values[0] if values else default


def _build_stream_settings(network: str, security: str, opts: dict[str, str]) -> dict:
    stream: dict = {"network": network or "tcp"}
    if security in ("tls", "reality"):
        stream["security"] = security
        tls: dict = {}
        if opts.get("sni"):
            tls["serverName"] = opts["sni"]
        if opts.get("fp"):
            tls["fingerprint"] = opts["fp"]
        if security == "reality":
            tls["publicKey"] = opts.get("pbk", "")
            if opts.get("sid"):
                tls["shortId"] = opts["sid"]
            if opts.get("spx"):
                tls["spiderX"] = opts["spx"]
            stream["realitySettings"] = tls
        else:
            if opts.get("alpn"):
                tls["alpn"] = opts["alpn"].split(",")
            stream["tlsSettings"] = tls
    if network == "ws":
        ws: dict = {"path": opts.get("path") or "/"}
        if opts.get("host"):
            ws["host"] = opts["host"]
        stream["wsSettings"] = ws
    elif network == "grpc":
        stream["grpcSettings"] = {"serviceName": opts.get("serviceName", "")}
    elif network in ("http", "h2"):
        stream["httpSettings"] = {
            "path": opts.get("path") or "/",
            "host": [opts["host"]] if opts.get("host") else [],
        }
    elif network == "xhttp":
        xhttp: dict = {"path": opts.get("path") or "/"}
        if opts.get("host"):
            xhttp["host"] = opts["host"]
        stream["xhttpSettings"] = xhttp
    return stream


def _query_opts(query: dict[str, list[str]]) -> dict[str, str]:
    return {
        key: unquote(_first(query, key))
        for key in ("sni", "fp", "alpn", "host", "path", "serviceName", "pbk", "sid", "spx")
        if _first(query, key)
    }


def _parse_vless(link: str) -> dict:
    parts = urlsplit(link)
    if not parts.username or not parts.hostname or not parts.port:
        raise ValueError("vless link must look like vless://uuid@host:port?...")
    query = parse_qs(parts.query)
    user: dict = {
        "id": unquote(parts.username),
        "encryption": _first(query, "encryption", "none") or "none",
    }
    if _first(query, "flow"):
        user["flow"] = _first(query, "flow")
    return {
        "protocol": "vless",
        "settings": {
            "vnext": [
                {"address": parts.hostname, "port": parts.port, "users": [user]}
            ]
        },
        "streamSettings": _build_stream_settings(
            _first(query, "type", "tcp"),
            _first(query, "security", "none"),
            _query_opts(query),
        ),
    }


def _parse_trojan(link: str) -> dict:
    parts = urlsplit(link)
    if not parts.username or not parts.hostname or not parts.port:
        raise ValueError("trojan link must look like trojan://password@host:port?...")
    query = parse_qs(parts.query)
    return {
        "protocol": "trojan",
        "settings": {
            "servers": [
                {
                    "address": parts.hostname,
                    "port": parts.port,
                    "password": unquote(parts.username),
                }
            ]
        },
        # trojan share links default to tls
        "streamSettings": _build_stream_settings(
            _first(query, "type", "tcp"),
            _first(query, "security", "tls") or "tls",
            _query_opts(query),
        ),
    }


def _parse_vmess(link: str) -> dict:
    encoded = link[len("vmess://") :]
    payload = json.loads(base64.b64decode(encoded + "=" * (-len(encoded) % 4)))
    opts = {
        key: str(payload[src])
        for key, src in (("sni", "sni"), ("fp", "fp"), ("alpn", "alpn"), ("host", "host"), ("path", "path"))
        if payload.get(src)
    }
    return {
        "protocol": "vmess",
        "settings": {
            "vnext": [
                {
                    "address": payload["add"],
                    "port": int(payload["port"]),
                    "users": [
                        {
                            "id": payload["id"],
                            "alterId": int(payload.get("aid", 0)),
                            "security": payload.get("scy", "auto") or "auto",
                        }
                    ],
                }
            ]
        },
        "streamSettings": _build_stream_settings(
            str(payload.get("net", "tcp")),
            "tls" if payload.get("tls") in ("tls", True) else "none",
            opts,
        ),
    }


def parse_link(link: str) -> dict:
    link = link.strip()
    if link.startswith("vless://"):
        return _parse_vless(link)
    if link.startswith("trojan://"):
        return _parse_trojan(link)
    if link.startswith("vmess://"):
        return _parse_vmess(link)
    raise ValueError(f"Unsupported share link scheme: {link.split('://', 1)[0]}://")


def build_config(links: list[str], start_port: int) -> tuple[dict, list[str]]:
    inbounds, outbounds, rules, proxy_urls = [], [], [], []
    for index, link in enumerate(links, start=1):
        port = start_port + index - 1
        outbound = parse_link(link)
        outbound["tag"] = f"out-tunnel-{index}"
        inbounds.append(
            {
                "tag": f"in-tunnel-{index}",
                "listen": "0.0.0.0",
                "port": port,
                "protocol": "http",
            }
        )
        outbounds.append(outbound)
        rules.append(
            {
                "type": "field",
                "inboundTag": [f"in-tunnel-{index}"],
                "outboundTag": f"out-tunnel-{index}",
            }
        )
        proxy_urls.append(f"http://xray:{port}")
    config = {
        "log": {"loglevel": "warning"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "routing": {"rules": rules},
    }
    return config, proxy_urls


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("links", nargs="+", help="vless:// / vmess:// / trojan:// share links")
    parser.add_argument("--start-port", type=int, default=10809)
    args = parser.parse_args()

    config, proxy_urls = build_config(args.links, args.start_port)
    json.dump(config, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    print("\n[crawl.static_proxies]", file=sys.stderr)
    print(f"urls = {json.dumps(proxy_urls)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
