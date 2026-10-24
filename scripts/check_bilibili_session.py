"""Read-only authentication check; never refresh, clear, or print credentials."""

import argparse
import json
import logging
from pathlib import Path

from curl_cffi import requests

from alphapulse.runtime.config import load_settings
from alphapulse.sources.bilibili.space_cli import BilibiliCliSpaceDiscoveryClient, _import_bili_cli


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        settings = load_settings(args.config)
        auth, _ = _import_bili_cli()
        saved = auth._load_saved_credential()
        client = BilibiliCliSpaceDiscoveryClient(cookies=settings.sources.bilibili.cookies)
        credential = saved if saved is not None else client._credential(auth)
        if credential is None:
            print(json.dumps({"authenticated": False, "reason": "no_credentials"}))
            return 1
        response = requests.get(
            "https://api.bilibili.com/x/web-interface/nav",
            cookies=credential.get_cookies(),
            headers={"Referer": "https://www.bilibili.com/"},
            impersonate="chrome",
            timeout=20,
        )
        payload = response.json()
        authenticated = response.status_code == 200 and payload.get("code") == 0 and (payload.get("data") or {}).get("isLogin") is True
        print(json.dumps({
            "authenticated": authenticated,
            "credential_source": "saved_cli" if saved is not None else "configured_cookies",
            "http_status": response.status_code,
            "api_code": payload.get("code"),
        }))
        return 0 if authenticated else 1
    except Exception as exc:
        print(json.dumps({"authenticated": None, "error_type": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
