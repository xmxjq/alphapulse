"""Interactive web QR login with verified, non-empty credentials."""

import time
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit


GENERATE_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
POLL_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
QR_LOGIN_HOSTS = frozenset({"passport.bilibili.com", "account.bilibili.com"})
COOKIE_FIELDS = {
    "SESSDATA": "sessdata",
    "bili_jct": "bili_jct",
    "DedeUserID": "dedeuserid",
    "buvid3": "buvid3",
    "buvid4": "buvid4",
}


class LoginFailure(RuntimeError):
    """A fixed diagnostic, never a response body or credential value."""


def validate_qr_destination(url: str, expected_key: str) -> None:
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname in QR_LOGIN_HOSTS
            and parsed.port in (None, 443)
            and parsed.username is None
            and not parsed.fragment
            and not any(char in url for char in "\r\n\t")
            and parse_qs(parsed.query).get("qrcode_key") == [expected_key]
        )
    except ValueError:
        valid = False
    if not valid:
        raise LoginFailure("Unexpected QR destination.")


def credential_fields(data: dict[str, Any], cookies: dict[str, str]) -> dict[str, str]:
    if data.get("code") != 0:
        raise LoginFailure("QR approval has not completed.")
    query = parse_qs(urlsplit(str(data.get("url") or "")).query)
    fields = {
        field: cookies.get(name) or next(iter(query.get(name, [])), "")
        for name, field in COOKIE_FIELDS.items()
    }
    if not fields["sessdata"]:
        raise LoginFailure("No session returned; existing credentials were not changed.")
    fields["ac_time_value"] = str(data.get("refresh_token") or "")
    return fields


def _json_response(response) -> dict[str, Any]:
    if response.status_code != 200:
        raise LoginFailure(f"Login endpoint returned HTTP {response.status_code}.")
    try:
        payload = response.json()
    except (ValueError, TypeError):
        raise LoginFailure("Login endpoint returned a non-JSON response.") from None
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise LoginFailure("Login endpoint rejected the request.")
    return payload


def run_qr_login(
    session,
    *,
    render_qr: Callable[[str], None],
    credential_factory,
    save_credential,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    timeout: float = 180,
):
    generated = _json_response(session.get(
        GENERATE_URL, params={"source": "main-fe-header"}, timeout=20, allow_redirects=False,
    )).get("data")
    if not isinstance(generated, dict) or not generated.get("qrcode_key") or not generated.get("url"):
        raise LoginFailure("Login endpoint did not return a QR challenge.")
    qr_url = str(generated["url"])
    validate_qr_destination(qr_url, generated["qrcode_key"])
    render_qr(qr_url)
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        response = session.get(
            POLL_URL,
            params={"qrcode_key": generated["qrcode_key"], "source": "main-fe-header"},
            timeout=20,
            allow_redirects=False,
        )
        data = _json_response(response).get("data")
        if not isinstance(data, dict):
            raise LoginFailure("Login endpoint returned an invalid state.")
        code = data.get("code")
        if code in (86101, 86090):
            sleep(2)
            continue
        if code == 86038:
            raise LoginFailure("QR challenge expired; run login again.")
        if code != 0:
            raise LoginFailure("Login endpoint returned an unrecognized state.")
        cookies = session.cookies.get_dict()
        cookies.update(response.cookies.get_dict())
        fields = credential_fields(data, cookies)
        credential = credential_factory(**fields)
        validated = _json_response(session.get(
            NAV_URL, cookies=credential.get_cookies(), timeout=20, allow_redirects=False,
        ))
        if (validated.get("data") or {}).get("isLogin") is not True:
            raise LoginFailure("Session validation failed; existing credentials were not changed.")
        save_credential(credential)
        return credential
    raise LoginFailure("Login timed out; run login again.")
