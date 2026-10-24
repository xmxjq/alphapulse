"""Run directly in an interactive terminal; never capture its QR in logs."""

import argparse
import logging
import sys

from alphapulse.sources.bilibili.login import LoginFailure, run_qr_login


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    if not sys.stdout.isatty():
        print("Run this login in an interactive terminal.")
        return 2
    logging.disable(logging.CRITICAL)
    from bili_cli import auth
    from curl_cffi import requests
    import qrcode

    def render(url: str) -> None:
        print("Scan with the Bilibili app and confirm on your phone.")
        compact = auth._render_compact_qr(url)
        if compact:
            print(compact)
        else:
            qr = qrcode.QRCode()
            qr.add_data(url)
            qr.make(fit=True)
            qr.print_ascii(tty=True)

    try:
        with requests.Session(
            impersonate="chrome",
            headers={"Referer": "https://www.bilibili.com/"},
        ) as session:
            credential = run_qr_login(
                session,
                render_qr=render,
                credential_factory=auth.Credential,
                save_credential=auth.save_credential,
            )
        saved = auth._load_saved_credential()
        if saved is None or saved.sessdata != credential.sessdata:
            print("Credential file readback failed.")
            return 1
        print("Login verified. Credentials saved and read back successfully.")
        return 0
    except LoginFailure as exc:
        print(str(exc))
        return 1
    except Exception as exc:
        print(f"Login failed ({type(exc).__name__}); no credential details were printed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
