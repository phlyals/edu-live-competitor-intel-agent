#!/usr/bin/env python3
"""Decode a saved QR image into a validated Douyin profile/room URL."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse, urlunparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from PIL import Image
import zxingcpp

from runtime_common import load_config, utc_now


SHARED_PYTHON = Path("/Volumes/ExternalStorage/AgentInfrastructure/isolated/shared/avtranscribe/.venv/bin/python")
HOME_RESOLVER = Path(__file__).resolve().with_name("resolve_douyin_home.py")


def emit(payload: dict) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("status") in {"READY", "READY_SHORT_LINK"} else 1


def is_douyin_host(host: str | None) -> bool:
    normalized = (host or "").lower().rstrip(".")
    return normalized == "douyin.com" or normalized.endswith(".douyin.com")


def confined(path: Path, root: Path, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError(f"{label} must stay inside analysis_drafts")
    return resolved


class DouyinOnlyRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        if not is_douyin_host(urlparse(newurl).hostname):
            raise ValueError("Short-link redirect left the allowed Douyin domains")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def resolve_short_link(url: str) -> str:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    opener = build_opener(DouyinOnlyRedirects())
    with opener.open(request, timeout=15) as response:
        final_url = response.geturl()
        response.read(1024)
    if not is_douyin_host(urlparse(final_url).hostname):
        raise ValueError("Resolved URL is not on an allowed Douyin domain")
    return final_url


def resolve_recorder_input(url: str) -> dict:
    try:
        process = subprocess.run(
            [str(SHARED_PYTHON), str(HOME_RESOLVER), "--url", url],
            capture_output=True, text=True, check=False, timeout=30,
        )
        payload = json.loads(process.stdout)
        if process.returncode != 0 or payload.get("status") != "READY":
            raise ValueError(payload.get("reason") or "Upstream homepage resolver failed")
        return {
            "recorder_input_status": "READY",
            "douyin_unique_id": payload.get("douyin_unique_id"),
            "monitor_url": payload.get("monitor_url"),
            "monitor_url_source": payload.get("source"),
        }
    except Exception as exc:
        return {
            "recorder_input_status": "UNKNOWN",
            "recorder_input_reason": str(exc),
        }


def normalize_payload(payload: str, follow_short_link: bool) -> dict:
    parsed = urlparse(payload.strip())
    if parsed.scheme not in {"http", "https"}:
        return {"status": "WAITING_TOOL", "reason": "QR payload is not an HTTP(S) URL", "qr_payload": payload}
    if not is_douyin_host(parsed.hostname):
        return {"status": "REJECTED", "reason": "QR URL is outside the allowed Douyin domains", "qr_payload": payload}

    final_url = payload.strip()
    if (parsed.hostname or "").lower() == "v.douyin.com":
        if not follow_short_link:
            return {
                "status": "READY_SHORT_LINK",
                "qr_payload": payload,
                "monitor_url": final_url,
                "reason": "Short link is accepted by the reused recorder; canonical resolution was not requested",
            }
        final_url = resolve_short_link(final_url)
        parsed = urlparse(final_url)

    canonical = urlunparse((parsed.scheme, parsed.netloc.lower(), parsed.path.rstrip("/"), "", "", ""))
    parts = [part for part in parsed.path.split("/") if part]
    sec_uid = parts[1] if len(parts) >= 2 and parts[0] == "user" else None
    kind = "DOUYIN_PROFILE" if sec_uid else "DOUYIN_LIVE_ROOM" if (parsed.hostname or "").lower() == "live.douyin.com" else "DOUYIN_URL"
    return {
        "status": "READY",
        "qr_payload": payload,
        "resolved_url": final_url,
        "canonical_url": canonical,
        "monitor_url": canonical,
        "url_kind": kind,
        "sec_uid": sec_uid,
    }


def self_test() -> int:
    expected = "https://www.douyin.com/user/MS4wLjAB-local-self-test"
    barcode = zxingcpp.create_barcode(expected, zxingcpp.BarcodeFormat.QRCode, ec_level="M")
    image = Image.fromarray(barcode.to_image(scale=8))
    results = zxingcpp.read_barcodes(image, formats=zxingcpp.BarcodeFormat.QRCode)
    decoded = results[0].text if results else None
    normalized = normalize_payload(decoded or "", False) if decoded else {"status": "ERROR"}
    ready = decoded == expected and normalized.get("status") == "READY" and normalized.get("sec_uid") == "MS4wLjAB-local-self-test"
    return emit({
        "status": "READY" if ready else "ERROR",
        "checked_at": utc_now(),
        "implementation": str(Path(__file__).resolve()),
        "decoder": "zxing-cpp",
        "decoder_version": importlib.metadata.version("zxing-cpp"),
        "image_loader": "Pillow",
        "image_loader_version": importlib.metadata.version("Pillow"),
        "synthetic_qr_decoded": decoded == expected,
        "normalization_validated": normalized.get("status") == "READY",
        "network_request_performed": False,
    })


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--account-name")
    parser.add_argument("--buyin-uid")
    parser.add_argument("--douyin-id", help="Douyin ID visibly displayed beside the QR code")
    parser.add_argument("--resolve-short-link", action="store_true")
    parser.add_argument("--resolve-recorder-input", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.input or not args.output:
        return emit({"status": "ERROR", "reason": "--input and --output are required"})

    config = load_config()
    root = Path(config["storage"]["directories"]["analysis_drafts"]).resolve()
    source = args.input.expanduser().resolve()
    if not source.is_file():
        return emit({"status": "WAITING_HUMAN", "reason": "QR input image does not exist", "input": str(source)})
    if source.stat().st_size > 20 * 1024 * 1024:
        return emit({"status": "REJECTED", "reason": "QR input image exceeds the 20 MiB safety limit"})
    destination = confined(args.output, root, "output")
    with Image.open(source) as image:
        results = zxingcpp.read_barcodes(image, formats=zxingcpp.BarcodeFormat.QRCode)
    if not results:
        return emit({"status": "WAITING_HUMAN", "reason": "No QR code was decoded from the saved image", "input": str(source)})
    if len(results) != 1:
        return emit({"status": "WAITING_HUMAN", "reason": "Expected exactly one QR code in the image", "decoded_count": len(results)})

    normalized = normalize_payload(results[0].text, args.resolve_short_link)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    preserved_dir = root / "qr"
    preserved_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    preserved = preserved_dir / f"{digest}.png"
    if not preserved.exists():
        shutil.copy2(source, preserved)
        preserved.chmod(0o600)
    if args.douyin_id:
        if not all(char.isalnum() or char in "._-" for char in args.douyin_id):
            return emit({"status": "REJECTED", "reason": "--douyin-id contains unsupported characters"})
        recorder_input = {
            "recorder_input_status": "READY",
            "douyin_unique_id": args.douyin_id,
            "monitor_url": f"https://live.douyin.com/{args.douyin_id}",
            "monitor_url_source": "Buyin QR card visible Douyin ID",
        }
    else:
        recorder_input = resolve_recorder_input(results[0].text) if args.resolve_recorder_input else {}
    record = {
        "schema_version": 1,
        "profile_id": config["profile_id"],
        "account_name": args.account_name,
        "buyin_creator_uid": args.buyin_uid,
        "source_input": str(source),
        "qr_image": str(preserved),
        "qr_image_sha256": digest,
        "decoded_at": utc_now(),
        "decoder": "zxing-cpp",
        "decoder_version": importlib.metadata.version("zxing-cpp"),
        **normalized,
        **recorder_input,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(destination)
    return emit({**record, "output": str(destination)})


if __name__ == "__main__":
    raise SystemExit(main())
