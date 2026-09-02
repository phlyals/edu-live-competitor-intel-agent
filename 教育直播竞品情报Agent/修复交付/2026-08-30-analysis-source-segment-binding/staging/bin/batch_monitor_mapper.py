#!/usr/bin/env python3
"""Read current Buyin QR cards for the latest MVP-A unique competitors.

The script uses the already logged-in Tabbit window only.  It never writes
SQLite, Feishu, recording configuration, or the original MVP-A result.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

sys.path.insert(0, str(Path(__file__).resolve().parent))

from browser_scanner import (  # noqa: E402
    CuaClient,
    NativeTabbitScanner,
    ScanFailure,
    all_visible_text,
    exact_nodes,
    extract_snapshot_url,
    find_action,
    node_ref,
    verify_selected_tab,
)


OUTPUT_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/batch-monitor")
PRODUCT_RESULT = Path(
    "/Volumes/ExternalStorage/同行直播录制/analysis/drafts/mvp-a-direct/20260823-092702/result.json"
)
CHEN_MAPPING = Path(
    "/Volumes/ExternalStorage/同行直播录制/analysis/drafts/mvp-d/20260823-224002/account_mapping.json"
)
DETAIL_PATH = "/dashboard/followed-daren"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def compact_time_id() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def atomic_write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(json_bytes(value))
    temporary.chmod(0o600)
    temporary.replace(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def confined_output(path: Path) -> Path:
    root = OUTPUT_ROOT.resolve()
    resolved = path.expanduser().resolve()
    if root not in resolved.parents:
        raise ValueError("Output directory must be a child of batch-monitor")
    return resolved


def candidate_rows(result: dict) -> list[dict]:
    grouped: dict[str, dict] = {}
    for observation in result.get("observations") or []:
        name = str(observation.get("account_name") or "").strip()
        if not name:
            continue
        item = grouped.setdefault(name, {
            "name": name,
            "observation_count": 0,
            "historical_buyin_uids": set(),
        })
        item["observation_count"] += 1
        uid = observation.get("buyin_creator_uid")
        if isinstance(uid, str) and uid.startswith("v2_"):
            item["historical_buyin_uids"].add(uid)
    return [
        {
            "name": item["name"],
            "observation_count": item["observation_count"],
            "historical_buyin_uids": sorted(item["historical_buyin_uids"]),
        }
        for item in sorted(grouped.values(), key=lambda value: value["name"])
    ]


def canonical_detail(url: str, uid: str) -> str:
    parsed = urlparse(url)
    pairs = [(key, value) for key, value in parse_qs(parsed.query, keep_blank_values=True).items()]
    daren_type = next((value[0] for key, value in pairs if key == "daren_type" and value), "1")
    return urlunparse(("https", "buyin.jinritemai.com", DETAIL_PATH, "", urlencode({"daren_type": daren_type, "uid": uid}), ""))


def text_value(node: dict) -> str:
    return str(node.get("value") or node.get("label") or node.get("description") or "").strip()


def element_by_role(snapshot: dict, role: str, predicate) -> dict | None:
    for node in snapshot.get("elements") or []:
        if node.get("role") == role and predicate(node):
            return node
    return None


def visible_detail_uid(snapshot: dict) -> tuple[str | None, str | None]:
    url = extract_snapshot_url(snapshot) or ""
    parsed = urlparse(url)
    if parsed.hostname != "buyin.jinritemai.com" or parsed.path != DETAIL_PATH:
        return None, None
    values = parse_qs(parsed.query).get("uid") or []
    return (values[0], url) if len(values) == 1 and values[0].startswith("v2_") else (None, None)


def has_human_block(snapshot: dict) -> bool:
    text = all_visible_text(snapshot)
    return any(token in text for token in ("验证码", "安全验证", "访问频繁", "风控", "重新登录", "登录失效"))


def safe_click(scanner: NativeTabbitScanner, node: dict) -> dict:
    reference = node_ref(node)
    if not reference:
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "页面元素缺少可操作引用")
    try:
        scanner.client.call("click", {**scanner._window(), "element_token": reference, "delivery_mode": "foreground"})
    except ScanFailure:
        # A native click may finish navigation before its response is emitted.
        # The caller validates the next snapshot instead of treating that as a
        # successful identity mapping.
        pass
    time.sleep(0.6)
    return scanner.snapshot()


def switch_to_product(scanner: NativeTabbitScanner) -> dict:
    snapshot = scanner.snapshot()
    if "/dashboard/merch-picking-library/merch-promoting" in (extract_snapshot_url(snapshot) or ""):
        return snapshot
    saved_product_url = getattr(scanner, "_batch_product_url", None)
    if isinstance(saved_product_url, str) and saved_product_url:
        # This mapping tab is owned by the current batch.  Reuse it instead of
        # cycling across the user's other browser tabs after every detail page.
        return scanner.navigate_raw(saved_product_url)
    tab = find_action(snapshot, ("商品决策页",))
    if tab:
        snapshot = safe_click(scanner, tab)
    for _ in range(12):
        if "/dashboard/merch-picking-library/merch-promoting" in (extract_snapshot_url(snapshot) or ""):
            return snapshot
        scanner.client.call("hotkey", {**scanner._window(), "keys": ["ctrl", "shift", "tab"], "delivery_mode": "foreground"})
        time.sleep(0.4)
        snapshot = scanner.snapshot()
    raise ScanFailure("TABBIT_TAB_AMBIGUOUS", "无法回到商品决策页")


def ensure_live_list(scanner: NativeTabbitScanner) -> dict:
    snapshot = switch_to_product(scanner)
    if has_human_block(snapshot):
        raise ScanFailure("PAGE_BLOCKED", "商品页出现登录、验证码或风控提示")
    # A URL match is not a DOM-ready signal. Wait for the decision-page tab
    # controls before interacting; otherwise the mapper misclassifies a
    # transient loading AX tree as a permanent page-structure change.
    for _ in range(30):
        if exact_nodes(snapshot, "带货内容"):
            break
        time.sleep(0.4)
        snapshot = scanner.snapshot()
    if not exact_nodes(snapshot, "带货内容"):
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "商品决策页标签尚未加载")
    if not verify_selected_tab(snapshot, "带货内容"):
        snapshot = scanner.click_label(snapshot, "带货内容")
    if not verify_selected_tab(snapshot, "直播"):
        snapshot = scanner.click_label(snapshot, "直播")
    if not (verify_selected_tab(snapshot, "带货内容") and verify_selected_tab(snapshot, "直播")):
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "无法验证带货内容→直播标签")
    # Buyin first exposes the tab state and then replaces the result surface.
    # Do not read a stale video table as a live search result.
    for _ in range(24):
        search_button = element_by_role(
            snapshot,
            "AXButton",
            lambda node: text_value(node) == "search",
        )
        search_parent = search_button.get("parent_index") if search_button else None
        search_input = element_by_role(
            snapshot,
            "AXTextField",
            lambda node: node.get("parent_index") == search_parent and node.get("label") != "地址和搜索栏",
        )
        if search_input and search_button:
            return snapshot
        time.sleep(0.35)
        snapshot = scanner.snapshot()
    raise ScanFailure("PAGE_STRUCTURE_CHANGED", "直播列表搜索控件未在页面加载后出现")


def map_candidate(scanner: NativeTabbitScanner, candidate: dict) -> dict:
    name = candidate["name"]
    result = {
        **candidate,
        "current_buyin_uid": None,
        "buyin_detail_url": None,
        "detail_display_name": None,
        "douyin_id": None,
        "douyin_profile_url": None,
        "live_monitor_url": None,
        "mapping_status": "UNRESOLVED",
        "mapping_evidence": [],
        "conflict_reason": None,
        "error": None,
    }
    detail_opened = False
    try:
        snapshot = ensure_live_list(scanner)
        search_button = element_by_role(
            snapshot,
            "AXButton",
            lambda node: text_value(node) == "search",
        )
        search_parent = search_button.get("parent_index") if search_button else None
        search_input = element_by_role(
            snapshot,
            "AXTextField",
            lambda node: node.get("parent_index") == search_parent and node.get("label") != "地址和搜索栏",
        )
        if not search_input or not search_button:
            raise ScanFailure("PAGE_STRUCTURE_CHANGED", "直播列表搜索控件缺失")
        scanner.client.call("set_value", {**scanner._window(), "element_token": node_ref(search_input), "value": name})
        snapshot = safe_click(scanner, search_button)

        account_node = None
        for _ in range(16):
            if has_human_block(snapshot):
                raise ScanFailure("PAGE_BLOCKED", "搜索结果页出现登录、验证码或风控提示")
            account_node = element_by_role(snapshot, "AXStaticText", lambda node: text_value(node) == name)
            # The result list can include a page heading only in fixtures.  A
            # real candidate needs a matching fan-count sibling rendered here.
            if account_node and "粉丝数" in all_visible_text(snapshot):
                break
            account_node = None
            time.sleep(0.35)
            snapshot = scanner.snapshot()
        if not account_node:
            raise ScanFailure("ACCOUNT_NOT_FOUND", "直播结果中没有找到该同行")
        result["mapping_evidence"].append("filtered_live_result_exact_name")
        safe_click(scanner, account_node)

        detail_url = None
        uid = None
        for _ in range(20):
            snapshot = scanner.snapshot()
            if has_human_block(snapshot):
                raise ScanFailure("PAGE_BLOCKED", "达人详情页出现登录、验证码或风控提示")
            uid, detail_url = visible_detail_uid(snapshot)
            if uid:
                break
            time.sleep(0.35)
        if not uid or not detail_url:
            raise ScanFailure("DETAIL_PAGE_UNAVAILABLE", "未能验证达人详情URL UID")
        detail_opened = True
        result["current_buyin_uid"] = uid
        result["buyin_detail_url"] = canonical_detail(detail_url, uid)
        result["mapping_evidence"].append("current_buyin_detail_url_uid")

        # The QR trigger is an unlabeled icon on Buyin.  These coordinates are
        # derived from the current 1568×776 native Tabbit capture and only hit
        # the profile card's QR affordance.  Every click is followed by an
        # evidence read; a missing QR remains unresolved.
        qr_text = all_visible_text(snapshot)
        for x, y in ((337, 225), (389, 261), (336, 300)):
            if "抖音号：" in qr_text:
                break
            scanner.client.call("click", {**scanner._window(), "x": x, "y": y, "delivery_mode": "foreground"})
            time.sleep(0.5)
            snapshot = scanner.snapshot()
            qr_text = all_visible_text(snapshot)
        match = re.search(r"抖音号：\s*(\d{5,30})", qr_text)
        if not match:
            raise ScanFailure("QR_CARD_UNAVAILABLE", "未能读取达人详情二维码卡片中的抖音号")
        result["douyin_id"] = match.group(1)
        result["live_monitor_url"] = f"https://live.douyin.com/{match.group(1)}"
        result["mapping_evidence"].append("current_buyin_qr_card_douyin_id")

        elements = snapshot.get("elements") or []
        label_index = next(
            (index for index, node in enumerate(elements) if text_value(node) == "抖音号："),
            None,
        )
        if label_index is not None:
            for node in reversed(elements[max(0, label_index - 6):label_index]):
                value = text_value(node)
                if node.get("role") == "AXStaticText" and value and value != "抖音号：":
                    result["detail_display_name"] = value
                    break
        if result["detail_display_name"] is None:
            result["detail_display_name"] = name

        historical = set(result["historical_buyin_uids"])
        if historical and uid not in historical:
            result["mapping_status"] = "IDENTITY_CONFLICT"
            result["conflict_reason"] = "historical_buyin_uid_differs_from_current_detail_uid"
        else:
            result["mapping_status"] = "VERIFIED"
    except ScanFailure as exc:
        result["mapping_status"] = "PAGE_BLOCKED" if exc.error_type == "PAGE_BLOCKED" else "UNRESOLVED"
        result["error"] = {"type": exc.error_type, "message": str(exc)}
    except Exception as exc:  # preserve batch progress and never guess identity
        result["mapping_status"] = "UNRESOLVED"
        result["error"] = {"type": "MAPPING_INTERNAL_ERROR", "message": exc.__class__.__name__}
    finally:
        if detail_opened:
            try:
                saved_product_url = getattr(scanner, "_batch_product_url", None)
                if isinstance(saved_product_url, str) and saved_product_url:
                    scanner.navigate_raw(saved_product_url)
            except Exception:
                pass
    return result


def prior_chen(candidate: dict) -> dict:
    mapping = json.loads(CHEN_MAPPING.read_text(encoding="utf-8"))
    return {
        **candidate,
        "current_buyin_uid": mapping.get("buyin_creator_uid"),
        "buyin_detail_url": mapping.get("buyin_detail_url"),
        "detail_display_name": mapping.get("competitor_name"),
        "douyin_id": mapping.get("douyin_account_id"),
        "douyin_profile_url": mapping.get("douyin_account_url"),
        "live_monitor_url": mapping.get("live_monitor_url"),
        "mapping_status": "IDENTITY_CONFLICT" if mapping.get("historical_uid_conflict") else "VERIFIED",
        "mapping_evidence": ["existing_mvp_d_current_page_qr_card"],
        "conflict_reason": "historical_buyin_uid_differs_from_current_detail_uid" if mapping.get("historical_uid_conflict") else None,
        "error": None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--approved-current-tabbit", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if not args.approved_current_tabbit:
        parser.error("live mapping requires --approved-current-tabbit")
    if args.limit < 1 or args.limit > 20:
        parser.error("--limit must be between 1 and 20")
    if not PRODUCT_RESULT.is_file() or not CHEN_MAPPING.is_file():
        parser.error("required MVP-A result or existing Chen mapping is missing")

    output_dir = confined_output(args.output_dir or (OUTPUT_ROOT / compact_time_id()))
    output_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    source = json.loads(PRODUCT_RESULT.read_text(encoding="utf-8"))
    if (source.get("scan_summary") or {}).get("status") != "COMPLETE" or len(source.get("observations") or []) != 55:
        parser.error("source result is not the verified 55-observation COMPLETE scan")
    candidates = candidate_rows(source)
    results: list[dict] = []
    result_path = output_dir / "candidate_mappings.json"

    def persist_progress() -> None:
        atomic_write(result_path, {
            "schema_version": 1,
            "source_result": str(PRODUCT_RESULT),
            "mapped_at": utc_now(),
            "total_unique_competitors": len(candidates),
            "targets": results,
            "side_effects": {
                "sqlite_written": False,
                "feishu_base_written": False,
                "recording_config_written": False,
                "recording_started": False,
            },
        })

    scanner = NativeTabbitScanner(CuaClient(), output_dir)
    try:
        product_url, _ = scanner.start()
        scanner._batch_product_url = product_url
        for candidate in candidates:
            if candidate["name"] == "陈兴笃学":
                results.append(prior_chen(candidate))
            elif len(results) < args.limit:
                mapped = map_candidate(scanner, candidate)
                results.append(mapped)
                print(json.dumps({"name": mapped["name"], "mapping_status": mapped["mapping_status"]}, ensure_ascii=False), flush=True)
            else:
                results.append({
                    **candidate,
                    "current_buyin_uid": None,
                    "buyin_detail_url": None,
                    "detail_display_name": None,
                    "douyin_id": None,
                    "douyin_profile_url": None,
                    "live_monitor_url": None,
                    "mapping_status": "UNRESOLVED",
                    "mapping_evidence": [],
                    "conflict_reason": None,
                    "error": {"type": "LIMIT_REACHED", "message": "batch limit reached"},
                })
            persist_progress()
    finally:
        try:
            current = extract_snapshot_url(scanner.snapshot()) or ""
            if urlparse(current).hostname == "buyin.jinritemai.com":
                scanner.client.call(
                    "hotkey",
                    {**scanner._window(), "keys": ["ctrl", "w"], "delivery_mode": "foreground"},
                )
        except Exception:
            pass
        scanner.end()

    payload = {
        "schema_version": 1,
        "source_result": str(PRODUCT_RESULT),
        "mapped_at": utc_now(),
        "total_unique_competitors": len(candidates),
        "targets": results,
        "side_effects": {
            "sqlite_written": False,
            "feishu_base_written": False,
            "recording_config_written": False,
            "recording_started": False,
        },
    }
    atomic_write(result_path, payload)
    resolved_count = sum(1 for item in results if item.get("mapping_status") in {"VERIFIED", "IDENTITY_CONFLICT"})
    manifest = {
        "status": "COMPLETE" if resolved_count == len(results) else "PARTIAL",
        "output": str(result_path),
        "sha256": sha256_file(result_path),
        "mapped_count": len(results),
        "resolved_count": resolved_count,
    }
    atomic_write(output_dir / "mapping_manifest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
