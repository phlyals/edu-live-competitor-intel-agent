#!/usr/bin/env python3
"""MVP-A read-only Giant Buyin live-content browser scanner.

Live mode controls the already-authenticated Tabbit window through cua-driver.
Fixture mode never calls cua-driver and exists only for deterministic tests.
No code path writes SQLite, Feishu, or starts a background service.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from scanner import observation_key, validate_pages, verified_identity


PROFILE_ID = "edu_live_competitor_intel"
CUA_DRIVER = Path("/Users/mac/.local/bin/cua-driver")
CUA_SOCKET = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/home/Library/Caches/cua-driver/cua-driver.sock")
DEFAULT_OUTPUT_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/analysis/drafts/mvp-a")
BUYIN_HOST = "buyin.jinritemai.com"
DECISION_PATH = "/dashboard/merch-picking-library/merch-promoting"
PRODUCT_ID_FIELDS = ("promotion_id", "id", "product_id", "commodity_id")
SAFE_NAV_QUERY_VALUES = {"id", "enter_from", "page_name", "decision_enter_from"}
CSV_FIELDS = (
    "source_page", "source_batch", "source_position", "collected_at",
    "live_title", "live_date", "account_name", "buyin_creator_uid",
    "identity_status", "follower_count", "location", "sales",
    "settlement_amount", "total_views", "peak_popularity", "click_rate",
    "order_conversion_rate", "profile_url_redacted",
)


class ScanFailure(RuntimeError):
    def __init__(self, error_type: str, message: str, *, incomplete: bool = False, evidence: dict | None = None):
        super().__init__(message)
        self.error_type = error_type
        self.incomplete = incomplete
        self.evidence = evidence or {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def compact_time_id() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")


def extract_product_id(value: str) -> tuple[str, list[str]]:
    """Extract one unambiguous numeric product id from an id or URL."""
    raw = value.strip()
    if re.fullmatch(r"\d{8,30}", raw):
        return raw, ["direct_product_id"]
    try:
        parsed = urlparse(raw)
    except ValueError as exc:
        raise ScanFailure("PRODUCT_ID_MISSING", "输入不是有效商品链接或商品ID") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ScanFailure("PRODUCT_ID_MISSING", "输入不是有效商品链接或商品ID")
    candidates: list[tuple[str, str]] = []
    for field, candidate in parse_qsl(parsed.query, keep_blank_values=True):
        if field in PRODUCT_ID_FIELDS and re.fullmatch(r"\d{8,30}", candidate or ""):
            candidates.append((field, candidate))
    distinct = sorted({candidate for _, candidate in candidates})
    fields = sorted({field for field, _ in candidates})
    if not distinct:
        raise ScanFailure("PRODUCT_ID_MISSING", "链接中无法可靠提取商品ID", evidence={"candidate_fields": fields})
    if len(distinct) != 1:
        raise ScanFailure(
            "PRODUCT_ID_AMBIGUOUS",
            "链接中存在多个不同的候选商品ID",
            evidence={"candidate_fields": fields},
        )
    return distinct[0], fields


def replace_decision_product_id(current_url: str, product_id: str) -> str:
    """Replace only the decision page's id query value, preserving all others."""
    parsed = urlparse(current_url)
    if parsed.hostname != BUYIN_HOST or parsed.path != DECISION_PATH:
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "当前Tabbit页不是巨量百应商品决策页")
    query = parse_qsl(parsed.query, keep_blank_values=True)
    id_positions = [index for index, (key, _) in enumerate(query) if key == "id"]
    if len(id_positions) != 1:
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "当前商品决策URL必须且只能包含一个id参数")
    rewritten = [(key, product_id if key == "id" else value) for key, value in query]
    return urlunparse(parsed._replace(query=urlencode(rewritten)))


def product_id_from_decision_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    if parsed.hostname != BUYIN_HOST or parsed.path != DECISION_PATH:
        return None
    values = [value for key, value in parse_qsl(parsed.query, keep_blank_values=True) if key == "id"]
    return values[0] if len(values) == 1 and re.fullmatch(r"\d{8,30}", values[0] or "") else None


def redact_url(url: str) -> str:
    """Keep navigation semantics but remove temporary/unknown parameter values."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return "INVALID_URL"
    safe_query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        safe_query.append((key, value if key in SAFE_NAV_QUERY_VALUES else "REDACTED"))
    return urlunparse(parsed._replace(query=urlencode(safe_query), fragment=""))


def sanitize_diagnostic(message: str, *, limit: int = 1000) -> str:
    """Remove URLs' unknown values, socket paths, and opaque connection handles."""
    def replace_url(match: re.Match[str]) -> str:
        return redact_url(match.group(0).rstrip(".,;)]}"))

    cleaned = re.sub(r"https?://[^\s]+", replace_url, message)
    cleaned = re.sub(r"(?:/[^\s]+)?\.sock(?:et)?\b", "[REDACTED_SOCKET]", cleaned)
    cleaned = re.sub(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}\b", "[REDACTED_HANDLE]", cleaned)
    return cleaned[:limit]


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(data)
    temporary.chmod(0o600)
    temporary.replace(path)


def json_bytes(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def walk(value: Any) -> Iterable[dict]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def node_text(node: dict) -> str:
    parts = []
    for key in ("name", "accessible_name", "text", "label", "value", "description"):
        value = node.get(key)
        if isinstance(value, str) and value.strip() and value.strip() not in parts:
            parts.append(value.strip())
    return " ".join(parts)


def node_labels(node: dict) -> list[str]:
    labels = []
    for key in ("name", "accessible_name", "text", "label", "value", "description"):
        value = node.get(key)
        if isinstance(value, str) and value.strip() and value.strip() not in labels:
            labels.append(value.strip())
    return labels


def node_ref(node: dict) -> str | None:
    for key in ("ref", "action_ref", "element_ref", "element_token"):
        value = node.get(key)
        if isinstance(value, str) and (value.startswith("p") or value.startswith("s")):
            return value
    return None


def node_role(node: dict) -> str:
    return str(node.get("role") or node.get("type") or "").lower()


def node_selected(node: dict) -> bool:
    if any(node.get(key) in (True, "true", "page") for key in ("selected", "checked", "current", "aria_selected")):
        return True
    attributes = node.get("attributes")
    if isinstance(attributes, dict) and any(attributes.get(key) in (True, "true", "page") for key in ("selected", "checked", "aria-selected", "aria-current")):
        return True
    class_name = str(node.get("class") or node.get("class_name") or (attributes or {}).get("class") or "")
    return bool(re.search(r"(^|[-_\s])(active|selected|checked)([-_\s]|$)", class_name, re.I))


def node_disabled(node: dict) -> bool:
    if any(node.get(key) in (True, "true") for key in ("disabled", "aria_disabled")):
        return True
    attributes = node.get("attributes")
    return isinstance(attributes, dict) and any(attributes.get(key) in (True, "true", "disabled") for key in ("disabled", "aria-disabled"))


def exact_nodes(snapshot: dict, label: str) -> list[dict]:
    wanted = re.sub(r"\s+", "", label)
    return [
        node for node in walk(snapshot)
        if any(re.sub(r"\s+", "", value) == wanted for value in node_labels(node))
    ]


def find_action(snapshot: dict, labels: Iterable[str]) -> dict | None:
    normalized = {re.sub(r"\s+", "", label) for label in labels}
    candidates = []
    for node in walk(snapshot):
        texts = {re.sub(r"\s+", "", value) for value in node_labels(node)}
        if texts.intersection(normalized) and node_ref(node):
            candidates.append(node)
    if not candidates:
        return None
    enabled = [node for node in candidates if not node_disabled(node)]
    return (enabled or candidates)[0]


def all_visible_text(snapshot: dict) -> str:
    seen = []
    for node in walk(snapshot):
        text = node_text(node)
        if text and text not in seen:
            seen.append(text)
    return "\n".join(seen)


def visible_text_sequence(snapshot: dict) -> str:
    """Preserve rendered order, including repeated labels in a footer.

    ``all_visible_text`` intentionally de-duplicates for diagnostics.  That is
    unsuitable for a footer such as “共 55 个 直播”, because “直播” also
    appears in the selected tab earlier on the page.
    """
    values = []
    for node in walk(snapshot):
        values.extend(node_labels(node))
    return "\n".join(values)


def extract_snapshot_url(snapshot: dict, *, host: str = BUYIN_HOST) -> str | None:
    direct_urls = []
    fallback_urls = []
    for node in walk(snapshot):
        if node.get("role") == "AXTextField" and node.get("label") == "地址和搜索栏":
            value = node.get("value")
            if isinstance(value, str):
                try:
                    if urlparse(value).hostname == host:
                        direct_urls.append(value)
                except ValueError:
                    pass
        for key in ("current_url", "url", "href"):
            value = node.get(key)
            if not isinstance(value, str):
                continue
            try:
                matches = urlparse(value).hostname == host
            except ValueError:
                matches = False
            if matches:
                if key == "current_url" or isinstance(node.get("tab_id"), str):
                    direct_urls.append(value)
                else:
                    fallback_urls.append(value)
    urls = direct_urls or fallback_urls
    return urls[0] if urls else None


def extract_product_name(snapshot: dict) -> str | None:
    for node in walk(snapshot):
        for key in ("product_name", "merchandise_name", "commodity_name"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    exclusions = {"巨量百应", "商品决策", "带货内容", "直播", "视频", "图文", "橱窗"}
    elements = snapshot.get("elements")
    if isinstance(elements, list):
        copy_offsets = [index for index, node in enumerate(elements) if "复制链接" in node_labels(node)]
        for offset in copy_offsets:
            for node in reversed(elements[max(0, offset - 12):offset]):
                value = str(node.get("value") or node.get("label") or "").strip()
                if (
                    node.get("role") == "AXStaticText"
                    and 2 <= len(value) <= 160
                    and value not in exclusions
                    and not re.fullmatch(r"[¥￥%0-9.,+\-]+", value)
                ):
                    return value
    for node in walk(snapshot):
        if node_role(node) in {"heading", "h1", "h2"}:
            value = node_text(node).strip()
            if 2 <= len(value) <= 160 and value not in exclusions:
                return value
    return None


def verify_selected_tab(snapshot: dict, label: str) -> bool:
    return any(node_selected(node) for node in exact_nodes(snapshot, label))


def extract_time_filter(snapshot: dict) -> tuple[str | None, str | None, bool]:
    if snapshot.get("filter_verified") is True:
        return snapshot.get("time_filter"), snapshot.get("filter_label"), True
    for node in walk(snapshot):
        for text in node_labels(node):
            match = re.fullmatch(r"近\s*(\d+)\s*天", text)
            if match and node_selected(node):
                return f"last_{match.group(1)}_days", re.sub(r"\s+", "", text), True
    for node in walk(snapshot):
        value = node.get("time_filter")
        label = node.get("filter_label")
        if isinstance(value, str) and isinstance(label, str):
            return value, label, bool(node.get("filter_verified"))
    return None, None, False


def last_30_days_row_evidence(rows: Iterable[dict]) -> bool:
    """Return true only when visible rows distinguish 30 days from 7 days."""
    today = datetime.now().astimezone().date()
    for row in rows:
        try:
            live_day = datetime.strptime(str(row.get("live_date") or ""), "%Y/%m/%d").date()
        except ValueError:
            continue
        delta = (today - live_day).days
        if 7 < delta <= 30:
            return True
    return False


def native_last_30_days_evidence(snapshot: dict) -> bool:
    """Prove the 30-day filter when native AX omits its selected state.

    Buyin exposes only 近7天 / 近30天 on this live surface.  After explicitly
    selecting 近30天, a visible live row dated 8--30 days ago proves that the
    result set is not the 7-day option.  If no such row is visible, we do not
    guess: the scan remains unable to verify the filter.
    """
    return last_30_days_row_evidence(extract_native_rows(snapshot, page_number=1))


def extract_reported_total(snapshot: dict) -> int | None:
    for node in walk(snapshot):
        value = node.get("reported_total")
        if isinstance(value, int) and value >= 0:
            return value
    text = visible_text_sequence(snapshot) + "\n" + str(snapshot.get("tree_markdown") or "")
    for pattern in (
        r"共\s*([0-9,]+)\s*条",
        r"([0-9,]+)\s*条(?:直播|结果|带货内容)",
        r"共\s*([0-9,]+)\s*个\s*直播",
    ):
        match = re.search(pattern, text)
        if match:
            return int(match.group(1).replace(",", ""))
    return None


HEADER_MAP = {
    "直播标题": "live_title", "标题": "live_title", "直播日期": "live_date", "日期": "live_date",
    "达人昵称": "account_name", "达人": "account_name", "账号名称": "account_name", "同行名称": "account_name",
    "粉丝数": "follower_count", "粉丝": "follower_count", "所在地": "location", "地区": "location",
    "销量": "sales", "销售量": "sales", "结算金额": "settlement_amount", "成交金额": "settlement_amount",
    "总观看人数": "total_views", "观看人数": "total_views", "最高在线": "peak_popularity", "峰值人气": "peak_popularity",
    "点击率": "click_rate", "下单转化率": "order_conversion_rate", "订单转化率": "order_conversion_rate",
}


def descendants(node: dict) -> list[dict]:
    result = []
    for value in node.values():
        if isinstance(value, (dict, list)):
            result.extend(walk(value))
    return result


def extract_rows(snapshot: dict, page_number: int, batch_number: int) -> list[dict]:
    """Extract semantic table rows; fixture snapshots may provide normalized observations."""
    for node in walk(snapshot):
        normalized = node.get("observations")
        if isinstance(normalized, list):
            rows = []
            for position, source in enumerate(normalized, start=1):
                if not isinstance(source, dict):
                    continue
                row = dict(source)
                row.setdefault("source_page", page_number)
                row.setdefault("source_batch", batch_number)
                row.setdefault("source_position", position)
                row.setdefault("collected_at", utc_now())
                rows.append(row)
            return rows

    semantic_rows = [node for node in walk(snapshot) if node_role(node) in {"row", "tr"}]
    headers: list[str] | None = None
    output = []
    for row_node in semantic_rows:
        cells = [child for child in descendants(row_node) if node_role(child) in {"cell", "gridcell", "columnheader", "th", "td"}]
        texts = [node_text(cell).strip() for cell in cells if node_text(cell).strip()]
        if not texts:
            continue
        mapped = [HEADER_MAP.get(re.sub(r"\s+", "", text)) for text in texts]
        if sum(field is not None for field in mapped) >= 2:
            headers = [field or f"unmapped_{index}" for index, field in enumerate(mapped)]
            continue
        if not headers or len(texts) < 2:
            continue
        record = {headers[index]: value for index, value in enumerate(texts[:len(headers)]) if not headers[index].startswith("unmapped_")}
        if not record.get("account_name") or not record.get("live_title"):
            continue
        record.update({
            "source_page": page_number,
            "source_batch": batch_number,
            "source_position": len(output) + 1,
            "collected_at": utc_now(),
        })
        account_name = str(record["account_name"])
        for child in descendants(row_node):
            href = child.get("href") or child.get("url")
            if isinstance(href, str) and "uid=" in href and account_name in node_text(child):
                record["_profile_url"] = href
                break
        output.append(record)
    return output


def extract_native_rows(snapshot: dict, page_number: int) -> list[dict]:
    """Read Tabbit's flat native AX table without relying on a DOM tree."""
    elements = snapshot.get("elements")
    if not isinstance(elements, list):
        return extract_rows(snapshot, page_number, page_number)
    by_index = {node.get("element_index"): node for node in elements if isinstance(node.get("element_index"), int)}
    children: dict[int, list[int]] = defaultdict(list)
    for node in elements:
        parent = node.get("parent_index")
        index = node.get("element_index")
        if isinstance(parent, int) and isinstance(index, int):
            children[parent].append(index)

    def text_values(index: int) -> list[str]:
        values: list[str] = []
        for child_index in children.get(index, []):
            child = by_index[child_index]
            value = child.get("value") or child.get("label")
            if child.get("role") == "AXStaticText" and isinstance(value, str) and value and value not in values:
                values.append(value)
            for value in text_values(child_index):
                if value not in values:
                    values.append(value)
        return values

    table = next((node for node in elements if node.get("role") == "AXTable"), None)
    if not table or not isinstance(table.get("element_index"), int):
        return []
    table_index = table["element_index"]
    rows = [node for node in elements if node.get("role") == "AXRow" and node.get("parent_index") == table_index]
    output: list[dict] = []
    for row in rows[1:]:
        row_index = row.get("element_index")
        if not isinstance(row_index, int):
            continue
        cells = [by_index[index] for index in children.get(row_index, []) if by_index[index].get("role") == "AXCell"]
        if len(cells) < 8:
            continue
        live_values, account_values = text_values(cells[0]["element_index"]), text_values(cells[1]["element_index"])
        title = next((value for value in live_values if value != "直播时间"), "")
        live_date = next((value for value in live_values if re.fullmatch(r"\d{4}/\d{2}/\d{2}", value)), "")
        account_name = next((value for value in account_values if value != "粉丝数" and "·" not in value), "")
        follower = ""
        if "粉丝数" in account_values:
            offset = account_values.index("粉丝数") + 1
            if offset < len(account_values):
                follower = account_values[offset]
        location = next((value for value in account_values if "·" in value), "")
        metrics = [(text_values(cell["element_index"]) + [""])[0] for cell in cells[2:8]]
        if not title or not live_date or not account_name:
            continue
        output.append({
            "live_title": title, "live_date": live_date, "account_name": account_name,
            "follower_count": follower, "location": location,
            "sales": metrics[0], "settlement_amount": metrics[1], "total_views": metrics[2],
            "peak_popularity": metrics[3], "click_rate": metrics[4], "order_conversion_rate": metrics[5],
            "source_page": page_number, "source_batch": page_number,
            "source_position": len(output) + 1, "collected_at": utc_now(),
        })
    return output


def extract_uid(profile_url: str) -> str | None:
    values = [value for key, value in parse_qsl(urlparse(profile_url).query, keep_blank_values=True) if key == "uid"]
    return values[0] if len(values) == 1 and values[0] else None


def identity_from_observation(row: dict) -> dict | None:
    profile_url = str(row.get("_profile_url") or row.get("profile_url") or "")
    uid = extract_uid(profile_url) if profile_url else None
    if not uid or row.get("_detail_verified") is not True:
        return None
    evidence = {
        "account_name": row.get("account_name"),
        "profile_url": profile_url,
        "buyin_creator_uid": uid,
        "detail_page_verified": True,
    }
    if not verified_identity(evidence):
        return None
    return {
        "account_name": row.get("account_name"),
        "buyin_creator_uid": uid,
        "profile_url_redacted": redact_url(profile_url),
        "verification_method": "live_result_creator_link_uid",
        "verified": True,
    }


def select_tabbit_content_window(windows: dict, *, expected_product_id: str | None = None) -> dict:
    """Select the intended Buyin content window using positive evidence.

    Window area/title alone is unsafe: a full-size Feishu or permission window
    can otherwise win the heuristic. Prefer a verified product URL or a
    product-decision title, and reject known unrelated full-size windows.
    """
    candidates = [
        node for node in walk(windows)
        if str(node.get("app_name") or "").lower() == "tabbit"
        and isinstance(node.get("pid"), int)
        and isinstance(node.get("window_id"), int)
    ]
    if not candidates:
        raise ScanFailure("TABBIT_NOT_FOUND", "未找到当前Tabbit窗口")

    def area(node: dict) -> float:
        bounds = node.get("bounds") or {}
        try:
            return float(bounds.get("width") or 0) * float(bounds.get("height") or 0)
        except (TypeError, ValueError):
            return 0.0

    max_area = max(area(node) for node in candidates)
    content = [node for node in candidates if max_area <= 0 or area(node) >= max_area * 0.8]
    def evidence(node: dict) -> int:
        text = " ".join(str(node.get(key) or "") for key in ("title", "url", "document_url", "name")).lower()
        score = 0
        if "buyin.jinritemai.com" in text or "jinritemai" in text:
            score += 100
        if "商品决策页" in text or ("商品" in text and "决策" in text):
            score += 50
        if expected_product_id and expected_product_id in text:
            score += 100
        if any(token in text for token in ("飞书", "消息", "permission", "权限", "设置")):
            score -= 100
        return score

    scored = sorted(((evidence(node), node) for node in content), key=lambda item: item[0], reverse=True)
    if scored and scored[0][0] > 0 and (len(scored) == 1 or scored[0][0] > scored[1][0]):
        return scored[0][1]
    unrelated = [node for node in content if evidence(node) < 0]
    neutral = [node for node in content if evidence(node) == 0]
    titled_neutral = [node for node in neutral if str(node.get("title") or "").strip()]
    if len(titled_neutral) == 1:
        return titled_neutral[0]
    if len(neutral) == 1 and unrelated:
        return neutral[0]
    if len(content) == 1:
        return content[0]
    raise ScanFailure("TABBIT_WINDOW_AMBIGUOUS", "无法唯一确定当前Tabbit内容窗口")


class CuaClient:
    def __init__(self, binary: Path = CUA_DRIVER):
        self.binary = binary

    def call(self, tool: str, arguments: dict, *, screenshot_out: Path | None = None) -> dict:
        command = [str(self.binary), "call", tool, json.dumps(arguments, ensure_ascii=False)]
        if screenshot_out:
            command.extend(["--screenshot-out-file", str(screenshot_out)])
        if not CUA_SOCKET.exists():
            raise ScanFailure("BROWSER_CONTROL_FAILED", "竞品情报Profile专用cua-driver未就绪，禁止回退到全局实例")
        command.extend(["--socket", str(CUA_SOCKET)])
        completed = subprocess.run(command, text=True, capture_output=True, timeout=45, check=False)
        if completed.returncode != 0:
            message = (completed.stderr or completed.stdout or "cua-driver call failed").strip()
            raise ScanFailure("BROWSER_CONTROL_FAILED", sanitize_diagnostic(message))
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise ScanFailure("BROWSER_CONTROL_FAILED", "cua-driver返回了非JSON结果") from exc
        if isinstance(payload, dict) and payload.get("isError") is True:
            raise ScanFailure("BROWSER_CONTROL_FAILED", sanitize_diagnostic(str(payload.get("content") or "cua-driver error")))
        return payload


class NativeTabbitScanner:
    """Native AX/CUA backend for Tabbit, which does not expose a CDP target."""

    def __init__(self, client: CuaClient, output_dir: Path):
        self.client = client
        self.output_dir = output_dir
        self.session = f"mvp-a-native-{compact_time_id()}"
        self.pid: int | None = None
        self.window_id: int | None = None
        self.completed_steps: list[str] = []
        self._screenshot_size: tuple[int, int] | None = None

    def _window(self) -> dict:
        if self.pid is None or self.window_id is None:
            raise ScanFailure("TABBIT_NOT_FOUND", "Tabbit窗口未绑定")
        return {"pid": self.pid, "window_id": self.window_id, "session": self.session}

    def start(self) -> tuple[str, str]:
        self.client.call("start_session", {"session": self.session})
        windows = self.client.call("list_windows", {"on_screen_only": True})
        selected = select_tabbit_content_window(windows)
        self.pid, self.window_id = selected["pid"], selected["window_id"]
        snapshot = self.snapshot()
        current = extract_snapshot_url(snapshot)
        if not current or product_id_from_decision_url(current) is None:
            tab = find_action(snapshot, ("商品决策页",))
            if tab:
                snapshot = self.click_action(tab)
                current = extract_snapshot_url(snapshot)
            else:
                # Tabbit's native AX tree sometimes omits the tab strip. Cycle only
                # existing tabs and verify the address bar after every movement.
                for _ in range(8):
                    self.client.call("hotkey", {**self._window(), "keys": ["ctrl", "shift", "tab"], "delivery_mode": "foreground"})
                    time.sleep(0.3)
                    snapshot = self.snapshot()
                    current = extract_snapshot_url(snapshot)
                    if current and product_id_from_decision_url(current) is not None:
                        break
        if not current or product_id_from_decision_url(current) is None:
            host = (urlparse(current or "").hostname or "").lower()
            if host == "douyinec.com" or host.endswith(".douyinec.com") or "login" in (current or "").lower():
                raise ScanFailure("BUYIN_LOGIN_REQUIRED", "当前Tabbit没有已登录的百应商品决策页，请登录百应并打开商品决策页后恢复原任务")
            raise ScanFailure("TABBIT_TAB_AMBIGUOUS", "无法验证商品决策页URL")
        self.completed_steps.append("tabbit_native_window_bound")
        return current, redact_url(current)

    def snapshot(self, *, screenshot: Path | None = None) -> dict:
        # A 12-row Buyin table can exceed 1,200 native AX nodes; the result
        # total and pagination are rendered after the table and were therefore
        # silently truncated.  Keep enough headroom to include the footer.
        args = {**self._window(), "include_screenshot": bool(screenshot), "max_elements": 5000, "max_depth": 24}
        payload = self.client.call("get_window_state", args, screenshot_out=screenshot)
        if screenshot and screenshot.is_file():
            with screenshot.open("rb") as handle:
                header = handle.read(24)
            if header.startswith(b"\x89PNG"):
                self._screenshot_size = struct.unpack(">II", header[16:24])
        return payload

    def navigate_raw(self, url: str) -> dict:
        snapshot = self.snapshot()
        address = next((node for node in walk(snapshot) if node.get("role") == "AXTextField" and node.get("label") == "地址和搜索栏"), None)
        if not address or not node_ref(address):
            raise ScanFailure("PAGE_STRUCTURE_CHANGED", "未找到Tabbit地址栏")
        self.client.call("set_value", {**self._window(), "element_token": node_ref(address), "value": url})
        self.client.call("press_key", {**self._window(), "element_token": node_ref(address), "key": "return", "delivery_mode": "foreground"})
        expected = urlparse(url)
        for _ in range(20):
            time.sleep(0.5)
            snapshot = self.snapshot()
            current = extract_snapshot_url(snapshot)
            if current:
                parsed = urlparse(current)
                if parsed.hostname == expected.hostname and parsed.path == expected.path:
                    return snapshot
        raise ScanFailure("PAGE_LOAD_TIMEOUT", "Tabbit页面未在10秒内完成加载")

    def navigate(self, url: str) -> dict:
        snapshot = self.navigate_raw(url)
        if product_id_from_decision_url(extract_snapshot_url(snapshot) or "") is None:
            raise ScanFailure("PAGE_LOAD_TIMEOUT", "目标商品页未完成URL验证")
        for _ in range(20):
            if extract_product_name(snapshot):
                return snapshot
            time.sleep(0.5)
            snapshot = self.snapshot()
        return snapshot

    def click_action(self, action: dict) -> dict:
        ref = node_ref(action)
        if not ref:
            raise ScanFailure("PAGE_STRUCTURE_CHANGED", "页面元素没有可操作引用")
        self.client.call("click", {**self._window(), "element_token": ref, "delivery_mode": "foreground"})
        time.sleep(0.7)
        return self.snapshot()

    def click_label(self, snapshot: dict, label: str) -> dict:
        action = find_action(snapshot, (label,))
        if not action:
            self.capture_failure("page-structure")
            raise ScanFailure("PAGE_STRUCTURE_CHANGED", f"找不到“{label}”")
        return self.click_action(action)

    def scroll_bottom(self) -> dict:
        evidence = self.output_dir / ".pagination.png"
        snapshot = self.snapshot(screenshot=evidence)
        width, height = self._screenshot_size or (1568, 776)
        self.client.call("scroll", {**self._window(), "x": int(width * .72), "y": int(height * .75), "direction": "down", "by": "page", "amount": 10})
        time.sleep(0.7)
        return self.snapshot(screenshot=evidence)

    def click_next_page(self, control: dict) -> dict:
        """Advance through the actionable native next-page control only."""
        ref = node_ref(control)
        if control.get("role") != "AXButton" or not ref or node_disabled(control):
            raise ScanFailure("INCOMPLETE", "未找到可验证的下一页控件", incomplete=True)
        self.client.call("click", {**self._window(), "element_token": ref, "delivery_mode": "foreground"})
        time.sleep(0.8)
        return self.snapshot()

    def select_time_filter(self, snapshot: dict, label: str = "近30天") -> dict:
        """Select one explicit time filter and require state or result evidence."""
        if verify_selected_tab(snapshot, label):
            return snapshot
        action = find_action(snapshot, (label,))
        if not action:
            self.capture_failure("time-filter-missing")
            raise ScanFailure("PAGE_STRUCTURE_CHANGED", f"找不到直播时间筛选“{label}”")
        snapshot = self.click_action(action)
        for _ in range(12):
            if verify_selected_tab(snapshot, label):
                return snapshot
            if label == "近30天" and native_last_30_days_evidence(snapshot):
                verified = dict(snapshot)
                verified.update({
                    "time_filter": "last_30_days",
                    "filter_label": "近30天",
                    "filter_verified": True,
                    "filter_verification_method": "visible_live_row_older_than_7_days_after_select_30_days",
                })
                return verified
            time.sleep(0.4)
            snapshot = self.snapshot()
        if label == "近30天":
            # AX can omit active styling when the first visible page happens
            # to contain only the latest seven days. Keep the clicked filter
            # pending and require full-result scope evidence before COMPLETE.
            pending = dict(snapshot)
            pending.update({
                "time_filter": "last_30_days",
                "filter_label": "近30天",
                "filter_verified": False,
                "filter_verification_method": "pending_full_result_scope_evidence",
            })
            return pending
        self.capture_failure("time-filter-unverified")
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", f"无法验证直播时间筛选“{label}”已选中")

    def capture_failure(self, stem: str) -> None:
        screenshot = self.output_dir / f"{stem}.png"
        try:
            snapshot = self.snapshot(screenshot=screenshot)
            summary = {
                "captured_at": utc_now(),
                "current_url_redacted": redact_url(extract_snapshot_url(snapshot) or ""),
                "visible_text_summary": sanitize_diagnostic(all_visible_text(snapshot), limit=12000),
                "completed_steps": self.completed_steps,
            }
            atomic_write(self.output_dir / f"{stem}.json", json_bytes(summary))
        except Exception:
            pass

    def end(self) -> None:
        try:
            self.client.call("end_session", {"session": self.session})
        except Exception:
            pass


class LiveScanner:
    def __init__(self, client: CuaClient, output_dir: Path):
        self.client = client
        self.output_dir = output_dir
        self.session = f"mvp-a-{compact_time_id()}"
        self.pid: int | None = None
        self.window_id: int | None = None
        self.target_id: str | None = None
        self.tab_id: str | None = None
        self.completed_steps: list[str] = []

    def start(self) -> tuple[str, str]:
        self.client.call("start_session", {"session": self.session})
        windows = self.client.call("list_windows", {"on_screen_only": False})
        candidates = []
        for node in walk(windows):
            app = str(node.get("app_name") or "")
            title = str(node.get("title") or "")
            if "tabbit" in f"{app} {title}".lower() and isinstance(node.get("pid"), int) and isinstance(node.get("window_id"), int):
                candidates.append(node)
        if not candidates:
            raise ScanFailure("TABBIT_NOT_FOUND", "未找到当前Tabbit窗口")
        on_screen = [item for item in candidates if item.get("is_on_screen") is True]
        selected = (on_screen or candidates)
        if len(selected) != 1:
            raise ScanFailure("TABBIT_WINDOW_AMBIGUOUS", "找到多个Tabbit窗口，无法安全选择")
        self.pid = selected[0]["pid"]
        self.window_id = selected[0]["window_id"]
        bound = self.client.call("get_browser_state", {
            "pid": self.pid, "window_id": self.window_id, "session": self.session,
            "snapshot_format": "semantic_v2",
        })
        target_ids = sorted({
            node["target_id"] for node in walk(bound)
            if isinstance(node.get("target_id"), str)
        })
        tabs = []
        for node in walk(bound):
            tab_id = node.get("tab_id")
            url = node.get("url") or node.get("current_url")
            target_id = node.get("target_id") or (target_ids[0] if len(target_ids) == 1 else None)
            if isinstance(tab_id, str) and isinstance(url, str) and isinstance(target_id, str):
                tabs.append((target_id, tab_id, url))
        decision_tabs = [item for item in tabs if product_id_from_decision_url(item[2])]
        if len(decision_tabs) != 1:
            raise ScanFailure("TABBIT_TAB_AMBIGUOUS", "必须且只能有一个可绑定的巨量百应商品决策标签页")
        self.target_id, self.tab_id, current_url = decision_tabs[0]
        self.completed_steps.append("tabbit_decision_tab_bound")
        return current_url, redact_url(current_url)

    def snapshot(self, *, query: str | None = None, screenshot: Path | None = None) -> dict:
        assert self.target_id and self.tab_id
        arguments = {
            "target_id": self.target_id,
            "tab_id": self.tab_id,
            "session": self.session,
            "snapshot_format": "semantic_v2",
            "include_screenshot": bool(screenshot),
        }
        if query:
            arguments["query"] = query
        return self.client.call("get_browser_state", arguments, screenshot_out=screenshot)

    def navigate(self, url: str) -> dict:
        snapshot = self.navigate_raw(url)
        page_url = extract_snapshot_url(snapshot)
        if page_url and product_id_from_decision_url(page_url):
            return snapshot
        raise ScanFailure("PAGE_LOAD_TIMEOUT", "目标商品页未在10秒内完成可验证加载")

    def navigate_raw(self, url: str) -> dict:
        assert self.target_id and self.tab_id
        self.client.call("browser_navigate", {
            "target_id": self.target_id, "tab_id": self.tab_id, "session": self.session, "url": url,
        })
        last = {}
        expected = urlparse(url)
        for _ in range(20):
            time.sleep(0.5)
            last = self.snapshot()
            page_url = extract_snapshot_url(last)
            if page_url:
                actual = urlparse(page_url)
                if actual.hostname == expected.hostname and actual.path == expected.path:
                    return last
        raise ScanFailure("PAGE_LOAD_TIMEOUT", "页面未在10秒内完成可验证加载")

    def click_label(self, snapshot: dict, label: str) -> dict:
        action = find_action(snapshot, (label,))
        if not action:
            self.capture_failure("page-structure")
            raise ScanFailure("PAGE_STRUCTURE_CHANGED", f"找不到可语义点击的“{label}”标签")
        assert self.target_id and self.tab_id
        self.client.call("browser_click", {
            "target_id": self.target_id, "tab_id": self.tab_id, "session": self.session, "ref": node_ref(action),
        })
        time.sleep(0.7)
        return self.snapshot()

    def click_action(self, action: dict) -> dict:
        assert self.target_id and self.tab_id
        self.client.call("browser_click", {
            "target_id": self.target_id, "tab_id": self.tab_id, "session": self.session, "ref": node_ref(action),
        })
        time.sleep(0.7)
        return self.snapshot()

    def scroll(self) -> dict:
        assert self.target_id and self.tab_id
        self.client.call("browser_pointer", {
            "target_id": self.target_id, "tab_id": self.tab_id, "session": self.session,
            "action": "scroll", "delta_y": 900,
        })
        time.sleep(0.7)
        return self.snapshot()

    def capture_failure(self, stem: str) -> None:
        screenshot = self.output_dir / f"{stem}.png"
        try:
            snapshot = self.snapshot(screenshot=screenshot)
            summary = {
                "captured_at": utc_now(),
                "current_url_redacted": redact_url(extract_snapshot_url(snapshot) or ""),
                "visible_text_summary": sanitize_diagnostic(all_visible_text(snapshot), limit=12000),
                "completed_steps": list(self.completed_steps),
            }
            atomic_write(self.output_dir / f"{stem}.json", json_bytes(summary))
        except Exception:
            pass

    def end(self) -> None:
        try:
            self.client.call("end_session", {"session": self.session})
        except Exception:
            pass


def verify_live_surface(scanner: LiveScanner | NativeTabbitScanner, snapshot: dict) -> tuple[dict, str, str]:
    ensure_no_human_block(snapshot)
    if not verify_selected_tab(snapshot, "带货内容"):
        snapshot = scanner.click_label(snapshot, "带货内容")
    if not verify_selected_tab(snapshot, "带货内容"):
        scanner.capture_failure("carrying-content-tab-not-selected")
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "“带货内容”标签点击后未验证为激活")
    if not verify_selected_tab(snapshot, "直播"):
        snapshot = scanner.click_label(snapshot, "直播")
    if not (verify_selected_tab(snapshot, "带货内容") and verify_selected_tab(snapshot, "直播")):
        scanner.capture_failure("live-tabs-not-selected")
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "无法同时验证“带货内容”和“直播”为激活状态")
    text = all_visible_text(snapshot)
    if not any(token in text for token in ("直播标题", "直播日期", "直播时间", "直播带货", "共")):
        scanner.capture_failure("live-results-not-loaded")
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "直播结果区域未完成可验证加载")
    if isinstance(scanner, NativeTabbitScanner):
        # MVP-C fixes the scope to one explicit, visible filter.  Do not infer a
        # default value from layout or from a previous successful scan.
        snapshot = scanner.select_time_filter(snapshot, "近30天")
    time_filter, filter_label, verified = extract_time_filter(snapshot)
    pending_native_scope = (
        isinstance(scanner, NativeTabbitScanner)
        and snapshot.get("filter_verification_method") == "pending_full_result_scope_evidence"
        and time_filter == "last_30_days"
        and filter_label == "近30天"
    )
    if (not verified and not pending_native_scope) or not time_filter or not filter_label:
        scanner.capture_failure("time-filter-unverified")
        raise ScanFailure("PAGE_STRUCTURE_CHANGED", "无法验证当前直播时间筛选")
    steps = ["tab_带货内容_verified", "tab_直播_verified", "live_results_loaded"]
    if verified:
        steps.append("time_filter_verified")
    for step in steps:
        if step not in scanner.completed_steps:
            scanner.completed_steps.append(step)
    return snapshot, time_filter, filter_label


def ensure_no_human_block(snapshot: dict) -> None:
    text = all_visible_text(snapshot)
    signals = (
        "请登录", "扫码登录", "登录已失效", "请输入验证码", "安全验证",
        "访问过于频繁", "操作过于频繁", "权限不足", "暂无权限", "无权限访问", "访问受限",
    )
    found = [signal for signal in signals if signal in text]
    if found:
        raise ScanFailure("HUMAN_VERIFICATION_REQUIRED", f"检测到登录/验证码/风控信号：{found[0]}")


def end_signal_from_snapshot(snapshot: dict, row_count: int, reported_total: int | None) -> dict | None:
    text = all_visible_text(snapshot)
    if re.search(r"没有更多|暂无更多|已加载全部|到底了", text):
        return {"type": "no_more_results_message", "verified": True, "evidence": "platform_visible_message"}
    next_node = find_action(snapshot, ("下一页", "下页"))
    if next_node and node_disabled(next_node):
        return {"type": "next_disabled", "verified": True, "evidence": "semantic_disabled_state"}
    load_more = find_action(snapshot, ("加载更多", "查看更多"))
    if load_more and node_disabled(load_more):
        return {"type": "load_more_disabled", "verified": True, "evidence": "semantic_disabled_state"}
    for node in walk(snapshot):
        current = node.get("current_page")
        total_pages = node.get("total_pages")
        if isinstance(current, int) and isinstance(total_pages, int) and current == total_pages:
            return {"type": "platform_page_count_exhausted", "verified": True, "evidence": f"page {current}/{total_pages}"}
    return None


def native_pagination_state(snapshot: dict) -> tuple[str, dict | None]:
    """Read Tabbit's native footer without treating a coordinate click as proof.

    In the native accessibility tree an enabled next page is an ``AXButton``
    with a ``right`` glyph.  On the last page Buyin renders only that glyph and
    omits an actionable button.  Require a numbered footer as context so table
    sort chevrons are never mistaken for pagination.
    """
    elements = snapshot.get("elements")
    if not isinstance(elements, list):
        return "unknown", None
    by_index = {node.get("element_index"): node for node in elements if isinstance(node.get("element_index"), int)}
    children: dict[int, list[dict]] = defaultdict(list)
    for node in elements:
        parent = node.get("parent_index")
        if isinstance(parent, int):
            children[parent].append(node)
    for parent_index, descendants_ in children.items():
        parent = by_index.get(parent_index) or {}
        if parent.get("role") != "AXList":
            continue
        numeric = [node for node in descendants_ if re.fullmatch(r"\d+", str(node.get("label") or node.get("value") or ""))]
        if not numeric:
            continue
        right_button = next((node for node in descendants_ if node.get("role") == "AXButton" and str(node.get("label") or "").lower() == "right"), None)
        right_icon = next((node for node in descendants_ if node.get("role") == "AXImage" and str(node.get("label") or "").lower() == "right"), None)
        if right_button:
            return ("disabled" if node_disabled(right_button) else "enabled"), right_button
        if right_icon:
            return "disabled", right_icon
    return "unknown", None


def scan_result_pages(scanner: LiveScanner, first_snapshot: dict) -> tuple[list[dict], list[dict], dict, int | None, list[str], dict]:
    observations: list[dict] = []
    pages: list[dict] = []
    seen: set[str] = set()
    snapshot = first_snapshot
    page_number = 1
    batch_number = 1
    zero_progress_scrolls = 0
    context_actions: list[str] = []
    reported_total = extract_reported_total(snapshot)
    while page_number <= 1000 and batch_number <= 1000:
        ensure_no_human_block(snapshot)
        rows = extract_rows(snapshot, page_number, batch_number)
        new_rows = []
        for row in rows:
            key = observation_key(row)
            if key not in seen:
                seen.add(key)
                new_rows.append(row)
                observations.append(row)
        pages.append({"page": len(pages) + 1, "rows": [dict(row) for row in new_rows]})
        signal = end_signal_from_snapshot(snapshot, len(observations), reported_total)
        if signal:
            if not observations and reported_total is None:
                raise ScanFailure(
                    "INCOMPLETE", "页面未显示结果总数且未读取到记录，不能证明是零结果", incomplete=True,
                )
            return observations, pages, signal, reported_total, context_actions, snapshot

        next_node = find_action(snapshot, ("下一页", "下页"))
        if next_node and not node_disabled(next_node):
            snapshot = scanner.click_action(next_node)
            context_actions.append("next")
            page_number += 1
            batch_number += 1
            zero_progress_scrolls = 0
            continue
        load_more = find_action(snapshot, ("加载更多", "查看更多"))
        if load_more and not node_disabled(load_more):
            snapshot = scanner.click_action(load_more)
            context_actions.append("load_more")
            batch_number += 1
            zero_progress_scrolls = 0
            continue
        snapshot = scanner.scroll()
        context_actions.append("scroll")
        batch_number += 1
        if new_rows:
            zero_progress_scrolls = 0
        else:
            zero_progress_scrolls += 1
        if zero_progress_scrolls >= 3:
            raise ScanFailure(
                "INCOMPLETE",
                "连续滚动无新增结果，但平台未提供可信的无更多结果信号",
                incomplete=True,
                evidence={"last_page": page_number, "last_batch": batch_number, "read_count": len(observations)},
            )
    raise ScanFailure("INCOMPLETE", "遍历超过安全上限，未取得结束证据", incomplete=True)


def scan_native_result_pages(
    scanner: NativeTabbitScanner,
    first_snapshot: dict,
    observations: list[dict] | None = None,
    pages: list[dict] | None = None,
) -> tuple[list[dict], list[dict], dict, int | None]:
    """Traverse native pagination with an explicit total and terminal control proof.

    A page that happens not to advance after a coordinate click is not a
    trustworthy end signal.  COMPLETE requires all three independent facts:
    the visible total at the start and end agrees, every total row was read,
    and the platform exposes a disabled next/no-more state.
    """
    observations = observations if observations is not None else []
    pages = pages if pages is not None else []
    snapshot = first_snapshot
    reported_total: int | None = None
    for page_number in range(1, 1001):
        ensure_no_human_block(snapshot)
        rows = extract_native_rows(snapshot, page_number)
        if not rows:
            footer_snapshot = scanner.scroll_bottom()
            ensure_no_human_block(footer_snapshot)
            reported_total = extract_reported_total(footer_snapshot)
            text = all_visible_text(footer_snapshot) + "\n" + str(footer_snapshot.get("tree_markdown") or "")
            if reported_total == 0 and "暂无数据" in text and "共" in text and "个" in text:
                return [], [], {
                    "type": "no_more_results_message", "verified": True,
                    "evidence": "platform_visible_no_data_and_zero_result_footer",
                }, 0
            raise ScanFailure(
                "INCOMPLETE", "当前分页未读取到可验证直播记录",
                incomplete=True,
                evidence={"page": page_number, "reported_total": reported_total, "read_count": len(observations)},
            )
        # The platform total counts rendered result rows. Two live sessions may
        # expose identical visible fields, so row-content de-duplication would
        # undercount legitimate observations. Page advancement is verified by
        # the full-page fingerprint below; retain every platform row here.
        observations.extend(rows)
        pages.append({"page": page_number, "rows": [dict(row) for row in rows]})
        before = sorted(observation_key(row) for row in rows)
        # Native AX exposes the pagination control only when it is in view.
        footer_snapshot = scanner.scroll_bottom()
        ensure_no_human_block(footer_snapshot)
        current_total = extract_reported_total(footer_snapshot)
        if current_total is None:
            raise ScanFailure(
                "INCOMPLETE", "滚动到分页页脚后仍无法读取页面结果总数",
                incomplete=True,
                evidence={"page": page_number, "read_count": len(observations)},
            )
        if reported_total is None:
            reported_total = current_total
        elif current_total != reported_total:
            raise ScanFailure(
                "INCOMPLETE", "扫描期间页面结果总数发生变化，拒绝将动态结果标记为完整",
                incomplete=True,
                evidence={"initial_reported_total": reported_total, "current_reported_total": current_total, "page": page_number},
            )
        if len(observations) > reported_total:
            raise ScanFailure(
                "INCOMPLETE", "实际读取条数超过页面总数",
                incomplete=True,
                evidence={"reported_total": reported_total, "read_count": len(observations), "page": page_number},
            )
        terminal = end_signal_from_snapshot(footer_snapshot, len(observations), reported_total)
        pagination, next_control = native_pagination_state(footer_snapshot)
        if terminal or pagination == "disabled":
            if len(observations) != reported_total:
                raise ScanFailure(
                    "INCOMPLETE", "平台已到末页但实际读取条数与页面总数不一致",
                    incomplete=True,
                    evidence={"reported_total": reported_total, "read_count": len(observations), "page": page_number},
                )
            if terminal:
                return observations, pages, terminal, reported_total
            return observations, pages, {
                "type": "next_disabled", "verified": True,
                "evidence": "native_pagination_right_icon_without_actionable_button",
            }, reported_total
        if pagination != "enabled" or not next_control:
            raise ScanFailure(
                "INCOMPLETE", "分页页脚未提供可验证的下一页或无更多结果控件",
                incomplete=True,
                evidence={"page": page_number, "read_count": len(observations)},
            )
        snapshot = scanner.click_next_page(next_control)
        after_rows = extract_native_rows(snapshot, page_number + 1)
        after_fingerprint = sorted(observation_key(row) for row in after_rows)
        if before == after_fingerprint:
            raise ScanFailure(
                "INCOMPLETE", "下一页控件显示可用但点击后未推进，不能把它当作末页证据",
                incomplete=True,
                evidence={"page": page_number, "read_count": len(observations)},
            )
    raise ScanFailure("INCOMPLETE", "分页超过安全上限", incomplete=True)


def snapshot_fingerprint(snapshot: dict, page_number: int, batch_number: int) -> list[str]:
    return sorted(observation_key(row) for row in extract_rows(snapshot, page_number, batch_number))


def restore_list_context(
    scanner: LiveScanner,
    *,
    list_url: str,
    product_id: str,
    filter_label: str,
    actions: list[str],
    expected_fingerprint: list[str],
) -> dict:
    snapshot = scanner.navigate(list_url)
    actual_url = extract_snapshot_url(snapshot) or ""
    if product_id_from_decision_url(actual_url) != product_id:
        raise ScanFailure("PAGE_CONTEXT_LOST", "返回列表后商品ID发生变化")
    snapshot, _, restored_filter = verify_live_surface(scanner, snapshot)
    if restored_filter != filter_label:
        raise ScanFailure("PAGE_CONTEXT_LOST", "返回列表后时间筛选发生变化")
    for action_name in actions:
        if action_name == "next":
            action = find_action(snapshot, ("下一页", "下页"))
            if not action or node_disabled(action):
                raise ScanFailure("PAGE_CONTEXT_LOST", "无法恢复原分页位置")
            snapshot = scanner.click_action(action)
        elif action_name == "load_more":
            action = find_action(snapshot, ("加载更多", "查看更多"))
            if not action or node_disabled(action):
                raise ScanFailure("PAGE_CONTEXT_LOST", "无法恢复原加载批次")
            snapshot = scanner.click_action(action)
        elif action_name == "scroll":
            snapshot = scanner.scroll()
        else:
            raise ScanFailure("PAGE_CONTEXT_LOST", f"未知上下文动作：{action_name}")
        ensure_no_human_block(snapshot)
    actual_fingerprint = snapshot_fingerprint(snapshot, len(actions) + 1, len(actions) + 1)
    if expected_fingerprint and actual_fingerprint != expected_fingerprint:
        raise ScanFailure("PAGE_CONTEXT_LOST", "返回后列表内容指纹与离开前不一致")
    if not (verify_selected_tab(snapshot, "带货内容") and verify_selected_tab(snapshot, "直播")):
        raise ScanFailure("PAGE_CONTEXT_LOST", "返回后未保持“带货内容 → 直播”上下文")
    return snapshot


def verify_detail_identities(
    scanner: LiveScanner,
    observations: list[dict],
    *,
    list_url: str,
    product_id: str,
    filter_label: str,
    context_actions: list[str],
    terminal_snapshot: dict,
) -> None:
    """Verify each exposed creator URL in the detail page, restoring list context after each."""
    candidates: dict[str, str] = {}
    for row in observations:
        profile_url = row.get("_profile_url")
        account_name = row.get("account_name")
        if isinstance(profile_url, str) and extract_uid(profile_url) and isinstance(account_name, str):
            candidates.setdefault(profile_url, account_name)
    terminal_fingerprint = snapshot_fingerprint(
        terminal_snapshot, len(context_actions) + 1, len(context_actions) + 1,
    )
    for profile_url, account_name in candidates.items():
        expected_uid = extract_uid(profile_url)
        detail = scanner.navigate_raw(profile_url)
        ensure_no_human_block(detail)
        actual_url = extract_snapshot_url(detail) or ""
        actual_uid = extract_uid(actual_url)
        visible = re.sub(r"\s+", "", all_visible_text(detail))
        if actual_uid != expected_uid or re.sub(r"\s+", "", account_name) not in visible:
            scanner.capture_failure("identity-mismatch")
            raise ScanFailure("IDENTITY_MISMATCH", f"同行详情身份验证失败：{account_name}")
        for row in observations:
            if row.get("_profile_url") == profile_url:
                row["_detail_verified"] = True
        terminal_snapshot = restore_list_context(
            scanner,
            list_url=list_url,
            product_id=product_id,
            filter_label=filter_label,
            actions=context_actions,
            expected_fingerprint=terminal_fingerprint,
        )


def attach_identities(observations: list[dict]) -> tuple[list[dict], list[dict]]:
    """Resolve verified uid-bearing creator links and cache them for this scan."""
    cache: dict[str, dict] = {}
    unresolved = []
    for row in observations:
        identity = identity_from_observation(row)
        if identity:
            cache[identity["buyin_creator_uid"]] = identity
            row["buyin_creator_uid"] = identity["buyin_creator_uid"]
            row["identity_status"] = "VERIFIED"
            row["profile_url_redacted"] = identity["profile_url_redacted"]
        else:
            row["buyin_creator_uid"] = None
            row["identity_status"] = "UNRESOLVED"
            row["profile_url_redacted"] = None
            unresolved.append({
                "account_name": row.get("account_name"),
                "source_page": row.get("source_page"),
                "source_batch": row.get("source_batch"),
                "source_position": row.get("source_position"),
                "reason": "stable_buyin_creator_uid_not_verified",
            })
    return list(cache.values()), unresolved


def clean_observation(row: dict) -> dict:
    return {key: value for key, value in row.items() if not key.startswith("_") and key != "profile_url"}


def output_payloads(
    *,
    output_dir: Path,
    original_input: str,
    target_product_id: str,
    final_product_id: str | None,
    final_url: str | None,
    product_name: str | None,
    product_verified: bool,
    started_at: str,
    ended_at: str,
    time_filter: str | None,
    filter_label: str | None,
    filter_verified: bool,
    reported_total: int | None,
    pages: list[dict],
    observations: list[dict],
    end_signal: dict | None,
    errors: list[dict],
    status: str,
) -> tuple[dict, dict]:
    unique_creators, unresolved = attach_identities(observations)
    cleaned = [clean_observation(row) for row in observations]
    result = {
        "schema_version": 1,
        "profile_id": PROFILE_ID,
        "product": {
            "original_input": original_input,
            "target_product_id": target_product_id,
            "final_page_product_id": final_product_id,
            "name": product_name,
            "page_verified": product_verified,
            "final_navigation_url_redacted": redact_url(final_url or ""),
        },
        "scan_summary": {
            "status": status,
            "started_at": started_at,
            "ended_at": ended_at,
            "content_type": "live",
            "time_filter": time_filter,
            "filter_label": filter_label,
            "filter_verified": filter_verified,
            "page_reported_result_count": reported_total,
            "live_observation_count": len(cleaned),
            "unique_creator_count": len(unique_creators),
            "unresolved_identity_count": len(unresolved),
            "page_or_batch_count": len(pages),
            "end_signal": end_signal,
        },
        "observations": cleaned,
        "unique_creators": sorted(unique_creators, key=lambda item: item["buyin_creator_uid"]),
        "unresolved_identities": unresolved,
        "errors": errors,
    }
    manifest = {
        "schema_version": 1,
        "scan_id": output_dir.name,
        "profile_id": PROFILE_ID,
        "input_original": original_input,
        "target_product_id": target_product_id,
        "final_page_product_id": final_product_id,
        "product_name": product_name,
        "product_verification": "VERIFIED" if product_verified else "FAILED",
        "final_navigation_url_redacted": redact_url(final_url or ""),
        "content_type": "live",
        "time_filter": time_filter,
        "filter_label": filter_label,
        "filter_verified": filter_verified,
        "page_reported_result_count": reported_total,
        "actual_live_observation_count": len(cleaned),
        "unique_creator_count": len(unique_creators),
        "unresolved_identity_count": len(unresolved),
        "page_or_batch_count": len(pages),
        "end_signal": end_signal,
        "status": status,
        "error_types": [error.get("type") for error in errors],
        "started_at": started_at,
        "ended_at": ended_at,
        "output_files": {
            "result_json": str(output_dir / "result.json"),
            "result_csv": str(output_dir / "result.csv"),
            "scan_manifest_json": str(output_dir / "scan_manifest.json"),
        },
        "file_sha256": {},
        "side_effects": {
            "sqlite_written": False,
            "feishu_base_written": False,
            "runtime_worker_modified": False,
            "background_service_started": False,
        },
    }
    return result, manifest


def write_outputs(output_dir: Path, result: dict, manifest: dict) -> None:
    result_path = output_dir / "result.json"
    csv_path = output_dir / "result.csv"
    manifest_path = output_dir / "scan_manifest.json"
    result_data = json_bytes(result)
    atomic_write(result_path, result_data)

    csv_temp = csv_path.with_name(f".{csv_path.name}.{os.getpid()}.tmp")
    csv_temp.parent.mkdir(parents=True, exist_ok=True)
    with csv_temp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(result["observations"])
    csv_temp.chmod(0o600)
    csv_temp.replace(csv_path)

    manifest["file_sha256"] = {
        "result.json": sha256_file(result_path),
        "result.csv": sha256_file(csv_path),
    }
    canonical = dict(manifest)
    canonical["file_sha256"] = dict(manifest["file_sha256"])
    canonical["file_sha256"]["scan_manifest.json"] = None
    manifest["file_sha256"]["scan_manifest.json"] = {
        "sha256": sha256_bytes(json_bytes(canonical)),
        "scope": "canonical manifest with this value set to null",
    }
    atomic_write(manifest_path, json_bytes(manifest))


def run_realtime_check(output_dir: Path) -> None:
    """Validate live local prerequisites without consulting or updating capability caches."""
    if not CUA_DRIVER.is_file():
        raise ScanFailure("BROWSER_CONTROL_FAILED", "未找到cua-driver")
    storage_root = Path("/Volumes/ExternalStorage")
    if not storage_root.is_dir() or not os.path.ismount(storage_root):
        raise ScanFailure("STORAGE_UNAVAILABLE", "ExternalStorage未挂载")
    if storage_root.resolve() not in output_dir.resolve().parents:
        raise ScanFailure("STORAGE_UNAVAILABLE", "扫描输出目录必须位于ExternalStorage内")
    if shutil.disk_usage(storage_root).free < 50 * 1024**3:
        raise ScanFailure("STORAGE_LOW_SPACE", "外置盘剩余空间低于50GB安全阈值")
    if not CUA_SOCKET.exists():
        raise ScanFailure("BROWSER_CONTROL_FAILED", "竞品情报Profile专用cua-driver未就绪")
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        probe = output_dir / f".write-probe-{os.getpid()}"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as exc:
        raise ScanFailure("STORAGE_UNAVAILABLE", "扫描输出目录不可写") from exc


def fixture_run(capture_path: Path, identity_path: Path, output_dir: Path, original_input: str) -> tuple[dict, dict]:
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    identities = json.loads(identity_path.read_text(encoding="utf-8"))
    identity_by_name = {item["account_name"]: item for item in identities.get("accounts", [])}
    for page in capture.get("pages", []):
        for row in page.get("rows", []):
            evidence = identity_by_name.get(row.get("account_name"))
            if evidence:
                row["_profile_url"] = evidence.get("profile_url")
                row["_detail_verified"] = evidence.get("detail_page_verified") is True
    capture["end_signal"] = {
        "type": "platform_page_count_exhausted", "verified": True, "evidence": "fixture: page 3/3",
    }
    validated = validate_pages(capture, expected_total=32, require_complete=True, require_legacy_fields=True)
    observations = []
    for page_index, page in enumerate(validated["pages"], start=1):
        for position, row in enumerate(page["rows"], start=1):
            normalized = dict(row)
            normalized.update({
                "source_page": page_index, "source_batch": page_index,
                "source_position": position, "collected_at": capture.get("captured_at") or utc_now(),
            })
            observations.append(normalized)
    started_at = capture.get("captured_at") or utc_now()
    return output_payloads(
        output_dir=output_dir,
        original_input=original_input,
        target_product_id="3836325491917324425",
        final_product_id="3836325491917324425",
        final_url=f"https://{BUYIN_HOST}{DECISION_PATH}?id=3836325491917324425&universal_page_params_id=fixture",
        product_name=(capture.get("product") or {}).get("name"),
        product_verified=True,
        started_at=started_at,
        ended_at=utc_now(),
        time_filter="historical_fixture_scope",
        filter_label="历史fixture",
        filter_verified=True,
        reported_total=validated["reported_total"],
        pages=validated["pages"],
        observations=observations,
        end_signal=validated["end_signal"],
        errors=[],
        status="COMPLETE",
    )


def live_run(product_input: str, output_dir: Path) -> tuple[dict, dict]:
    started_at = utc_now()
    target_id, _ = extract_product_id(product_input)
    scanner = NativeTabbitScanner(CuaClient(), output_dir)
    current_url = None
    final_url = None
    final_id = None
    product_name = None
    time_filter = None
    filter_label = None
    filter_verified = False
    reported_total = None
    pages: list[dict] = []
    observations: list[dict] = []
    end_signal = None
    errors = []
    product_verified = False
    status = "FAILED"
    try:
        run_realtime_check(output_dir)
        current_url, _ = scanner.start()
        final_url = replace_decision_product_id(current_url, target_id)
        snapshot = scanner.navigate(final_url)
        actual_url = extract_snapshot_url(snapshot) or ""
        final_id = product_id_from_decision_url(actual_url)
        if final_id != target_id:
            scanner.capture_failure("product-id-mismatch")
            raise ScanFailure("PRODUCT_ID_MISMATCH", "加载后页面URL商品ID与目标ID不一致")
        scanner.completed_steps.append("target_product_id_verified")
        product_name = extract_product_name(snapshot)
        if not product_name:
            scanner.capture_failure("product-page-mismatch")
            raise ScanFailure("PRODUCT_PAGE_MISMATCH", "URL商品ID一致，但无法验证页面商品名称或关键信息")
        product_verified = True
        scanner.completed_steps.append("product_page_information_verified")
        snapshot, time_filter, filter_label = verify_live_surface(scanner, snapshot)
        filter_verified = extract_time_filter(snapshot)[2]
        observations, pages, end_signal, reported_total = scan_native_result_pages(
            scanner, snapshot, observations=observations, pages=pages,
        )
        if not filter_verified:
            if last_30_days_row_evidence(observations):
                filter_verified = True
                scanner.completed_steps.append("time_filter_verified_by_full_result_scope")
            else:
                raise ScanFailure(
                    "INCOMPLETE",
                    "已选择近30天，但当前结果中缺少可证明筛选范围的页面证据",
                    incomplete=True,
                    evidence={"filter_label": filter_label, "read_count": len(observations)},
                )
        if reported_total is not None and reported_total != len(observations):
            raise ScanFailure(
                "INCOMPLETE", "页面显示总数与实际读取直播内容数不一致", incomplete=True,
                evidence={"reported_total": reported_total, "read_count": len(observations)},
            )
        capture = {
            "product": {"reported_live_count": reported_total},
            "pages": pages or ([{"page": 1, "rows": []}] if reported_total == 0 else []),
            "end_signal": end_signal,
        }
        validate_pages(
            capture,
            expected_total=reported_total,
            require_complete=True,
            require_legacy_fields=False,
            require_unique_observations=False,
        )
        status = "COMPLETE"
    except ScanFailure as exc:
        status = "INCOMPLETE" if exc.incomplete else "FAILED"
        errors.append({"type": exc.error_type, "message": str(exc), "evidence": exc.evidence})
        if getattr(scanner, "target_id", None) or getattr(scanner, "pid", None):
            scanner.capture_failure("scan-stopped")
    except Exception as exc:
        status = "FAILED"
        errors.append({
            "type": "INTERNAL_VALIDATION_ERROR",
            "message": "扫描结果验证异常，已停止且未写入外部系统",
            "evidence": {"exception_class": exc.__class__.__name__},
        })
        if getattr(scanner, "target_id", None) or getattr(scanner, "pid", None):
            scanner.capture_failure("scan-stopped")
    finally:
        scanner.end()
    return output_payloads(
        output_dir=output_dir,
        original_input=product_input,
        target_product_id=target_id,
        final_product_id=final_id,
        final_url=final_url,
        product_name=product_name,
        product_verified=product_verified,
        started_at=started_at,
        ended_at=utc_now(),
        time_filter=time_filter,
        filter_label=filter_label,
        filter_verified=filter_verified,
        reported_total=reported_total,
        pages=pages,
        observations=observations,
        end_signal=end_signal,
        errors=errors,
        status=status,
    )


def failed_input_outputs(product_input: str, output_dir: Path, exc: ScanFailure) -> tuple[dict, dict]:
    now = utc_now()
    return output_payloads(
        output_dir=output_dir, original_input=product_input, target_product_id="",
        final_product_id=None, final_url=None, product_name=None, product_verified=False,
        started_at=now, ended_at=now, time_filter=None, filter_label=None,
        filter_verified=False, reported_total=None, pages=[], observations=[], end_signal=None,
        errors=[{"type": exc.error_type, "message": str(exc), "evidence": exc.evidence}], status="FAILED",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("product", nargs="?", help="Full product URL or direct numeric product ID")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--approved-read-only-dry-run", action="store_true")
    parser.add_argument("--fixture-capture", type=Path, help="Offline-only historical capture fixture")
    parser.add_argument("--fixture-identities", type=Path, help="Offline-only historical identity fixture")
    args = parser.parse_args()

    output_dir = (args.output_dir or (DEFAULT_OUTPUT_ROOT / compact_time_id())).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    product_input = args.product or ""
    if args.fixture_capture or args.fixture_identities:
        if not (args.fixture_capture and args.fixture_identities):
            parser.error("fixture mode requires both --fixture-capture and --fixture-identities")
        result, manifest = fixture_run(args.fixture_capture, args.fixture_identities, output_dir, product_input or "fixture")
    else:
        if not product_input:
            parser.error("product URL or product ID is required")
        if not args.approved_read_only_dry_run:
            parser.error("live mode requires --approved-read-only-dry-run")
        try:
            extract_product_id(product_input)
            result, manifest = live_run(product_input, output_dir)
        except ScanFailure as exc:
            result, manifest = failed_input_outputs(product_input, output_dir, exc)
    write_outputs(output_dir, result, manifest)
    first_error = (result.get("errors") or [{}])[0]
    print(json.dumps({"status": manifest["status"], "output_dir": str(output_dir), "error_type": first_error.get("type"), "error_message": first_error.get("message")}, ensure_ascii=False))
    return 0 if manifest["status"] == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
