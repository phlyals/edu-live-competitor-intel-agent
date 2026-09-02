"""Acceptance-only I/O. Never import the business runtime or initialise its DB."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import quote

DEFAULT_CONFIG = Path.home() / ".hermes/profiles/edu_live_competitor_intel/runtime/v3/v3_config.json"
DEFAULT_OUTPUT = Path(__file__).resolve().parent.parent / "验收结果"
EXPECTED_MODULES = ["开场", "干货", "需求", "信任", "商品承接", "成交", "答疑"]


class CheckError(Exception):
    def __init__(self, code, message, status="ERROR"):
        super().__init__(message)
        self.code, self.status = code, status


def fail(code, message, status="ERROR"):
    raise CheckError(code, message, status)


def json_object(value, label):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            fail("INVALID_JSON", f"格式异常：{label} 不是合法 JSON", "FORMAT_ERROR")
    if not isinstance(value, dict):
        fail("INVALID_OBJECT", f"格式异常：{label} 必须是 JSON 对象", "FORMAT_ERROR")
    return value


def metadata(row):
    return json_object(row.get("metadata_json") or {}, "metadata_json")


def local_path(value, args):
    if not isinstance(value, str) or not value or "://" in value or "\0" in value:
        fail("NONLOCAL_PATH", "需要本地文件路径；不会访问 URL 或外部 API")
    path = Path(value).expanduser()
    if not path.is_absolute():
        if not args.data_root:
            fail("RELATIVE_PATH", "数据库含相对路径，请通过 --data-root 指定解析根目录")
        path = Path(args.data_root).expanduser().resolve() / path
    return path.resolve()


def load_artifact(row, json_column, args):
    inline = row.get(json_column)
    output = row.get("output_path")
    if inline is not None and output:
        fail("AMBIGUOUS_STORAGE", "格式异常：同时存在内联 JSON 和 output_path，无法确定真本", "FORMAT_ERROR")
    if inline is not None:
        return json_object(inline, json_column)
    if output:
        path = local_path(output, args)
        if not path.is_file():
            fail("ARTIFACT_NOT_FOUND", f"产物文件不存在：{path}", "FAIL")
        try:
            return json_object(path.read_text(encoding="utf-8"), path.name)
        except (OSError, UnicodeError):
            fail("ARTIFACT_READ_FAILED", "无法读取 UTF-8 产物文件")
    # V3 also persists the complete ASR result in metadata_json.
    if json_column == "transcript_json" and "segments" in metadata(row):
        return metadata(row)
    fail("ARTIFACT_NOT_FOUND", "没有可读取的本地产物或内联 JSON", "FAIL")


def number(value):
    if isinstance(value, bool) or value is None:
        raise ValueError("not a number")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("not finite")
    return value


def interval(row, allow_point=False):
    """Explicit seconds only; never guess milliseconds, wall time or prose."""
    if not isinstance(row, dict):
        raise ValueError("timestamp must be an object")
    pairs = [(a, b) for a, b in [("start", "end"), ("start_time", "end_time")]
             if a in row or b in row]
    if len(pairs) != 1:
        raise ValueError("missing or ambiguous timestamps")
    a, b = pairs[0]
    start, end = number(row.get(a)), number(row.get(b))
    if start < 0 or end < start or (end == start and not allow_point):
        raise ValueError("invalid timestamp interval")
    return start, end


def segment(start, end):
    return {"start_time": start, "end_time": end}


def probe(path, args, audio=False):
    if not path.is_file():
        fail("MEDIA_NOT_FOUND", f"媒体文件不存在：{path}", "FAIL")
    # Only local file protocols; a playlist must not cause network requests.
    command = [args.ffprobe, "-v", "error", "-protocol_whitelist", "file,pipe",
               "-show_entries", "format=duration,size:stream=codec_type,duration",
               "-of", "json", str(path)]
    try:
        proc = subprocess.run(command, capture_output=True, text=True, timeout=args.probe_timeout)
    except FileNotFoundError:
        fail("FFPROBE_NOT_FOUND", "找不到 FFprobe，请安装或指定 --ffprobe")
    except subprocess.TimeoutExpired:
        fail("FFPROBE_TIMEOUT", "FFprobe 读取超时")
    if proc.returncode:
        fail("FFPROBE_FAILED", "FFprobe 无法读取媒体；文件可能损坏或格式不受支持")
    try:
        payload = json.loads(proc.stdout)
        fmt = payload["format"]
        streams = payload.get("streams", [])
        if audio and not any(s.get("codec_type") == "audio" for s in streams):
            fail("NO_AUDIO_STREAM", "文件中没有音频流", "FAIL")
        duration = number(fmt["duration"])
        size = number(fmt["size"])
        if duration <= 0 or size <= 0:
            raise ValueError("invalid duration or size")
    except (KeyError, TypeError, ValueError):
        fail("INVALID_MEDIA_METADATA", "FFprobe 未返回有效时长和大小")
    return duration, size


def expected_duration(session):
    try:
        values = []
        for key in ("started_at", "ended_at"):
            value = session.get(key)
            if not isinstance(value, datetime):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            values.append(value)
        # Mixing timezone-aware and naive dates is intentionally rejected.
        result = (values[1] - values[0]).total_seconds()
        if result <= 0:
            raise ValueError("non-positive session duration")
        return result
    except (AttributeError, TypeError, ValueError):
        fail("INVALID_SESSION_TIME", "场次尚未结束，或起止时间缺失/无效/时区不一致", "UNCOMPUTABLE")


class Repository:
    """All table names are fixed; both identifiers and values are parameterised."""
    def __init__(self, conn, args):
        from psycopg2 import sql
        self.conn, self.args, self.sql = conn, args, sql
        layout = args.layout
        if layout == "auto":
            with conn.cursor() as cur:
                cur.execute("SELECT table_name FROM information_schema.tables "
                            "WHERE table_schema=%s AND table_name IN ('sessions','live_sessions')",
                            (args.db_schema,))
                names = {row[0] for row in cur.fetchall()}
            if len(names) != 1:
                fail("AMBIGUOUS_LAYOUT", "未找到唯一场次表，请明确指定 --layout")
            layout = "runtime-v3" if "live_sessions" in names else "canonical"
        self.layout = layout

    def rows(self, table, session_id):
        from psycopg2.extras import RealDictCursor
        statement = self.sql.SQL("SELECT * FROM {}.{} WHERE session_id=%s").format(
            self.sql.Identifier(self.args.db_schema), self.sql.Identifier(table))
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(statement, (session_id,))
            return [dict(row) for row in cur.fetchall()]

    def session(self, session_id):
        rows = self.rows("live_sessions" if self.layout == "runtime-v3" else "sessions", session_id)
        if len(rows) != 1:
            fail("SESSION_NOT_FOUND", "场次不存在或 session_id 不唯一", "FAIL")
        return rows[0]

    def recording_path(self, session):
        if self.layout == "canonical":
            return local_path(session.get("recording_path"), self.args)
        rows = self.rows("recording_segments", session["session_id"])
        paths = {str(local_path(r["path"], self.args)) for r in rows if r.get("path")}
        full = {p for p in paths if Path(p).name == "整场直播.ts"}
        candidates = full or paths
        if len(candidates) != 1:
            fail("RECORDING_NOT_UNIQUE", "未找到唯一整场录像；不会拼接或累计分段时长", "FAIL")
        return Path(next(iter(candidates)))

    def transcript(self, session_id, transcript_id=None):
        rows = self.rows("transcripts", session_id)
        if transcript_id:
            rows = [r for r in rows if str(r.get("transcript_id")) == transcript_id]
        else:
            full = [r for r in rows if metadata(r).get("coverage_scope") == "FULL_SESSION"
                    and metadata(r).get("sample_only") is not True]
            if full:
                rows = full
            elif self.layout == "runtime-v3":
                fail("NO_FULL_TRANSCRIPT", "没有标记 FULL_SESSION 的整场转录，样本不能代表整场", "UNCOMPUTABLE")
        if not rows:
            fail("TRANSCRIPT_NOT_FOUND", "没有对应转录结果", "UNCOMPUTABLE")
        if len(rows) != 1:
            fail("AMBIGUOUS_TRANSCRIPT", "存在多个转录版本，请指定 --transcript-id", "UNCOMPUTABLE")
        row = rows[0]
        if row.get("status", "COMPLETE") != "COMPLETE":
            fail("TRANSCRIPT_NOT_COMPLETE", "转录尚未完成或已失效", "UNCOMPUTABLE")
        return row

    def report(self, session_id):
        rows = self.rows("analyses" if self.layout == "runtime-v3" else "analysis_reports", session_id)
        key = "analysis_id" if self.layout == "runtime-v3" else "report_id"
        if self.args.report_id:
            rows = [r for r in rows if str(r.get(key)) == self.args.report_id]
        if not rows:
            fail("REPORT_NOT_FOUND", "未找到分析报告", "FAIL")
        if len(rows) != 1:
            fail("AMBIGUOUS_REPORT", "格式异常：存在多个报告，请指定 --report-id", "FORMAT_ERROR")
        row = rows[0]
        if row.get("status", "COMPLETE") != "COMPLETE" or row.get("lineage_state", "CURRENT") != "CURRENT":
            fail("REPORT_NOT_CURRENT", "报告尚未完成或已失效", "FAIL")
        return row


@contextmanager
def repository(args):
    try:
        import psycopg2
    except ImportError:
        fail("MISSING_DEPENDENCY", "请安装 requirements.txt 中的 psycopg2 驱动")
    dsn = os.environ.get("ACCEPTANCE_DATABASE_URL")
    if not dsn:
        try:
            config = json.loads(Path(args.config).expanduser().read_text(encoding="utf-8"))
            dsn = config.get("postgresql", {}).get("dsn")
        except (OSError, ValueError, AttributeError):
            fail("CONFIG_ERROR", "无法读取数据库配置；请设置 ACCEPTANCE_DATABASE_URL 或 --config")
    if not dsn:
        fail("CONFIG_ERROR", "缺少 PostgreSQL DSN")
    conn = None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=10, application_name="edu_acceptance_readonly",
                               options="-c default_transaction_read_only=on -c statement_timeout=15000")
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)
        yield Repository(conn, args)
    except psycopg2.Error as exc:
        # Never leak connection strings, passwords or SQL parameter data.
        fail("DATABASE_ERROR", f"数据库只读访问失败（SQLSTATE={exc.pgcode or 'connection'}）")
    finally:
        if conn is not None:
            try:
                conn.rollback()
            finally:
                conn.close()


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        fail("ARGUMENT_ERROR", "命令行参数无效，请使用 --help 查看用法")


def parser(description):
    result = JsonArgumentParser(description=description)
    result.add_argument("session_id")
    result.add_argument("--config", default=str(DEFAULT_CONFIG))
    result.add_argument("--layout", choices=["auto", "runtime-v3", "canonical"], default="auto")
    result.add_argument("--db-schema", default="public")
    result.add_argument("--data-root")
    result.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    result.add_argument("--ffprobe", default="ffprobe")
    result.add_argument("--probe-timeout", type=float, default=60)
    result.add_argument("--transcript-id")
    result.add_argument("--report-id")
    return result


def output_path(args, script_name):
    # Preserve ordinary IDs exactly. Escape path separators and control chars.
    safe = "".join(quote(char, safe="") if char in "/\\%" or ord(char) < 32 or ord(char) == 127 else char
                   for char in args.session_id)
    name = f"{safe}_{script_name}.json"
    if not args.session_id or len(name.encode("utf-8")) > 255:
        fail("INVALID_SESSION_ID", "场次 ID 为空或过长，无法按约定生成安全文件名")
    return Path(args.output_dir).expanduser().resolve() / name


def write_result(path, result):
    payload = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".acceptance-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
        temporary.replace(path)
    finally:
        if temporary and temporary.exists():
            temporary.unlink()
    return payload


def run_cli(script_name, description, defaults, check):
    result = defaults()
    path = None
    try:
        args = parser(description).parse_args()
        result.update(session_id=args.session_id, script=script_name,
                      checked_at=datetime.now(timezone.utc).isoformat())
        path = output_path(args, script_name)
        if not math.isfinite(args.probe_timeout) or args.probe_timeout <= 0:
            fail("ARGUMENT_ERROR", "--probe-timeout 必须大于零")
        with repository(args) as repo:
            check(args, repo, result)
    except CheckError as exc:
        result.update(status=exc.status, error_code=exc.code, message=str(exc))
    except Exception as exc:
        result.update(status="ERROR", error_code="UNEXPECTED_ERROR",
                      message=f"验收未完成（{type(exc).__name__}）；未输出底层错误以避免泄露配置")
    try:
        payload = write_result(path, result) if path else json.dumps(result, ensure_ascii=False, indent=2)
    except OSError:
        result.update(status="ERROR", error_code="OUTPUT_WRITE_FAILED", message="无法写入验收结果目录")
        payload = json.dumps(result, ensure_ascii=False, indent=2)
    print(payload, end="" if payload.endswith("\n") else "\n")
    return 2 if result.get("status") == "ERROR" else (0 if result.get("status") == "PASS" else 1)
