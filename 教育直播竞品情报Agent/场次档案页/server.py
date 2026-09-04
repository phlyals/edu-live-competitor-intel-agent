#!/usr/bin/env python3
"""Local, read-only session archive service for the live competitor system."""
from __future__ import annotations

import json
import mimetypes
import os
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

RUNTIME = Path("/Users/mac/.hermes/profiles/edu_live_competitor_intel/runtime/v3")
sys.path.insert(0, str(RUNTIME))
import v3_runtime  # type: ignore  # noqa: E402

ROOT = Path(__file__).resolve().parent
MEDIA_ROOTS = [Path("/Volumes/ExternalStorage/同行直播录制").resolve()]
ANALYSIS_ROOT = Path("/Volumes/ExternalStorage/同行直播录制/analysis").resolve()
HOST = os.environ.get("SESSION_ARCHIVE_HOST", "127.0.0.1")
PORT = int(os.environ.get("SESSION_ARCHIVE_PORT", "8765"))


def json_object(value):
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def safe_path(raw: str | None) -> Path | None:
    if not raw:
        return None
    try:
        candidate = Path(str(raw)).expanduser().resolve()
    except (OSError, RuntimeError):
        return None
    if any(candidate == root or root in candidate.parents for root in MEDIA_ROOTS):
        return candidate
    return None


def safe_analysis_path(raw: str | None) -> Path | None:
    if not raw:
        return None
    try:
        candidate = Path(str(raw)).expanduser().resolve()
    except (OSError, RuntimeError):
        return None
    return candidate if candidate == ANALYSIS_ROOT or ANALYSIS_ROOT in candidate.parents else None


def compact_analysis(row: dict | None) -> dict:
    if not row:
        return {"status": "NOT_READY", "summary": {}}
    metadata = json_object(row.get("metadata_json"))
    path = safe_analysis_path(row.get("output_path"))
    payload = {}
    if path and path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
    result = payload.get("result") if isinstance(payload.get("result"), dict) else payload
    summary = {}
    for key in ("hook", "pain_points", "course_content", "interaction_patterns", "product_handoff", "cta", "claims", "risks"):
        values = result.get(key) if isinstance(result, dict) else None
        if not isinstance(values, list):
            continue
        compact = []
        for item in values[:8]:
            if isinstance(item, dict):
                text = item.get("summary") or item.get("text") or item.get("claim")
                if text:
                    compact.append({"text": str(text), "start": item.get("start"), "end": item.get("end")})
        if compact:
            summary[key] = compact
    return {"status": row.get("status"), "qualification_status": row.get("qualification_status"), "analysis_id": row.get("analysis_id"), "doc_url": str(metadata.get("feishu_doc_url") or ""), "summary": summary}


def db_rows(query: str, params=()):
    with v3_runtime.connect() as conn:
        return [dict(row) for row in conn.execute(query, params).fetchall()]


def db_row(query: str, params=()):
    with v3_runtime.connect() as conn:
        row = conn.execute(query, params).fetchone()
        return dict(row) if row else None


def human_bytes(value: int | None) -> str:
    size = float(value or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return "0 B"


def session_list(limit=100):
    limit = max(1, min(int(limit or 100), 200))
    return db_rows(
        """SELECT s.session_id,s.platform_session_id,s.started_at,s.ended_at,s.status,
                  s.completeness,s.source_url,s.metadata_json,
                  c.account_name,c.competitor_id,
                  m.live_status,
                  t.transcript_id,t.status AS transcript_status,t.output_path AS transcript_path,
                  t.metadata_json AS transcript_metadata,
                  a.analysis_id,a.status AS analysis_status,a.output_path AS analysis_path,
                  a.metadata_json AS analysis_metadata
           FROM live_sessions s
           LEFT JOIN monitor_targets m ON m.monitor_target_id=s.monitor_target_id
           LEFT JOIN competitors c ON c.competitor_id=m.competitor_id
           LEFT JOIN LATERAL (SELECT * FROM transcripts t0 WHERE t0.session_id=s.session_id
                              AND t0.status NOT IN ('CANCELLED_SUPERSEDED_SOURCE')
                              ORDER BY CASE WHEN t0.status='COMPLETE' THEN 0 ELSE 1 END,t0.updated_at DESC LIMIT 1) t ON TRUE
           LEFT JOIN LATERAL (SELECT * FROM analyses a0 WHERE a0.session_id=s.session_id
                              AND a0.lineage_state<>'SUPERSEDED'
                              ORDER BY CASE WHEN a0.status='COMPLETE' THEN 0 ELSE 1 END,a0.updated_at DESC LIMIT 1) a ON TRUE
           WHERE s.status<>'DUPLICATE_SUPERSEDED'
           ORDER BY COALESCE(s.started_at,s.session_id) DESC LIMIT ?""",
        (limit,),
    )


def transcript_payload(row: dict, *, offset=0, limit=100, query=""):
    metadata = json_object(row.get("metadata_json"))
    path = safe_path(row.get("output_path") or metadata.get("final_output_path"))
    segments = []
    if path and path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            segments = payload.get("segments") or []
        except (OSError, json.JSONDecodeError):
            segments = []
    query = str(query or "").strip().lower()
    if query:
        segments = [s for s in segments if query in str(s.get("text") or "").lower()]
    total = len(segments)
    offset = max(0, int(offset or 0))
    limit = max(1, min(int(limit or 100), 300))
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "segments": segments[offset : offset + limit],
        "source": str(path) if path else None,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "SessionArchive/1.0"

    def log_message(self, fmt, *args):
        return

    def send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: Path):
        try:
            body = path.read_bytes()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mimetypes.guess_type(path.name)[0] or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        qs = parse_qs(parsed.query)
        if path == "/" or path == "/index.html":
            return self.send_file(ROOT / "index.html")
        if path == "/style.css":
            return self.send_file(ROOT / "style.css")
        if path == "/app.js":
            return self.send_file(ROOT / "app.js")
        if path == "/api/health":
            return self.send_json({"ok": True, "service": "session-archive", "profile_id": "edu_live_competitor_intel"})
        if path == "/api/sessions":
            rows = session_list((qs.get("limit") or [100])[0])
            return self.send_json({"ok": True, "sessions": rows})
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 3 and parts[0] == "api" and parts[1] == "sessions":
            session_id = parts[2]
            session = db_row(
                """SELECT s.*,c.account_name,c.competitor_id,m.live_status,
                          j.status AS recording_status,j.completed_dir,j.partial_dir,j.updated_at AS recording_updated_at
                   FROM live_sessions s
                   LEFT JOIN monitor_targets m ON m.monitor_target_id=s.monitor_target_id
                   LEFT JOIN competitors c ON c.competitor_id=m.competitor_id
                   LEFT JOIN recording_jobs j ON j.session_id=s.session_id
                   WHERE s.session_id=? AND s.status<>'DUPLICATE_SUPERSEDED'""",
                (session_id,),
            )
            if not session:
                return self.send_json({"ok": False, "error": "session_not_found"}, HTTPStatus.NOT_FOUND)
            if len(parts) == 4 and parts[3] == "transcript":
                transcript = db_row(
                    "SELECT * FROM transcripts WHERE session_id=? AND status<>'CANCELLED_SUPERSEDED_SOURCE' ORDER BY CASE WHEN status='COMPLETE' THEN 0 ELSE 1 END,updated_at DESC LIMIT 1",
                    (session_id,),
                )
                if not transcript:
                    return self.send_json({"ok": True, "status": "NOT_READY", "total": 0, "segments": []})
                return self.send_json({"ok": True, "status": transcript.get("status"), **transcript_payload(transcript, offset=(qs.get("offset") or [0])[0], limit=(qs.get("limit") or [100])[0], query=(qs.get("q") or [""])[0])})
            transcript = db_row("SELECT * FROM transcripts WHERE session_id=? AND status<>'CANCELLED_SUPERSEDED_SOURCE' ORDER BY CASE WHEN status='COMPLETE' THEN 0 ELSE 1 END,updated_at DESC LIMIT 1", (session_id,))
            analysis = db_row("SELECT * FROM analyses WHERE session_id=? AND lineage_state<>'SUPERSEDED' ORDER BY CASE WHEN status='COMPLETE' THEN 0 ELSE 1 END,updated_at DESC LIMIT 1", (session_id,))
            session["transcript"] = {"status": transcript.get("status") if transcript else "NOT_READY", "doc_url": json_object(transcript.get("metadata_json") if transcript else "").get("feishu_doc_url", "")}
            session["analysis"] = compact_analysis(analysis)
            return self.send_json({"ok": True, "session": session})
        if len(parts) == 2 and parts[0] == "session":
            # Stable browser route; the client fetches the read-only API.
            return self.send_file(ROOT / "index.html")
        self.send_error(HTTPStatus.NOT_FOUND)


def main():
    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(json.dumps({"status": "READY", "url": f"http://{HOST}:{PORT}/", "profile_id": "edu_live_competitor_intel"}, ensure_ascii=False), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
