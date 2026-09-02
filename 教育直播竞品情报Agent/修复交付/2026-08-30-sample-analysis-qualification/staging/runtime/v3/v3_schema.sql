PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS inbox_messages (
    inbox_id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    message_id TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    content TEXT NOT NULL,
    received_at TEXT NOT NULL,
    parsed_json TEXT NOT NULL DEFAULT '{}',
    task_id TEXT,
    UNIQUE(platform, message_id)
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    business_state TEXT NOT NULL,
    runtime_state TEXT NOT NULL,
    delivery_state TEXT NOT NULL,
    product_id TEXT,
    input_json TEXT NOT NULL DEFAULT '{}',
    current_step TEXT NOT NULL,
    resume_from TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_attempt_at TEXT,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    last_success_at TEXT,
    error_type TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS task_attempts (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    step TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    error_type TEXT,
    error_message TEXT,
    output_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(task_id, step, attempt_no)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    checkpoint_type TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    source_event_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(task_id, checkpoint_type)
);

CREATE TABLE IF NOT EXISTS domain_events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'INFO',
    created_at TEXT NOT NULL,
    object_type TEXT,
    object_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(event_type, object_type, object_id, created_at)
);

CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    platform_product_id TEXT NOT NULL,
    title TEXT,
    source_url TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(platform, platform_product_id)
);

CREATE TABLE IF NOT EXISTS competitors (
    competitor_id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    platform_account_id TEXT NOT NULL,
    account_name TEXT,
    account_status TEXT NOT NULL DEFAULT 'UNKNOWN',
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(platform, platform_account_id)
);

CREATE TABLE IF NOT EXISTS scan_runs (
    scan_id TEXT PRIMARY KEY,
    task_id TEXT REFERENCES tasks(task_id),
    product_id TEXT NOT NULL REFERENCES products(product_id),
    status TEXT NOT NULL,
    evidence_state TEXT NOT NULL DEFAULT 'INCOMPLETE',
    started_at TEXT,
    ended_at TEXT,
    imported_at TEXT NOT NULL,
    filter_label TEXT,
    filter_verified INTEGER NOT NULL DEFAULT 0,
    reported_total INTEGER,
    observed_count INTEGER NOT NULL DEFAULT 0,
    result_digest TEXT NOT NULL UNIQUE,
    result_path TEXT NOT NULL,
    manifest_path TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS scan_observations (
    observation_id TEXT PRIMARY KEY,
    scan_id TEXT NOT NULL REFERENCES scan_runs(scan_id),
    product_id TEXT NOT NULL REFERENCES products(product_id),
    competitor_id TEXT REFERENCES competitors(competitor_id),
    observation_index INTEGER NOT NULL,
    source_page INTEGER,
    source_batch INTEGER,
    source_position INTEGER,
    platform_observation_key TEXT NOT NULL,
    account_name TEXT,
    buyin_creator_uid TEXT,
    live_title TEXT,
    live_date TEXT,
    collected_at TEXT,
    identity_state TEXT NOT NULL DEFAULT 'WAITING_IDENTITY',
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(scan_id, observation_index)
);

CREATE TABLE IF NOT EXISTS identities (
    identity_id TEXT PRIMARY KEY,
    competitor_id TEXT NOT NULL REFERENCES competitors(competitor_id),
    platform TEXT NOT NULL,
    stable_id TEXT NOT NULL,
    canonical_url TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    verification_status TEXT NOT NULL,
    verified_at TEXT,
    UNIQUE(platform, stable_id)
);

CREATE TABLE IF NOT EXISTS product_competitors (
    relation_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL REFERENCES products(product_id),
    competitor_id TEXT NOT NULL REFERENCES competitors(competitor_id),
    relation_status TEXT NOT NULL DEFAULT 'UNKNOWN',
    absent_streak INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_scan_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(product_id, competitor_id)
);

CREATE TABLE IF NOT EXISTS monitor_targets (
    monitor_target_id TEXT PRIMARY KEY,
    competitor_id TEXT NOT NULL REFERENCES competitors(competitor_id),
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    live_url TEXT NOT NULL,
    live_status TEXT NOT NULL DEFAULT 'UNKNOWN',
    last_checked_at TEXT,
    next_check_at TEXT,
    consecutive_unknown INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(competitor_id)
);

CREATE TABLE IF NOT EXISTS live_sessions (
    session_id TEXT PRIMARY KEY,
    monitor_target_id TEXT NOT NULL REFERENCES monitor_targets(monitor_target_id),
    platform_session_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'DETECTED',
    started_at TEXT,
    ended_at TEXT,
    completeness TEXT NOT NULL DEFAULT 'UNKNOWN',
    source_url TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(monitor_target_id, platform_session_id)
);

CREATE TABLE IF NOT EXISTS recording_jobs (
    job_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    status TEXT NOT NULL,
    pid INTEGER,
    account_key TEXT NOT NULL,
    recording_key TEXT NOT NULL,
    partial_dir TEXT NOT NULL,
    completed_dir TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    restart_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    UNIQUE(session_id),
    UNIQUE(recording_key)
);

CREATE TABLE IF NOT EXISTS recording_segments (
    segment_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    path TEXT NOT NULL,
    checksum TEXT,
    captured_from TEXT NOT NULL,
    captured_to TEXT,
    status TEXT NOT NULL DEFAULT 'PARTIAL',
    bytes INTEGER NOT NULL DEFAULT 0,
    lifecycle_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    superseded_by_segment_id TEXT REFERENCES recording_segments(segment_id),
    lifecycle_updated_at TEXT,
    UNIQUE(session_id, path)
);

CREATE TABLE IF NOT EXISTS transcripts (
    transcript_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    source_digest TEXT NOT NULL,
    engine TEXT NOT NULL,
    model TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    language TEXT,
    source_path TEXT,
    output_path TEXT,
    low_confidence_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(session_id, source_digest)
);

CREATE TABLE IF NOT EXISTS analyses (
    analysis_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    transcript_id TEXT REFERENCES transcripts(transcript_id),
    analysis_type TEXT NOT NULL,
    source_digest TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    output_path TEXT,
    lineage_state TEXT NOT NULL DEFAULT 'CURRENT',
    scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    transcript_content_digest TEXT,
    analysis_spec_version TEXT,
    model_version TEXT,
    prompt_version TEXT,
    artifact_digest TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_analyses_transcript_spec
ON analyses(transcript_id, analysis_type, transcript_content_digest, analysis_spec_version, model_version, prompt_version)
WHERE transcript_id IS NOT NULL AND transcript_content_digest IS NOT NULL
  AND analysis_spec_version IS NOT NULL AND model_version IS NOT NULL AND prompt_version IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_analyses_transcript_id ON analyses(transcript_id);
CREATE INDEX IF NOT EXISTS idx_analyses_qualification ON analyses(scope, qualification_status, lineage_state, status);

CREATE TABLE IF NOT EXISTS approvals (
    approval_id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    requested_version TEXT,
    decision TEXT NOT NULL DEFAULT 'PENDING',
    decided_by TEXT,
    decided_at TEXT,
    nonce_hash TEXT,
    notes TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(object_type, object_id, requested_version)
);

CREATE TABLE IF NOT EXISTS outbox (
    outbox_id TEXT PRIMARY KEY,
    dedupe_key TEXT NOT NULL UNIQUE,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 8,
    next_attempt_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_until TEXT,
    last_attempt_at TEXT,
    last_error_type TEXT,
    last_error TEXT,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    receipt_json TEXT NOT NULL DEFAULT '{}',
    scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED'
);

CREATE TABLE IF NOT EXISTS delivery_receipts (
    receipt_id TEXT PRIMARY KEY,
    outbox_id TEXT NOT NULL REFERENCES outbox(outbox_id),
    destination TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    verified_at TEXT,
    receipt_json TEXT NOT NULL DEFAULT '{}',
    scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    UNIQUE(outbox_id)
);

CREATE INDEX IF NOT EXISTS idx_outbox_qualification ON outbox(scope, qualification_status, status);

CREATE TABLE IF NOT EXISTS dead_letters (
    dead_letter_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    reason_type TEXT NOT NULL,
    reason TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(source_type, source_id)
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    edge_id TEXT PRIMARY KEY,
    downstream_type TEXT NOT NULL,
    downstream_id TEXT NOT NULL,
    upstream_type TEXT NOT NULL,
    upstream_id TEXT NOT NULL,
    upstream_version TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'CURRENT',
    created_at TEXT NOT NULL,
    UNIQUE(downstream_type, downstream_id, upstream_type, upstream_id, upstream_version)
);

CREATE TABLE IF NOT EXISTS evidence_bundles (
    bundle_id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'REQUIRED',
    manifest_path TEXT,
    manifest_hash TEXT,
    verified_at TEXT,
    scope TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    qualification_status TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(object_type, object_id)
);

CREATE TABLE IF NOT EXISTS strategy_candidates (
    candidate_id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES live_sessions(session_id),
    analysis_id TEXT REFERENCES analyses(analysis_id),
    strategy_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING_REVIEW',
    source_digest TEXT NOT NULL,
    content_path TEXT,
    lineage_state TEXT NOT NULL DEFAULT 'CURRENT',
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(strategy_type, source_digest)
);

CREATE TABLE IF NOT EXISTS knowledge_versions (
    version_id TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    version_no INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'DRAFT',
    content_path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    supersedes_version_id TEXT REFERENCES knowledge_versions(version_id),
    approved_by TEXT,
    approved_at TEXT,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(object_key, version_no),
    UNIQUE(object_key, content_hash)
);

CREATE TABLE IF NOT EXISTS knowledge_diffs (
    diff_id TEXT PRIMARY KEY,
    candidate_id TEXT REFERENCES strategy_candidates(candidate_id),
    base_version_id TEXT REFERENCES knowledge_versions(version_id),
    proposed_version_no INTEGER,
    status TEXT NOT NULL DEFAULT 'PENDING_REVIEW',
    diff_path TEXT NOT NULL,
    diff_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(diff_hash)
);

CREATE TABLE IF NOT EXISTS review_items (
    review_id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    review_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    requested_at TEXT NOT NULL,
    requested_by TEXT,
    decided_at TEXT,
    decided_by TEXT,
    decision_notes TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(object_type, object_id, review_type)
);

CREATE TABLE IF NOT EXISTS retention_jobs (
    retention_job_id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    not_before TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_attempt_at TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(object_type, object_id, policy_name)
);

CREATE TABLE IF NOT EXISTS heartbeats (
    service_name TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    last_heartbeat_at TEXT NOT NULL,
    last_success_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    run_id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,
    differences_json TEXT NOT NULL DEFAULT '[]'
);

-- Runtime V3 Final control-plane additions.  Leases and evidence are separate
-- durable objects so a worker restart cannot be confused with business state.
CREATE TABLE IF NOT EXISTS task_leases (
    lease_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(task_id),
    worker_id TEXT NOT NULL,
    fencing_token BIGINT NOT NULL,
    acquired_at TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    released_at TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    UNIQUE(task_id, fencing_token)
);

CREATE TABLE IF NOT EXISTS identity_evidence (
    evidence_id TEXT PRIMARY KEY,
    identity_id TEXT NOT NULL REFERENCES identities(identity_id),
    evidence_type TEXT NOT NULL,
    source_url TEXT,
    source_path TEXT,
    source_digest TEXT,
    captured_at TEXT NOT NULL,
    verification_status TEXT NOT NULL DEFAULT 'PENDING',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(identity_id, evidence_type, source_digest)
);

CREATE TABLE IF NOT EXISTS identity_conflicts (
    conflict_id TEXT PRIMARY KEY,
    competitor_id TEXT NOT NULL REFERENCES competitors(competitor_id),
    identity_id TEXT REFERENCES identities(identity_id),
    conflict_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    detected_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(competitor_id, identity_id, conflict_type, status)
);

CREATE TABLE IF NOT EXISTS worker_nodes (
    node_id TEXT PRIMARY KEY,
    node_role TEXT NOT NULL,
    hostname TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'UNKNOWN',
    fencing_epoch BIGINT NOT NULL DEFAULT 0,
    last_heartbeat_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS recording_leases (
    lease_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    node_id TEXT NOT NULL,
    fencing_token BIGINT NOT NULL,
    acquired_at TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    UNIQUE(session_id, status)
);

CREATE TABLE IF NOT EXISTS recording_gaps (
    gap_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    gap_from TEXT NOT NULL,
    gap_to TEXT,
    reason TEXT NOT NULL,
    detected_by TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(session_id, gap_from, reason)
);

CREATE TABLE IF NOT EXISTS media_manifests (
    manifest_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES live_sessions(session_id),
    status TEXT NOT NULL DEFAULT 'PENDING',
    manifest_path TEXT,
    manifest_hash TEXT,
    segment_count BIGINT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    complete_from TEXT,
    complete_to TEXT,
    verified_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(session_id)
);

CREATE TABLE IF NOT EXISTS deployment_releases (
    release_id TEXT PRIMARY KEY,
    release_version TEXT NOT NULL UNIQUE,
    deployment_state TEXT NOT NULL,
    fleet_generation BIGINT NOT NULL DEFAULT 0,
    cutover_epoch BIGINT NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    manifest_hash TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS projection_reconciliations (
    reconciliation_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    table_id TEXT NOT NULL,
    run_id TEXT REFERENCES reconciliation_runs(run_id),
    direction TEXT NOT NULL,
    status TEXT NOT NULL,
    missing_count BIGINT NOT NULL DEFAULT 0,
    extra_count BIGINT NOT NULL DEFAULT 0,
    mismatch_count BIGINT NOT NULL DEFAULT 0,
    differences_json TEXT NOT NULL DEFAULT '[]',
    checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capacity_test_runs (
    test_id TEXT PRIMARY KEY,
    target_concurrency BIGINT NOT NULL,
    duration_seconds BIGINT NOT NULL,
    status TEXT NOT NULL,
    evidence_path TEXT,
    evidence_hash TEXT,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL,
    ended_at TEXT
);

CREATE TABLE IF NOT EXISTS fault_drill_runs (
    drill_id TEXT PRIMARY KEY,
    drill_type TEXT NOT NULL,
    status TEXT NOT NULL,
    evidence_path TEXT,
    evidence_hash TEXT,
    metrics_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL,
    ended_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id TEXT PRIMARY KEY,
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    object_type TEXT,
    object_id TEXT,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(status, lease_until, updated_at);
CREATE INDEX IF NOT EXISTS idx_outbox_claim ON outbox(status, next_attempt_at, lease_until);
CREATE INDEX IF NOT EXISTS idx_monitor_schedule ON monitor_targets(status, next_check_at);
CREATE INDEX IF NOT EXISTS idx_recording_jobs_status ON recording_jobs(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_scan_runs_product ON scan_runs(product_id, imported_at);
CREATE INDEX IF NOT EXISTS idx_scan_observations_scan ON scan_observations(scan_id, observation_index);
CREATE INDEX IF NOT EXISTS idx_delivery_receipts_object ON delivery_receipts(object_type, object_id);
CREATE INDEX IF NOT EXISTS idx_dead_letters_source ON dead_letters(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_review_items_status ON review_items(status, requested_at);
CREATE INDEX IF NOT EXISTS idx_retention_jobs_claim ON retention_jobs(status, next_attempt_at, lease_until);
CREATE INDEX IF NOT EXISTS idx_lineage_downstream ON lineage_edges(downstream_type, downstream_id);
CREATE INDEX IF NOT EXISTS idx_task_leases_claim ON task_leases(task_id, status, lease_until);
CREATE INDEX IF NOT EXISTS idx_identity_evidence_identity ON identity_evidence(identity_id, verification_status);
CREATE INDEX IF NOT EXISTS idx_identity_conflicts_status ON identity_conflicts(status, detected_at);
CREATE INDEX IF NOT EXISTS idx_worker_nodes_heartbeat ON worker_nodes(status, last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_recording_leases_claim ON recording_leases(session_id, status, lease_until);
CREATE INDEX IF NOT EXISTS idx_recording_gaps_session ON recording_gaps(session_id, gap_from);
CREATE INDEX IF NOT EXISTS idx_projection_reconciliations_run ON projection_reconciliations(run_id, status);
CREATE INDEX IF NOT EXISTS idx_recording_segments_lifecycle ON recording_segments(lifecycle_status, session_id);

INSERT INTO schema_meta(key, value) VALUES ('schema_version', '3')
ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now');
