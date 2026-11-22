-- TrackLedger dashboard: one query per tile, all on workspace.trackledger.

-- Tile 1. How did the network perform? Monthly on-time arrivals and cancellations (train services only).
SELECT
  service_month,
  on_time_share,
  cancelled_share,
  planned_arrivals
FROM workspace.trackledger.report_punctuality_monthly
ORDER BY service_month;

-- Tile 2. What caused the disruptions? Disruptions per month by cause group, with the published total.
SELECT
  d.start_month,
  d.cause_group,
  d.disruptions,
  r.published AS published_total,
  r.status AS reconciliation_status
FROM workspace.trackledger.report_disruptions_monthly AS d
LEFT JOIN workspace.trackledger.reconciliation AS r
  ON r.period = d.start_month
  AND r.metric = 'disruption_count'
ORDER BY d.start_month, d.cause_group;

-- Tile 3. How did each operator perform? Year to date, largest operators first.
SELECT
  operator,
  SUM(planned_arrivals) AS planned_arrivals,
  SUM(CASE WHEN has_delay_data THEN on_time_arrivals END)
    / SUM(CASE WHEN has_delay_data THEN measured_arrivals END) AS on_time_share,
  SUM(cancelled_arrivals) / SUM(planned_arrivals) AS cancelled_share
FROM workspace.trackledger.report_operator_monthly
GROUP BY operator
ORDER BY planned_arrivals DESC;

-- Tile 4. Can I trust these numbers? Last run, files, row balance, quarantine and reconciliation.
WITH last_run AS (
  SELECT run_id
  FROM workspace.trackledger.run_log
  ORDER BY finished_at DESC
  LIMIT 1
),
tasks AS (
  SELECT l.*
  FROM workspace.trackledger.run_log AS l
  JOIN last_run USING (run_id)
),
ledger AS (
  SELECT
    COUNT(*) AS files_loaded,
    SUM(raw_rows) AS rows_loaded,
    SUM(CASE WHEN balanced THEN 0 ELSE 1 END) AS unbalanced_files
  FROM workspace.trackledger.file_ledger
  WHERE dataset = 'services' AND stage = 'clean'
),
recon AS (
  SELECT
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS checks_passed,
    SUM(CASE WHEN status = 'explained' THEN 1 ELSE 0 END) AS checks_explained,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS checks_failed
  FROM workspace.trackledger.reconciliation
)
SELECT
  MAX(t.run_id) AS run_id,
  MAX(t.finished_at) AS finished_at,
  MAX(CASE WHEN t.task = 'ingest' THEN t.files_processed END) AS new_files,
  MAX(CASE WHEN t.task = 'clean' THEN t.rows_quarantined END) AS rows_quarantined,
  MAX(CASE WHEN t.task = 'reconcile' THEN t.status END) AS reconcile_status,
  MAX(l.files_loaded) AS files_loaded,
  MAX(l.rows_loaded) AS rows_loaded,
  MAX(l.unbalanced_files) AS unbalanced_files,
  MAX(r.checks_passed) AS checks_passed,
  MAX(r.checks_explained) AS checks_explained,
  MAX(r.checks_failed) AS checks_failed
FROM tasks AS t
CROSS JOIN ledger AS l
CROSS JOIN recon AS r;
