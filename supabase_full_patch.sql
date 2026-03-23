-- ================================================================
--  supabase_full_patch.sql
--  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
--  Run in Supabase SQL Editor before applying app_full_patch.R
-- ================================================================

-- ── GAP 1: Add retirement fields (if not already added) ──────────
ALTER TABLE keyword_bank
  ADD COLUMN IF NOT EXISTS last_matched_at  timestamp,
  ADD COLUMN IF NOT EXISTS retired_at       timestamp,
  ADD COLUMN IF NOT EXISTS retired_by       text;

UPDATE keyword_bank
  SET last_matched_at = now()
  WHERE status = 'approved'
    AND last_matched_at IS NULL;

-- ── GAP 6: Keyword changelog view ─────────────────────────────────
-- (changelog stored in SQLite — this view shows Supabase side)
CREATE OR REPLACE VIEW keyword_bank_health AS
SELECT
  status,
  tier,
  language,
  COUNT(*)                                              AS total,
  SUM(times_matched)                                    AS total_matches,
  ROUND(AVG(times_matched))::integer                    AS avg_matches,
  COUNT(*) FILTER (
    WHERE last_matched_at < now() - INTERVAL '90 days'
       OR last_matched_at IS NULL
  )                                                     AS stale_count,
  MAX(last_matched_at)                                  AS most_recent_match
FROM keyword_bank
GROUP BY status, tier, language
ORDER BY status, tier;

-- ── Retirement candidates view ────────────────────────────────────
CREATE OR REPLACE VIEW keyword_retirement_candidates AS
SELECT
  id,
  keyword,
  tier,
  category,
  language,
  times_matched,
  last_matched_at,
  EXTRACT(DAY FROM now() - last_matched_at)::integer AS days_since_match,
  reviewed_by,
  created_at
FROM keyword_bank
WHERE status = 'approved'
  AND (
    last_matched_at < now() - INTERVAL '90 days'
    OR last_matched_at IS NULL
  )
ORDER BY last_matched_at ASC NULLS FIRST;

-- ── RPC: fetch retirement candidates (called from R) ─────────────
CREATE OR REPLACE FUNCTION get_stale_keywords(days_threshold integer DEFAULT 90)
RETURNS TABLE (
  id             bigint,
  keyword        text,
  tier           integer,
  category       text,
  language       text,
  times_matched  integer,
  last_matched_at timestamp
) AS $$
  SELECT id, keyword, tier, category, language,
         times_matched, last_matched_at
  FROM   keyword_bank
  WHERE  status = 'approved'
    AND  (last_matched_at < now() - make_interval(days => days_threshold)
          OR last_matched_at IS NULL)
  ORDER  BY last_matched_at ASC NULLS FIRST;
$$ LANGUAGE sql;

-- ── Check everything looks right ─────────────────────────────────
SELECT status, tier, COUNT(*) as n
FROM keyword_bank
GROUP BY status, tier
ORDER BY status, tier;
