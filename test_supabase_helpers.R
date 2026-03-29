# ================================================================
#  test_supabase_helpers.R — verify supabase_helpers.R works
#  Run this BEFORE migrating app.R
#  Usage: source("test_supabase_helpers.R")
# ================================================================

readRenviron("secrets/.Renviron")
source("supabase_helpers.R")

# ── Test 1: Connection ────────────────────────────────────────────
cat("\n── Test 1: Connection\n")
cat(sprintf("  ping: %s\n", if (supa_ping()) "✅ OK" else "❌ FAILED"))

# ── Test 2: Insert a test case ───────────────────────────────────
cat("\n── Test 2: Insert test case\n")
test_case <- data.frame(
  case_id      = "TEST-00001",
  county       = "Nairobi",
  sub_location = "Kibera",
  src_lat      = -1.3133,
  src_lng      = 36.7833,
  platform     = "YouTube",
  source       = "test",
  handle       = "@test_user",
  tweet_text   = "This is a test case from supabase_helpers.R",
  language     = "English",
  ncic_level   = 2L,
  ncic_label   = "Prejudice / Stereotyping",
  section_13   = 0L,
  confidence   = "78%",
  conf_num     = 78L,
  conf_band    = "High",
  category     = "Ethnicity",
  risk_score   = 45L,
  risk_level   = "MEDIUM",
  network_score = 20L,
  signals      = "Test signal",
  timestamp    = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
  notes        = "",
  stringsAsFactors = FALSE
)
supa_save_cases(test_case)
cat("  insert: ✅ done\n")

# ── Test 3: Load cases ────────────────────────────────────────────
cat("\n── Test 3: Load cases\n")
cases <- supa_load_cases()
if (!is.null(cases) && nrow(cases) > 0) {
  cat(sprintf("  loaded: ✅ %d cases\n", nrow(cases)))
  cat(sprintf("  columns: %s\n", paste(names(cases)[1:8], collapse=", ")))
  cat(sprintf("  tweet_text field: %s\n",
              if ("tweet_text" %in% names(cases)) "✅ present" else "❌ missing"))
} else {
  cat("  ❌ No cases returned\n")
}

# ── Test 4: Audit log ─────────────────────────────────────────────
cat("\n── Test 4: Audit log\n")
supa_audit("omwangi", "Officer Mwangi", "TEST",
           case_id="TEST-00001", detail="supabase_helpers test")
cat("  audit write: ✅ done\n")

audit <- supa_load_audit(limit=3)
if (is.data.frame(audit) && nrow(audit) > 0) {
  cat(sprintf("  audit read: ✅ %d entries\n", nrow(audit)))
} else {
  cat("  ❌ No audit entries\n")
}

# ── Test 5: Delete test case ──────────────────────────────────────
cat("\n── Test 5: Cleanup test case\n")
supa_delete("cases", "case_id", "TEST-00001")
cat("  cleanup: ✅ done\n")

cat("\n── All tests complete ──\n")
cat("If all show ✅ run: source('migrate_to_supabase.R')\n")
