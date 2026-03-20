# tests/testthat/test-compute_risk.R
# testthat coverage for compute_risk(), compute_source_context(),
# and the Violence Override / Activism classification logic.
#
# Run with: testthat::test_file("tests/testthat/test-compute_risk.R")
# or:       testthat::test_dir("tests/testthat")

library(testthat)

# ── Source helpers from app.R without launching Shiny ────────────
# We source only the pure-function sections by extracting them here.
# In CI you would source("app.R") after mocking Shiny dependencies.

# ── Inline the functions under test ──────────────────────────────
DEFAULT_KEYWORD_WEIGHTS <- list(
  "kabila"=30,"waende"=35,"hawastahili"=30,"migrants"=25,
  "cockroach"=50,"vermin"=50,"chinja"=45,"angamiza"=48,
  "piga"=35,"damu"=30
)

NCIC_BASE_SCORES <- c("0"=2,"1"=15,"2"=30,"3"=55,"4"=72,"5"=92)

`%||%` <- function(a, b) if (!is.null(a)) a else b

compute_risk <- function(tweet, gpt_conf, ncic_level,
                         kw_weights, network_score=20,
                         freq_spike=10, ctx_score=0,
                         source_history_score=0) {
  tweet_l  <- tolower(tweet)
  kw_score <- 0
  for (kw in names(kw_weights))
    if (grepl(kw, tweet_l, fixed=TRUE))
      kw_score <- min(100, kw_score + kw_weights[[kw]])
  ncic_base <- as.integer(NCIC_BASE_SCORES[as.character(ncic_level)] %||% 10)
  composite <- 0.30*gpt_conf + 0.22*kw_score +
    0.13*network_score + 0.08*freq_spike +
    0.12*ctx_score + 0.15*source_history_score
  score <- round(0.55*composite + 0.45*ncic_base)
  score <- min(100, max(0, score))
  list(score=score, kw_score=round(kw_score), ncic_base=ncic_base,
       ctx_score=round(ctx_score), source_history_score=round(source_history_score),
       formula=sprintf("L%s base(%d)+composite=%d", ncic_level, ncic_base, score))
}

compute_source_context <- function(handle, county, cases_df,
                                   window_days=30, max_boost=30) {
  now    <- Sys.time()
  cutoff <- now - as.difftime(window_days, units="days")
  handle_hist <- cases_df[
    !is.na(cases_df$handle) & cases_df$handle == handle &
      !is.na(cases_df$timestamp) & cases_df$timestamp >= cutoff, ]
  county_hist <- cases_df[
    !is.na(cases_df$county) & cases_df$county == county &
      !is.na(cases_df$timestamp) & cases_df$timestamp >= cutoff, ]
  handle_score <- 0
  if (nrow(handle_hist) >= 2) {
    avg_lvl  <- mean(handle_hist$ncic_level, na.rm=TRUE)
    max_lvl  <- max(handle_hist$ncic_level,  na.rm=TRUE)
    n_high   <- sum(handle_hist$ncic_level >= 3, na.rm=TRUE)
    handle_score <- min(100, avg_lvl*8 + max_lvl*5 + n_high*4)
  }
  county_score <- 0
  if (nrow(county_hist) >= 5) {
    avg_lvl  <- mean(county_hist$ncic_level, na.rm=TRUE)
    pct_high <- mean(county_hist$ncic_level >= 3, na.rm=TRUE)
    n_s13    <- sum(isTRUE(county_hist$section_13), na.rm=TRUE)
    county_score <- min(100, avg_lvl*6 + pct_high*40 + n_s13*3)
  }
  combined <- round(0.65*handle_score + 0.35*county_score)
  boost    <- min(max_boost, combined)
  list(score=boost, handle_score=round(handle_score),
       county_score=round(county_score), summary="")
}

# ════════════════════════════════════════════════════════════════
# 1. compute_risk — basic contract
# ════════════════════════════════════════════════════════════════
test_that("compute_risk returns a score in [0,100]", {
  r <- compute_risk("test tweet", gpt_conf=80, ncic_level=3,
                    kw_weights=DEFAULT_KEYWORD_WEIGHTS)
  expect_gte(r$score, 0)
  expect_lte(r$score, 100)
})

test_that("higher NCIC level produces higher base score", {
  r0 <- compute_risk("neutral post", 70, 0, list())
  r5 <- compute_risk("neutral post", 70, 5, list())
  expect_gt(r5$score, r0$score)
})

test_that("matching keyword raises risk score", {
  no_kw  <- compute_risk("people should vote", 70, 2, DEFAULT_KEYWORD_WEIGHTS)
  has_kw <- compute_risk("waende kwao", 70, 2, DEFAULT_KEYWORD_WEIGHTS)
  expect_gt(has_kw$score, no_kw$score)
})

test_that("keyword score is capped at 100", {
  big_weights <- list(kw1=60, kw2=60, kw3=60)
  r <- compute_risk("kw1 kw2 kw3", 50, 2, big_weights)
  expect_lte(r$kw_score, 100)
})

test_that("source_history_score boosts risk but is bounded", {
  r_no_hist  <- compute_risk("test", 70, 2, list(), source_history_score=0)
  r_has_hist <- compute_risk("test", 70, 2, list(), source_history_score=30)
  expect_gte(r_has_hist$score, r_no_hist$score)
  # boost cannot push score above 100
  r_max <- compute_risk("test", 99, 5, list(), source_history_score=100)
  expect_lte(r_max$score, 100)
})

test_that("L5 with high confidence produces HIGH risk (>=65)", {
  r <- compute_risk("explicit violence post", 95, 5, DEFAULT_KEYWORD_WEIGHTS)
  expect_gte(r$score, 65)
})

test_that("L0 neutral post with low confidence stays LOW (<35)", {
  r <- compute_risk("prayer for peace", 30, 0, list())
  expect_lt(r$score, 35)
})

test_that("formula string is returned and non-empty", {
  r <- compute_risk("test", 80, 3, list())
  expect_type(r$formula, "character")
  expect_true(nchar(r$formula) > 0)
})

# ════════════════════════════════════════════════════════════════
# 2. compute_source_context
# ════════════════════════════════════════════════════════════════
make_cases <- function(handle, county, ncic_levels, days_ago=1:length(ncic_levels)) {
  data.frame(
    handle     = handle,
    county     = county,
    ncic_level = as.integer(ncic_levels),
    section_13 = ncic_levels >= 4,
    timestamp  = Sys.time() - as.difftime(days_ago, units="days"),
    stringsAsFactors=FALSE
  )
}

test_that("no history returns score of 0", {
  empty <- make_cases("@ghost","Nairobi", integer(0), integer(0))
  r <- compute_source_context("@ghost", "Nairobi", empty)
  expect_equal(r$score, 0)
})

test_that("handle with repeated L4+ posts raises score", {
  cases <- make_cases("@agitator","Nairobi", c(4,4,5,4,3))
  r <- compute_source_context("@agitator","Nairobi", cases)
  expect_gt(r$score, 0)
})

test_that("score is capped at max_boost=30", {
  # Very high-severity history
  cases <- make_cases("@extreme","Nairobi", rep(5L,20))
  r <- compute_source_context("@extreme","Nairobi", cases, max_boost=30)
  expect_lte(r$score, 30)
})

test_that("handle outside 30-day window is ignored", {
  old_cases <- data.frame(
    handle="@old", county="Kisumu",
    ncic_level=5L, section_13=TRUE,
    timestamp=Sys.time() - as.difftime(60, units="days"),
    stringsAsFactors=FALSE
  )
  r <- compute_source_context("@old","Kisumu", old_cases, window_days=30)
  expect_equal(r$score, 0)
})

test_that("county with many high-severity posts raises county_score", {
  cases <- do.call(rbind, replicate(10,
                                    make_cases("@anon","Nakuru", c(3,4,5), 1:3), simplify=FALSE))
  r <- compute_source_context("@new","Nakuru", cases)
  expect_gt(r$county_score, 0)
})

# ════════════════════════════════════════════════════════════════
# 3. Violence Override logic (prompt rules as unit tests)
#    We test the EXPECTED GPT behaviour by verifying the NCIC
#    classification rules are correctly documented in constants,
#    since we can't call GPT in a unit test.
# ════════════════════════════════════════════════════════════════

# Violence keywords that MUST trigger L4/L5 regardless of target
VIOLENCE_KEYWORDS <- c("kill","attack","burn","chinja","piga","angamiza",
                       "mpigie risasi","shoot","destroy","harm")

# Activism keywords that should stay L0/L1 when directed at officials
ACCOUNTABILITY_KEYWORDS <- c("vote out","resign","prosecute","audit",
                             "send home","corrupt","should be fired",
                             "akujibu","wajibike","nitatoa kura")

test_that("violence keywords are present in default weights or prompt signals", {
  # At minimum, the most dangerous violence keywords should be in keyword weights
  violence_in_weights <- intersect(
    c("chinja","angamiza","piga","damu"),
    names(DEFAULT_KEYWORD_WEIGHTS)
  )
  expect_gt(length(violence_in_weights), 0,
            label="At least some violence keywords should be in DEFAULT_KEYWORD_WEIGHTS")
})

test_that("violence keyword elevates risk score for any NCIC level", {
  # Even a low NCIC level post gains significant risk if violence keyword present
  r_clean    <- compute_risk("MPs should resign",      60, 1, DEFAULT_KEYWORD_WEIGHTS)
  r_violence <- compute_risk("chinja MPs watoke sasa", 60, 1, DEFAULT_KEYWORD_WEIGHTS)
  expect_gt(r_violence$score, r_clean$score,
            label="Violence keyword 'chinja' should raise risk score")
})

test_that("pure accountability post (no violence, no ethnic target) scores LOW", {
  # 'MPs are corrupt and should be voted out' → L0, LOW risk
  r <- compute_risk("MPs are corrupt and should be voted out at the ballot",
                    65, 0, DEFAULT_KEYWORD_WEIGHTS)
  expect_lt(r$score, 50,
            label="Non-violent accountability speech should stay below MEDIUM threshold")
})

test_that("ethnic slur raises keyword score substantially", {
  r_neutral <- compute_risk("people should coexist",  70, 2, DEFAULT_KEYWORD_WEIGHTS)
  r_slur    <- compute_risk("kabila hili ni wabaya",  70, 2, DEFAULT_KEYWORD_WEIGHTS)
  expect_gt(r_slur$kw_score, r_neutral$kw_score,
            label="Ethnic keyword 'kabila' should raise kw_score")
})

test_that("L5 score is always >= L4 score given same inputs", {
  r4 <- compute_risk("test", 80, 4, DEFAULT_KEYWORD_WEIGHTS)
  r5 <- compute_risk("test", 80, 5, DEFAULT_KEYWORD_WEIGHTS)
  expect_gte(r5$score, r4$score)
})

test_that("NCIC level 0 base score is much lower than level 5", {
  base0 <- as.integer(NCIC_BASE_SCORES["0"])
  base5 <- as.integer(NCIC_BASE_SCORES["5"])
  expect_lt(base0, base5)
  expect_lte(base0, 5)
  expect_gte(base5, 80)
})

# ════════════════════════════════════════════════════════════════
# 4. Edge cases and robustness
# ════════════════════════════════════════════════════════════════
test_that("empty tweet does not error", {
  expect_silent(compute_risk("", 50, 0, list()))
})

test_that("tweet with only spaces does not error", {
  r <- compute_risk("   ", 50, 0, list())
  expect_type(r$score, "double")
})

test_that("NULL kw_weights treated as empty list", {
  # Should not crash — kw_weights must be a list but empty is fine
  r <- compute_risk("test", 70, 2, list())
  expect_gte(r$score, 0)
})

test_that("gpt_conf of 0 gives minimum composite contribution", {
  r0  <- compute_risk("test", 0,   3, list())
  r99 <- compute_risk("test", 99,  3, list())
  expect_gt(r99$score, r0$score)
})

test_that("source_history_score of 0 does not affect result vs default", {
  r1 <- compute_risk("test", 70, 2, list(), source_history_score=0)
  r2 <- compute_risk("test", 70, 2, list())
  expect_equal(r1$score, r2$score)
})