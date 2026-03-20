# ================================================================
#  EARLY WARNING SIGNALS  —  v4
#  Kenya National Cohesion Intelligence Platform
#  R Shiny + bslib | NCIC Framework | HITL Learning | Async GPT
# ================================================================
#  NEW IN v4:
#   1.  App renamed: Early Warning Signals
#   2.  6-level NCIC taxonomy replacing binary labels:
#       0=Neutral · 1=Offensive · 2=Prejudice · 3=Dehumanization
#       4=Hate Speech · 5=Toxic
#   3.  Full NCIC legal framework (Cap 170, Section 13) in GPT prompt
#   4.  Officer identity tracked (login name stored, shown in all records)
#   5.  Validation drives RL: keyword weights adapt per confirmation,
#       few-shot example bank grows, confidence calibration runs
#   6.  Observer registry prevents duplicate observer accumulation
#   7.  Async cache: all cache writes on main thread (no race condition)
#   8.  Officer note persisted across pagination pages
#   9.  NCIC level override by officer, disagreements logged
#  10.  Officer performance dashboard (validations, accuracy, speed)
#  11.  NCIC report export (Section 13 cases, escalation queue)
#  12.  Plotly everywhere, shared reactive data, date filters global
#
#  Install (run once):
#   install.packages(c("shiny","bslib","bsicons","leaflet",
#     "leaflet.extras2","DT","dplyr","writexl","httr2","jsonlite",
#     "shinyjs","digest","future","promises","plotly","later",
#     "highcharter","sf","tmap","tools","DBI","RSQLite"))
#
#  secrets/.Renviron:
#   OPENAI_API_KEY=sk-proj-...
#   GMAIL_USER=you@gmail.com
#   GMAIL_PASS=xxxx xxxx xxxx xxxx
#   OFFICER_EMAIL=officer@ncic.go.ke
# ================================================================

readRenviron("secrets/.Renviron")

library(shiny);    library(bslib);      library(bsicons)
library(leaflet);  library(leaflet.extras2)
library(DT);       library(dplyr);      library(writexl)
library(httr2);    library(jsonlite);   library(shinyjs)
library(digest);   library(future);     library(promises)
library(plotly);   library(later);    library(highcharter)
library(sf);       library(tmap)
library(DBI);      library(RSQLite)

plan(multisession)
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── CONSTANTS ────────────────────────────────────────────────────
APP_NAME         <- "Radicalisation Signals"
APP_SUBTITLE     <- "AI Early Warning Platform for Kenya"
OFFICER_PASSWORD <- "intel2025"
OPENAI_MODEL     <- "gpt-4o-mini"
CACHE_DIR        <- "cache"
CACHE_FILE       <- file.path(CACHE_DIR, "classify_cache.rds")  # GPT response cache (RDS)
DB_FILE          <- file.path(CACHE_DIR, "ews.sqlite")           # persistent SQLite store
VAL_PAGE_SIZE    <- 6

# ── NCIC 6-LEVEL TAXONOMY ───────────────────────────────────────
NCIC_LEVELS <- c(
  "0" = "Neutral Discussion",
  "1" = "Offensive Language",
  "2" = "Prejudice / Stereotyping",
  "3" = "Dehumanization",
  "4" = "Hate Speech",
  "5" = "Toxic"
)

NCIC_COLORS <- c(
  "0" = "#198754",  # green       — Neutral Discussion
  "1" = "#85b800",  # yellow-green — Offensive Language
  "2" = "#ffc107",  # amber        — Prejudice / Stereotyping
  "3" = "#fd7e14",  # orange       — Dehumanization
  "4" = "#dc3545",  # red          — Hate Speech
  "5" = "#7b0000"   # deep red     — Toxic
)

NCIC_BASE_SCORES <- c("0"=2,"1"=15,"2"=30,"3"=55,"4"=72,"5"=92)

# Okabe-Ito color-blind safe palette for platform/category charts
OKABE_ITO <- c("#E69F00","#56B4E9","#009E73","#F0E442","#0072B2","#D55E00","#CC79A7","#000000")

NCIC_ACTIONS <- c(
  "0" = "No action required. Log for trend analysis.",
  "1" = "Monitor. Flag for weekly review.",
  "2" = "Document pattern. Request platform review if repeated.",
  "3" = "Issue monitoring flag. Initiate engagement with poster.",
  "4" = "Section 13 threshold met. Require takedown. Alert DCI.",
  "5" = "IMMEDIATE ESCALATION. Press charges. Contact DCI now."
)

NCIC_SECTION13 <- c("0"=FALSE,"1"=FALSE,"2"=FALSE,"3"=FALSE,"4"=TRUE,"5"=TRUE)

# maps old ml_labels to NCIC levels for seed data
LABEL_TO_NCIC <- c(
  "TOXIC"       = "5",
  "HATE SPEECH" = "4",
  "PENDING"     = "2",
  "SAFE"        = "0"
)

ncic_color   <- function(lvl) NCIC_COLORS[as.character(lvl)]   %||% "#6c757d"
ncic_name    <- function(lvl) NCIC_LEVELS[as.character(lvl)]   %||% "Unknown"
ncic_action  <- function(lvl) NCIC_ACTIONS[as.character(lvl)]  %||% "Monitor."
ncic_s13     <- function(lvl) isTRUE(NCIC_SECTION13[as.character(lvl)])


# ── SQLite DATABASE LAYER ─────────────────────────────────────────
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive=TRUE)

db_connect <- function() dbConnect(RSQLite::SQLite(), DB_FILE)

db_init <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS cases (",
    "case_id TEXT PRIMARY KEY, county TEXT, sub_location TEXT,",
    "src_lat REAL, src_lng REAL, target_county TEXT, tgt_lat REAL, tgt_lng REAL,",
    "target_group TEXT, platform TEXT, handle TEXT, tweet_text TEXT, language TEXT,",
    "ncic_level INTEGER, ncic_label TEXT, section_13 INTEGER, confidence TEXT,",
    "conf_num INTEGER, conf_band TEXT, category TEXT,",
    "validated_by TEXT, validated_at TEXT, action_taken TEXT,",
    "officer_ncic_override INTEGER, risk_score INTEGER, risk_level TEXT,",
    "risk_formula TEXT, kw_score INTEGER, network_score INTEGER,",
    "signals TEXT, trend_data TEXT, timestamp TEXT, timestamp_chr TEXT,",
    "notes TEXT DEFAULT '')"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS keyword_weights (",
    "keyword TEXT PRIMARY KEY, weight REAL)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS examples (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, tweet TEXT, ncic_level TEXT,",
    "category TEXT, reasoning TEXT, outcome TEXT, ts TEXT)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS disagreements (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, officer TEXT,",
    "case_id TEXT, tweet TEXT, gpt_level INTEGER, officer_level INTEGER)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS api_failures (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "case_id TEXT, tweet_text TEXT, error_msg TEXT, ",
    "attempt INTEGER DEFAULT 1, ts TEXT, resolved INTEGER DEFAULT 0)"))
  message("[db] SQLite initialised at ", DB_FILE)
}

db_init()

# ── Cases ────────────────────────────────────────────────────────
db_load_cases <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "cases")
  if (nrow(d) == 0) return(NULL)
  d$timestamp  <- as.POSIXct(d$timestamp, tz="Africa/Nairobi")
  d$section_13 <- as.logical(d$section_13)
  d
}

db_save_cases <- function(df) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbWithTransaction(con, {
    dbExecute(con, "DELETE FROM cases")
    dw <- df
    dw$timestamp  <- format(df$timestamp, "%Y-%m-%d %H:%M:%S")
    dw$section_13 <- as.integer(df$section_13)
    dbWriteTable(con, "cases", dw, append=TRUE)
  })
  invisible(TRUE)
}

db_update_case <- function(case_id, fields) {
  # Atomic update of specific fields for one case — used during validation
  con <- db_connect(); on.exit(dbDisconnect(con))
  cols <- names(fields)
  set_sql <- paste(sprintf('"%s" = :%s', cols, cols), collapse=", ")
  sql <- sprintf('UPDATE cases SET %s WHERE case_id = :dot_cid', set_sql)
  dbExecute(con, sql, params=c(fields, list(dot_cid=case_id)))
}

# ── API Failure Queue ────────────────────────────────────────────
db_log_failure <- function(case_id, tweet_text, error_msg, attempt=1L) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
            "INSERT INTO api_failures (case_id,tweet_text,error_msg,attempt,ts,resolved) VALUES (:case_id,:tweet,:err,:attempt,:ts,0)",
            params=list(case_id=case_id, tweet=substr(tweet_text,1,100),
                        err=substr(error_msg,1,200), attempt=as.integer(attempt),
                        ts=format(Sys.time(),"%Y-%m-%d %H:%M:%S")))
}

db_resolve_failure <- function(case_id) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con, "UPDATE api_failures SET resolved=1 WHERE case_id=:cid AND resolved=0",
            params=list(cid=case_id))
}

db_load_failures <- function(unresolved_only=TRUE) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  sql <- if (unresolved_only)
    "SELECT * FROM api_failures WHERE resolved=0 ORDER BY ts DESC"
  else
    "SELECT * FROM api_failures ORDER BY ts DESC LIMIT 100"
  tryCatch(dbGetQuery(con, sql),
           error=function(e) data.frame())
}

db_clear_failures <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con, "UPDATE api_failures SET resolved=1")
}

# ── Keyword weights ──────────────────────────────────────────────
db_load_weights <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "keyword_weights")
  if (nrow(d) == 0) return(NULL)
  as.list(setNames(d$weight, d$keyword))
}

db_save_weights <- function(w) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbWithTransaction(con, {
    dbExecute(con, "DELETE FROM keyword_weights")
    dbWriteTable(con, "keyword_weights",
                 data.frame(keyword=names(w), weight=unlist(w),
                            stringsAsFactors=FALSE), append=TRUE)
  })
  invisible(TRUE)
}

# ── Examples (few-shot bank) ─────────────────────────────────────
load_examples <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "examples")
  if (nrow(d) == 0) return(list())
  lapply(seq_len(nrow(d)), function(i)
    list(tweet=d$tweet[i], ncic_level=d$ncic_level[i],
         category=d$category[i], reasoning=d$reasoning[i],
         outcome=d$outcome[i], ts=as.POSIXct(d$ts[i])))
}

save_examples <- function(ex) invisible(NULL)  # legacy stub

add_example <- function(tweet, ncic_level, category, reasoning, outcome) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
            "INSERT INTO examples (tweet,ncic_level,category,reasoning,outcome,ts) VALUES (:tweet,:ncic_level,:category,:reasoning,:outcome,:ts)",
            params=list(tweet=tweet, ncic_level=as.character(ncic_level),
                        category=category %||% "", reasoning=reasoning %||% "",
                        outcome=outcome, ts=format(Sys.time(),"%Y-%m-%d %H:%M:%S")))
}

# ── Disagreements ────────────────────────────────────────────────
load_disagreements <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "disagreements")
  if (nrow(d) == 0)
    return(data.frame(ts=character(), officer=character(), case_id=character(),
                      tweet=character(), gpt_level=integer(), officer_level=integer(),
                      stringsAsFactors=FALSE))
  d[, c("ts","officer","case_id","tweet","gpt_level","officer_level")]
}

log_disagreement <- function(officer, case_id, tweet, gpt_lvl, off_lvl) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
            "INSERT INTO disagreements (ts,officer,case_id,tweet,gpt_level,officer_level) VALUES (:ts,:officer,:case_id,:tweet,:gpt_level,:officer_level)",
            params=list(ts=format(Sys.time(),"%Y-%m-%d %H:%M"), officer=officer,
                        case_id=case_id, tweet=substr(tweet,1,80),
                        gpt_level=as.integer(gpt_lvl), officer_level=as.integer(off_lvl)))
}

# ── ADAPTIVE KEYWORD WEIGHTS ─────────────────────────────────────
DEFAULT_KEYWORD_WEIGHTS <- list(
  "kabila"=30,"waende"=35,"hawastahili"=30,"migrants"=25,
  "extremists"=28,"tutawaonyesha"=40,"wezi"=32,"nguvu"=28,
  "never be trusted"=35,"go back"=30,"cockroach"=50,
  "vermin"=50,"funga"=28,"waondoke"=38,"sisi na wao"=32,
  "damu"=30,"piga"=35,"chinja"=45,"angamiza"=48
)

kw_weights_global <- db_load_weights() %||% DEFAULT_KEYWORD_WEIGHTS

save_kw_weights <- function(w) {
  db_save_weights(w)
}

# update keyword weights from officer validation (HITL Layer 1)
update_kw_weights <- function(tweet, ncic_level, action, weights) {
  tweet_l  <- tolower(tweet)
  lr       <- 0.02 * max(1, as.integer(ncic_level))
  for (kw in names(weights)) {
    if (grepl(kw, tweet_l, fixed=TRUE)) {
      weights[[kw]] <- if (action %in% c("CONFIRMED","ESCALATED"))
        min(100, weights[[kw]] * (1 + lr))
      else if (action == "CLEARED")
        max(5,   weights[[kw]] * (1 - lr))
      else
        max(5,   weights[[kw]] * (1 - lr * 0.4))
    }
  }
  if (action %in% c("CONFIRMED","ESCALATED") && as.integer(ncic_level) >= 3) {
    words <- unique(unlist(strsplit(tolower(tweet), "\\W+")))
    words <- words[nchar(words) >= 4 & !words %in% names(weights)]
    for (w in words) { weights[[w]] <- 12 }
  }
  weights
}

# ── FEW-SHOT builder ─────────────────────────────────────────────
build_few_shot <- function() {
  ex <- load_examples()
  if (length(ex) == 0) return("")
  confirmed <- Filter(function(e) e$outcome %in% c("CONFIRMED","ESCALATED"), ex)
  cleared   <- Filter(function(e) e$outcome == "CLEARED", ex)
  shots     <- c(tail(confirmed, 3), tail(cleared, 1))
  if (length(shots) == 0) return("")
  paste0(
    "VALIDATED EXAMPLES FROM KENYAN OFFICERS:\n\n",
    paste(sapply(shots, function(e) sprintf(
      'Post: "%s"\nNCIC Level: %s — %s\nOfficer outcome: %s\nReasoning: %s',
      e$tweet, e$ncic_level, ncic_name(e$ncic_level), e$outcome, e$reasoning
    )), collapse="\n---\n"),
    "\n\nUsing the same standards, now classify:\n"
  )
}

# ── GPT RESPONSE CACHE (RDS — fast in-memory key/value) ──────────
classify_cache    <- new.env(hash=TRUE, parent=emptyenv())
observer_registry <- new.env(hash=TRUE, parent=emptyenv())

load_rds_safe <- function(f, default) {
  if (file.exists(f)) tryCatch(readRDS(f), error=function(e) default) else default
}

load_cache <- function() {
  saved <- load_rds_safe(CACHE_FILE, list())
  for (k in names(saved)) assign(k, saved[[k]], envir=classify_cache)
  message(sprintf("[cache] %d entries loaded", length(saved)))
}

save_cache <- function() {
  tryCatch(saveRDS(as.list(classify_cache), CACHE_FILE),
           error=function(e) message("[cache] save failed: ", e$message))
}

load_cache()

# ── RISK FORMULA ─────────────────────────────────────────────────
conf_band <- function(conf) {
  if (conf>=90) "Very High" else if (conf>=75) "High" else
    if (conf>=60) "Moderate"  else "Low"
}

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
  
  list(
    score                = score,
    kw_score             = round(kw_score),
    ncic_base            = ncic_base,
    ctx_score            = round(ctx_score),
    source_history_score = round(source_history_score),
    formula              = sprintf(
      "NCIC L%s base(%d) + 0.30×conf(%d) + 0.22×kw(%d) + 0.13×net(%d) + 0.12×ctx(%d) + 0.15×src_hist(%d) = %d",
      ncic_level, ncic_base, round(gpt_conf), round(kw_score),
      round(network_score), round(ctx_score), round(source_history_score), score)
  )
}

# ── RESCORE: propagate weight changes back through all cases ──────
# Called after every validation so historical scores stay current.
rescore_all_cases <- function(cases_df, kw_weights) {
  for (i in seq_len(nrow(cases_df))) {
    rs <- compute_risk(
      cases_df$tweet_text[i],
      cases_df$conf_num[i],
      cases_df$ncic_level[i],
      kw_weights,
      cases_df$network_score[i],
      10, 0, 0   # source_history excluded for batch speed
    )
    cases_df$risk_score[i]   <- rs$score
    cases_df$risk_formula[i] <- rs$formula
    cases_df$risk_level[i]   <- if (rs$score >= 65) "HIGH" else
      if (rs$score >= 35) "MEDIUM" else "LOW"
  }
  cases_df
}

# ── SOURCE CONTEXT SCORING ────────────────────────────────────────
# Analyses historical posts from the same handle and/or county to
# produce a source_history_score (0–100) that feeds into risk formula.
# A handle with a pattern of L3+ posts gets a higher baseline risk
# even if the current post looks moderate in isolation.

compute_source_context <- function(handle, county, cases_df,
                                   window_days=30, max_boost=30) {
  now  <- Sys.time()
  cutoff <- now - as.difftime(window_days, units="days")
  
  # ── Handle history ────────────────────────────────────────────
  handle_hist <- cases_df[
    !is.na(cases_df$handle) &
      cases_df$handle == handle &
      !is.na(cases_df$timestamp) &
      cases_df$timestamp >= cutoff, ]
  
  handle_score <- 0
  handle_note  <- ""
  if (nrow(handle_hist) >= 2) {
    avg_lvl    <- mean(handle_hist$ncic_level, na.rm=TRUE)
    max_lvl    <- max(handle_hist$ncic_level,  na.rm=TRUE)
    n_high     <- sum(handle_hist$ncic_level >= 3, na.rm=TRUE)
    escalating <- if (nrow(handle_hist) >= 3) {
      # check if recent posts are trending upward in severity
      recent <- tail(handle_hist[order(handle_hist$timestamp), "ncic_level"], 5)
      sum(diff(recent) > 0) >= 2
    } else FALSE
    
    handle_score <- min(100,
                        avg_lvl * 8 +            # avg NCIC level weighted
                          max_lvl * 5 +            # worst single post
                          n_high  * 4 +            # count of L3+ posts
                          if (escalating) 15 else 0  # escalating pattern bonus
    )
    handle_note <- sprintf(
      "Handle %s: %d posts in %dd · avg L%.1f · max L%d · %d high-severity%s",
      handle, nrow(handle_hist), window_days, avg_lvl, max_lvl, n_high,
      if (escalating) " · ESCALATING PATTERN" else ""
    )
  }
  
  # ── County/region history ─────────────────────────────────────
  county_hist <- cases_df[
    !is.na(cases_df$county) &
      cases_df$county == county &
      !is.na(cases_df$timestamp) &
      cases_df$timestamp >= cutoff, ]
  
  county_score <- 0
  county_note  <- ""
  if (nrow(county_hist) >= 5) {
    avg_lvl  <- mean(county_hist$ncic_level, na.rm=TRUE)
    pct_high <- mean(county_hist$ncic_level >= 3, na.rm=TRUE)  # proportion L3+
    n_s13    <- sum(isTRUE(county_hist$section_13), na.rm=TRUE)
    
    county_score <- min(100,
                        avg_lvl    * 6 +
                          pct_high   * 40 +   # % of high-severity in county drives risk up
                          n_s13      * 3
    )
    county_note <- sprintf(
      "County %s: %d posts in %dd · avg L%.1f · %.0f%% high-severity · %d S13",
      county, nrow(county_hist), window_days,
      avg_lvl, pct_high*100, n_s13
    )
  }
  
  # Combine: handle history weighted higher than county baseline
  combined <- round(0.65 * handle_score + 0.35 * county_score)
  # Cap boost so source context never overwhelms the actual post content
  boost    <- min(max_boost, combined)
  
  list(
    score        = boost,
    handle_score = round(handle_score),
    county_score = round(county_score),
    handle_note  = handle_note,
    county_note  = county_note,
    summary      = if (nchar(handle_note)>0 || nchar(county_note)>0)
      paste(c(handle_note, county_note)[nchar(c(handle_note,county_note))>0],
            collapse=" | ")
    else ""
  )
}

# ── SUB-LOCATION LOOKUP ───────────────────────────────────────────
SUB_LOCATIONS <- list(
  "Nairobi"          = c("Kibera","Kayole","Mathare","Korogocho","Mukuru","Eastleigh","Kasarani","Embakasi","Ruaraka","Dagoretti","Langata","Westlands","Pumwani","Kamukunji","Makadara"),
  "Mombasa"          = c("Likoni","Kisauni","Changamwe","Mvita","Nyali","Jomvu","Miritini","Tudor","Bamburi","Shanzu"),
  "Kisumu"           = c("Kondele","Nyalenda","Manyatta","Bandani","Migosi","Kolwa","Winam","Kisumu East","Seme","Muhoroni"),
  "Nakuru"           = c("Nakuru East","Nakuru West","Naivasha","Gilgil","Molo","Rongai","Njoro","Subukia","Bahati","Kuresoi"),
  "Uasin Gishu"      = c("Langas","Huruma","Pioneer","Kimumu","Moiben","Turbo","Kapsabet","Nandi Hills","Burnt Forest","Eldoret CBD"),
  "Garissa"          = c("Garissa Township","Dadaab","Fafi","Ijara","Lagdera","Balambala","Hulugho"),
  "Mandera"          = c("Mandera East","Mandera West","Mandera North","Banissa","Lafey","Kutulo"),
  "Kakamega"         = c("Kakamega Central","Mumias","Lugari","Malava","Matungu","Butere","Khwisero","Shinyalu","Likuyani"),
  "Meru"             = c("Meru Town","Imenti North","Imenti South","Tigania West","Igembe South","Buuri","Imenti Central"),
  "Nyeri"            = c("Nyeri Town","Tetu","Kieni","Mathira","Othaya","Mukurweini"),
  "Kilifi"           = c("Kilifi North","Kilifi South","Kaloleni","Rabai","Ganze","Malindi","Magarini","Shimo la Tewa"),
  "Kitui"            = c("Kitui Central","Mutomo","Mwingi","Kitui West","Ikutha","Katulani","Nzambani"),
  "Turkana"          = c("Lodwar","Turkana Central","Turkana East","Loima","Turkana South","Kalokol"),
  "Wajir"            = c("Wajir East","Wajir West","Wajir North","Wajir South","Tarbaj","Eldas"),
  "Kisii"            = c("Kisii Central","Bobasi","South Mugirango","Bonchari","Kitutu Chache","Nyaribari Masaba","Masaba North"),
  "Marsabit"         = c("Marsabit Central","Moyale","Laisamis","North Horr","Saku"),
  "Isiolo"           = c("Isiolo North","Isiolo South","Garba Tulla","Merti"),
  "Samburu"          = c("Samburu North","Samburu Central","Samburu East"),
  "Trans Nzoia"      = c("Kiminini","Cherangany","Kwanza","Saboti","Endebess"),
  "West Pokot"       = c("Kapenguria","Sigor","Kacheliba","Pokot South","Pokot North"),
  "Elgeyo-Marakwet"  = c("Marakwet East","Marakwet West","Keiyo North","Keiyo South"),
  "Baringo"          = c("Baringo Central","Baringo North","Baringo South","Mogotio","Eldama Ravine","Tiaty"),
  "Laikipia"         = c("Laikipia West","Laikipia East","Laikipia North","Ol Jorok"),
  "Nyandarua"        = c("Ol Kalou","Kinangop","Kipipiri","Ndaragwa","Mirangine"),
  "Kirinyaga"        = c("Mwea","Gichugu","Ndia","Kirinyaga Central","Mwea East"),
  "Murang'a"         = c("Kangema","Mathioya","Kigumo","Maragwa","Kandara","Gatanga","Kahuro"),
  "Kiambu"           = c("Thika","Ruiru","Githunguri","Gatundu","Juja","Limuru","Kabete","Kikuyu"),
  "Nandi"            = c("Aldai","Nandi Hills","Emgwen","Mosop","Chesumei","Tinderet"),
  "Kericho"          = c("Ainamoi","Belgut","Kipkelion East","Kipkelion West","Soin/Sigowet","Bureti"),
  "Bomet"            = c("Bomet Central","Bomet East","Chepalungu","Konoin","Sotik"),
  "Narok"            = c("Narok North","Narok South","Narok East","Narok West","Emurua Dikirr","Kilgoris"),
  "Kajiado"          = c("Kajiado Central","Kajiado North","Kajiado East","Kajiado West","Loitoktok"),
  "Machakos"         = c("Machakos Town","Mavoko","Masinga","Yatta","Kangundo","Matungulu","Kathiani","Mwala"),
  "Makueni"          = c("Makueni","Kibwezi","Kilome","Kaiti","Mbooni","Nzaui"),
  "Tharaka-Nithi"    = c("Tharaka North","Tharaka South","Chuka","Igambang'ombe","Maara"),
  "Embu"             = c("Manyatta","Runyenjes","Mbeere South","Mbeere North"),
  "Taita Taveta"     = c("Taveta","Wundanyi","Mwatate","Voi"),
  "Kwale"            = c("Msambweni","Lunga Lunga","Matuga","Kinango"),
  "Tana River"       = c("Garsen","Galole","Bura"),
  "Lamu"             = c("Lamu East","Lamu West"),
  "Homa Bay"         = c("Kasipul","Kabondo Kasipul","Karachuonyo","Rangwe","Homa Bay Town","Ndhiwa","Suba North","Suba South"),
  "Migori"           = c("Rongo","Awendo","Suna East","Suna West","Uriri","Nyatike","Kuria West","Kuria East"),
  "Nyamira"          = c("Borabu","Masaba North","Nyamira North","Nyamira South","Kitutu Masaba"),
  "Siaya"            = c("Gem","Rarieda","Alego-Usonga","Ugenya","Ugunja","Bondo"),
  "Busia"            = c("Teso North","Teso South","Nambale","Matayos","Butula","Funyula","Bunyala"),
  "Vihiga"           = c("Luanda","Vihiga","Emuhaya","Hamisi","Sabatia"),
  "Bungoma"          = c("Webuye East","Webuye West","Kimilili","Tongaren","Sirisia","Mt Elgon","Kanduyi","Bumula","Kabuchai")
)

# ── SEED DATA — all 47 counties ──────────────────────────────────
set.seed(42)

counties <- data.frame(
  name  = c(
    "Nairobi","Mombasa","Kisumu","Nakuru","Uasin Gishu",
    "Garissa","Mandera","Kakamega","Meru","Nyeri",
    "Kilifi","Kitui","Turkana","Wajir","Kisii",
    "Marsabit","Isiolo","Samburu","Trans Nzoia","West Pokot",
    "Elgeyo-Marakwet","Baringo","Laikipia","Nyandarua","Kirinyaga",
    "Murang'a","Kiambu","Nandi","Kericho","Bomet",
    "Narok","Kajiado","Machakos","Makueni","Tharaka-Nithi",
    "Embu","Taita Taveta","Kwale","Tana River","Lamu",
    "Homa Bay","Migori","Nyamira","Siaya","Busia",
    "Vihiga","Bungoma"),
  lat   = c(
    -1.286,-4.043,-0.102,-0.303, 0.520,
    -0.454, 3.937, 0.281, 0.047,-0.417,
    -3.629,-1.366, 3.119, 1.747,-0.682,
    2.337, 0.354, 1.061, 1.014, 1.250,
    0.783, 0.617,-0.366,-0.411,-0.659,
    -0.717,-1.031, 0.185,-0.370,-0.798,
    -1.082,-1.852,-1.516,-2.234,-0.301,
    -0.530,-3.383,-4.174, 0.614,-2.273,
    -0.524,-1.063,-0.664, 0.062, 0.461,
    0.062, 0.582),
  lng   = c(
    36.817,39.668,34.762,36.080,35.270,
    39.646,41.855,34.752,37.649,36.950,
    39.852,38.012,35.597,40.058,34.768,
    37.994,37.582,37.072,34.955,35.116,
    35.507,36.026,36.809,36.598,37.288,
    37.047,36.862,35.187,35.286,35.233,
    35.872,36.776,37.257,37.636,37.687,
    37.460,38.358,39.084,40.012,40.912,
    34.459,34.481,34.927,34.289,34.137,
    34.716,34.559),
  count = c(
    187, 78, 95, 62, 54,
    43, 38, 51, 29, 22,
    35, 18, 15, 24, 41,
    12, 10,  8, 19, 11,
    14, 16, 13, 17, 20,
    23, 45, 18, 21, 16,
    25, 28, 32, 14, 11,
    15,  9, 12,  7,  5,
    26, 22, 13, 20, 17,
    12, 24),
  stringsAsFactors=FALSE)

# ── KENYA COUNTIES SF — read directly from shapefile ────────────────
# Place the shapefile folder at:  data/kenyan-counties/County.shp
# (relative to app.R).  All 47 real IEBC county boundaries are used.
.shp_path <- file.path("data", "kenyan-counties", "County.shp")
if (!file.exists(.shp_path))
  stop("Shapefile not found at ", .shp_path,
       ". Place County.shp/.dbf/.shx/.prj in data/kenyan-counties/")

KENYA_SF <- st_read(.shp_path, quiet=TRUE)

# Rename COUNTY → name for consistent joining downstream
names(KENYA_SF)[names(KENYA_SF) == "COUNTY"] <- "name"

# Remap shapefile county names → app county names
KENYA_SF$name[KENYA_SF$name == "Keiyo-Marakwet"] <- "Elgeyo-Marakwet"
KENYA_SF$name[KENYA_SF$name == "Tharaka"]         <- "Tharaka-Nithi"
KENYA_SF$name[KENYA_SF$name == "Murang'A"]        <- "Murang'a"

tmap_mode("view")  # set once at startup, not inside reactive



tweet_texts <- c(
  "Watu wa kabila hiyo hawana haki ya kuishi hapa Kenya yetu",
  "These politicians are corrupt and should face consequences",
  "Kesho tutawaonyesha nguvu yetu, waambie wajiandae!",
  "Let us unite as Kenyans and build this nation together with love",
  "Waende kwao, hawataki amani ya nchi hii",
  "Vote wisely this election, check the record of your candidate",
  "Those people from that region should never be trusted with public office",
  "Najivunia kuwa Mkenya, tunaendelea pamoja",
  "Kabila la wezi hawastahili nafasi katika serikali",
  "Prayer for peace and unity in our beautiful country Kenya",
  "Tukiungana tutashinda, msiyunje ili kupata maendeleo",
  "Heshima kwa wote, hakuna kabila bora kuliko lingine",
  "These migrants should go back to where they came from",
  "Religious extremists are ruining our peaceful coexistence",
  "Sisi sote ni ndugu, tunaomba amani na upendo Kenya nzima"
)

target_groups <- c(
  "Kikuyu community","Coast residents","Luo community","Kalenjin voters",
  "Somali residents","Northern Kenya pastoralists","Western Kenya farmers",
  "Opposition leaders","Government officials","Religious minorities",
  "Migrant workers","Women politicians","Youth groups","Business community","Civil society"
)

platforms  <- c("Twitter/X","Facebook","TikTok","Telegram","WhatsApp")
languages  <- c("Swahili","English","Sheng","Kikuyu","Luo","Kalenjin")
categories <- c("Ethnicity","Religion","Gender","Political","Regional","Incitement")
handles    <- c("@user_ke","@nairobi_talk","@kisumu_voice","@mombasa_news","@kenyatalk")

seed_ncic_levels  <- c("5","4","0","0","2","3","4","5")
seed_officers     <- c("Officer Mwangi","Officer Njeri","Officer Otieno",NA,NA,NA)

signal_presets <- c(
  "Extremist keyword|Us-vs-them framing|Network exposure",
  "Ethnic slur detected|Incitement language|Historical pattern match",
  "No hate signals detected",
  "Religious intolerance marker|Political targeting",
  "Dehumanising language|Coordinated posting|PEV-era pattern"
)

build_cases <- function() {
  do.call(rbind, lapply(seq_len(nrow(counties)), function(i) {
    n          <- counties$count[i]
    idx        <- seq(sum(counties$count[seq_len(i-1)])+1,
                      sum(counties$count[seq_len(i)]))
    target_idx <- sample(seq_len(nrow(counties))[-i], n, replace=TRUE)
    tweets     <- sample(tweet_texts, n, replace=TRUE)
    ncic_lvls  <- sample(seed_ncic_levels, n, replace=TRUE)
    confs      <- sample(62:99, n, replace=TRUE)
    net_sc     <- sample(10:50, n, replace=TRUE)
    freq_sp    <- sample(5:30,  n, replace=TRUE)
    secs_ago   <- sample(0:(30*86400), n, replace=TRUE)
    validated  <- sample(seed_officers, n, replace=TRUE)
    val_ts     <- ifelse(!is.na(validated),
                         format(Sys.time() - secs_ago/2, "%Y-%m-%d %H:%M"), NA_character_)
    
    risks <- mapply(compute_risk,
                    tweet=tweets, gpt_conf=confs, ncic_level=ncic_lvls,
                    MoreArgs=list(kw_weights=kw_weights_global,
                                  network_score=20, freq_spike=10, ctx_score=0),
                    SIMPLIFY=FALSE)
    
    data.frame(
      case_id          = paste0("KE-", formatC(idx, width=5, flag="0")),
      county           = counties$name[i],
      sub_location     = sample(SUB_LOCATIONS[[counties$name[i]]] %||%
                                  c("Central","East","West","North","South"), n, replace=TRUE),
      src_lat          = counties$lat[i],
      src_lng          = counties$lng[i],
      target_county    = counties$name[target_idx],
      tgt_lat          = counties$lat[target_idx],
      tgt_lng          = counties$lng[target_idx],
      target_group     = sample(target_groups, n, replace=TRUE),
      platform         = sample(platforms, n, replace=TRUE),
      handle           = sample(handles, n, replace=TRUE),
      tweet_text       = tweets,
      language         = sample(languages, n, replace=TRUE),
      ncic_level       = as.integer(ncic_lvls),
      ncic_label       = sapply(ncic_lvls, ncic_name),
      section_13       = sapply(ncic_lvls, ncic_s13),
      confidence       = paste0(confs,"%"),
      conf_num         = confs,
      conf_band        = sapply(confs, conf_band),
      category         = sample(categories, n, replace=TRUE),
      validated_by     = validated,
      validated_at     = val_ts,
      action_taken     = ifelse(!is.na(validated),
                                sample(c("CONFIRMED","DOWNGRADED","CLEARED"), n, replace=TRUE),
                                NA_character_),
      officer_ncic_override = NA_integer_,
      risk_score       = sapply(risks, `[[`, "score"),
      risk_formula     = sapply(risks, `[[`, "formula"),
      kw_score         = sapply(risks, `[[`, "kw_score"),
      network_score    = net_sc,
      signals          = sample(signal_presets, n, replace=TRUE),
      trend_data       = sapply(seq_len(n), function(j) {
        base <- sample(10:80,1)
        paste(pmin(100,pmax(0, base+cumsum(sample(-8:10,7,replace=TRUE)))),
              collapse=",")
      }),
      timestamp        = as.POSIXct(Sys.time()-secs_ago,
                                    origin="1970-01-01", tz="Africa/Nairobi"),
      stringsAsFactors = FALSE
    )
  }))
}


# ── LOAD OR SEED CASES FROM DB ───────────────────────────────────
# On first run: DB is empty → seed with generated cases and persist.
# On subsequent runs: load directly from DB — all validations intact.
{
  db_cases <- db_load_cases()
  if (is.null(db_cases) || nrow(db_cases) == 0) {
    message("[db] No cases found — seeding with generated data...")
    all_cases <- build_cases()
    all_cases$risk_level    <- with(all_cases,
                                    ifelse(risk_score>=65,"HIGH",
                                           ifelse(risk_score>=35,"MEDIUM","LOW")))
    all_cases$timestamp_chr <- format(all_cases$timestamp, "%Y-%m-%d %H:%M")
    all_cases$notes         <- ""
    db_save_cases(all_cases)
    message(sprintf("[db] Seeded %d cases", nrow(all_cases)))
  } else {
    all_cases <- db_cases
    message(sprintf("[db] Loaded %d cases from DB", nrow(all_cases)))
  }
}

DATE_MIN <- as.Date(min(all_cases$timestamp))
DATE_MAX <- as.Date(max(all_cases$timestamp))

# ── GPT CLASSIFY  ────────────────────────────────────────────────
NCIC_SYSTEM_PROMPT <- paste0(
  "You are an AI hate speech classifier operating under the National Cohesion and ",
  "Integration Act, Cap 170 of 2008 (Kenya). Your role is to assist trained NCIC officers — ",
  "NOT to make final legal determinations. All outputs require human review.\n\n",
  
  "═══ VIOLENCE OVERRIDE — APPLY FIRST ═══\n",
  "If the post contains ANY of the following, assign L4 or L5 REGARDLESS of target:\n",
  "  - Explicit or implicit call to physically harm, kill, attack, or injure ANY person\n",
  "  - Incitement to mob action, lynching, burning, or physical removal by force\n",
  "  - Threats against personal safety of any individual or group\n",
  "EXAMPLES THAT ARE ALWAYS L4/L5 regardless of target:\n",
  "  'Kill the MP' → L5 (explicit violence against individual)\n",
  "  'Burn down their houses' → L5 (incitement to violence)\n",
  "  'We should attack them at the rally' → L4/L5 (physical threat)\n",
  "  'Mpige risasi' → L5 (shoot them — violence regardless of target)\n",
  "The ACTIVISM test NEVER applies when violence, harm, or physical threat is present.\n\n",
  
  "═══ ACTIVISM TEST — apply ONLY when violence is absent ═══\n",
  "Activism is defined as: non-violent speech demanding accountability, reform, or political change.\n",
  "Apply BOTH tests:\n\n",
  "  TARGET TEST: Who is the target?\n",
  "    - PUBLIC OFFICIAL/INSTITUTION (MP, governor, government, police, judiciary) → may be L0/L1\n",
  "    - PROTECTED GROUP (ethnic, religious, regional community) → may be L2–L5\n",
  "    - AMBIGUOUS (could be either) → examine intent carefully\n\n",
  "  INTENT TEST: What is the speaker trying to achieve?\n",
  "    - Accountability/reform ('send home', 'vote out', 'resign', 'prosecute', 'audit') → L0/L1\n",
  "    - Exclusion/expulsion of a group ('waende', 'go back', 'they don't belong here') → L3/L4\n",
  "    - Dehumanisation ('they are animals/vermin/disease') → L3 minimum\n",
  "    - Mobilisation to act against a group ('tutawaonyesha nguvu') → L4\n\n",
  "  Set is_activism=true ONLY when: violence is absent AND target is official/institution AND intent is accountability.\n\n",
  
  "EXAMPLES:\n",
  "  'MPs are gluttons who should be sent home packing' → L0, is_activism=true (officials, accountability, no violence)\n",
  "  'Kill the MP' → L5, is_activism=false (violence override — never activism)\n",
  "  'Hang all corrupt politicians' → L4/L5, is_activism=false (violence override)\n",
  "  'Politicians from [tribe] are thieves' → L2/L3, is_activism=false (ethnic group targeted)\n",
  "  'We must fight corruption' → L0, is_activism=true (institutional, accountability framing)\n",
  "  'People from [region] go back to your home' → L4, is_activism=false (expulsion rhetoric)\n",
  "  'Tutawaonyesha nguvu yetu [ethnic implied]' → L4, is_activism=false (ethnic mobilisation)\n",
  "  'Vote out the government, they have failed us' → L0, is_activism=true (civic, non-violent)\n\n",
  
  "CLASSIFICATION FRAMEWORK — assign exactly one NCIC level:\n\n",
  "LEVEL 0 — NEUTRAL DISCUSSION\n",
  "Normal discourse, political opinion, criticism of public figures or institutions. ",
  "Non-violent accountability language directed at officials or government. SAFE.\n\n",
  "LEVEL 1 — OFFENSIVE LANGUAGE\n",
  "Rude or insulting language not directed at a protected group and without violence. ",
  "Harsh political rhetoric without ethnic/religious targeting. Monitor only.\n\n",
  "LEVEL 2 — PREJUDICE / STEREOTYPING\n",
  "Demeaning generalisations about an ethnic/religious/regional group presented as fact. ",
  "The GROUP is the target, not individual officials. Below S13 threshold.\n\n",
  "LEVEL 3 — DEHUMANIZATION\n",
  "Strips human dignity from a group: animal comparisons, disease metaphors, vermin. ",
  "Echoes 2007/08 PEV rhetoric ('madoadoa', 'mavi ya kuku'). S13 threshold zone.\n\n",
  "LEVEL 4 — HATE SPEECH\n",
  "Direct/indirect call to action AGAINST a protected group, OR physical threat against any person. ",
  "Expulsion rhetoric, ethnic mobilisation, implicit violence threats. S13 crossed.\n\n",
  "LEVEL 5 — TOXIC\n",
  "Explicit calls for physical violence, killing, or physical harm — against ANY target. ",
  "Immediate escalation. S13 charges. No activism exemption.\n\n",
  
  "CONTEXTUAL DE-ESCALATION SIGNALS (lower level if violence is absent):\n",
  "- target_is_official: named politician, MP, governor, public institution\n",
  "- accountability_frame: 'vote out', 'resign', 'prosecute', 'audit', 'send home'\n",
  "- no_group_identifier: no ethnic/religious/regional group named or clearly implied\n",
  "- civic_mobilisation: voter registration, peaceful protest, court action\n",
  "- no_violence_language: no harm, kill, attack, destroy, burn language\n\n",
  
  "CONTEXTUAL ESCALATION SIGNALS:\n",
  "- violence_language: kill, attack, burn, harm, destroy, piga, chinja, angamiza\n",
  "- ethnic_identifier: specific community named or strongly implied\n",
  "- expulsion_language: 'waende', 'go back', 'remove them from here'\n",
  "- dehumanising_metaphor: animal, disease, dirt, vermin comparisons\n",
  "- pev_pattern: echoes 2007/08 post-election violence rhetoric\n",
  "- coordinated_posting: same message across multiple accounts\n",
  "- electoral_period: within 90 days of election\n",
  "- hotspot_county: Nakuru, Kisumu, Mombasa, Uasin Gishu, Nairobi\n",
  "- coded_language: Sheng or coded hate speech\n\n",
  
  "Respond ONLY with valid JSON. No markdown. No explanation outside JSON."
)

classify_tweet <- function(tweet, kw_weights=kw_weights_global,
                           handle="@unknown", county="Unknown",
                           cases_df=NULL) {
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (nchar(api_key)==0) stop("OPENAI_API_KEY not set in secrets/.Renviron")
  
  # Compute source context BEFORE cache check so it updates even for cached posts
  src_ctx <- if (!is.null(cases_df) && nrow(cases_df) > 0)
    compute_source_context(handle, county, cases_df)
  else
    list(score=0, handle_score=0, county_score=0, summary="")
  
  # cache key includes example bank size so it invalidates as bank grows
  ex_count <- length(load_examples())
  key      <- digest(paste0(tolower(trimws(tweet)), "|ex:", ex_count))
  
  if (exists(key, envir=classify_cache)) {
    result <- get(key, envir=classify_cache)
    # Re-run risk formula with fresh source context even on cache hit
    ctx_sc <- length(result$contextual_factors %||% list()) * 8
    rs <- compute_risk(tweet, result$confidence %||% 50,
                       result$ncic_level %||% 0, kw_weights,
                       result$network_score %||% 20, 10, ctx_sc,
                       src_ctx$score)
    result$risk_score         <- rs$score
    result$risk_formula       <- rs$formula
    result$source_history_score <- src_ctx$score
    result$source_context_note  <- src_ctx$summary
    return(result)
  }
  
  few_shot <- build_few_shot()
  
  user_prompt <- paste0(
    few_shot,
    'Classify this post:\n"', gsub('"',"'",tweet), '"\n\n',
    'Apply VIOLENCE OVERRIDE first, then ACTIVISM TEST if no violence.\n\n',
    'Return exactly:\n',
    '{"ncic_level":<0-5>,\n',
    ' "label":"Neutral Discussion"|"Offensive Language"|"Prejudice / Stereotyping"|',
    '"Dehumanization"|"Hate Speech"|"Toxic",\n',
    ' "confidence":<0-100>,\n',
    ' "category":"Ethnicity"|"Religion"|"Gender"|"Political"|"Regional"|"Incitement"|"None",\n',
    ' "is_activism":<true|false>,\n',
    ' "activism_reasoning":"<cite: violence present/absent, target type, intent>",\n',
    ' "target_type":"protected_group"|"public_official"|"institution"|"none",\n',
    ' "reasoning":"<one sentence citing NCIC level and key contextual factors>",\n',
    ' "legal_basis":"<Cap 170 section or None>",\n',
    ' "section_13":<true|false>,\n',
    ' "escalation_required":<true|false>,\n',
    ' "action":"<NCIC-aligned recommendation>",\n',
    ' "contextual_factors":["<factor>"],\n',
    ' "deescalation_factors":["<factor>"],\n',
    ' "signals":["<signal1>","<signal2>"],\n',
    ' "source_region":"<region or Unknown>",\n',
    ' "target_group":"<group or None>",\n',
    ' "keyword_score":<0-100>,\n',
    ' "network_score":<0-100>}'
  )
  
  resp <- tryCatch(
    request("https://api.openai.com/v1/chat/completions") |>
      req_headers("Content-Type"="application/json",
                  "Authorization"=paste("Bearer",api_key)) |>
      req_body_json(list(
        model=OPENAI_MODEL, max_tokens=500, temperature=0,
        messages=list(
          list(role="system", content=NCIC_SYSTEM_PROMPT),
          list(role="user",   content=user_prompt)
        ))) |>
      req_error(is_error=\(r) FALSE) |>
      req_perform() |>
      resp_body_json(),
    error=function(e) list(error=list(message=conditionMessage(e)))
  )
  
  if (!is.null(resp$error)) stop(resp$error$message)
  raw    <- gsub("```json|```|\\n","",trimws(resp$choices[[1]]$message$content))
  result <- tryCatch(fromJSON(raw),
                     error=function(e) stop("JSON parse: ",e$message))
  
  # apply risk formula on main thread with source context
  ctx_sc <- length(result$contextual_factors %||% list()) * 8
  rs <- compute_risk(tweet,
                     result$confidence    %||% 50,
                     result$ncic_level    %||% 0,
                     kw_weights,
                     result$network_score %||% 20,
                     10, ctx_sc,
                     src_ctx$score)
  result$risk_score           <- rs$score
  result$risk_formula         <- rs$formula
  result$ctx_score            <- ctx_sc
  result$source_history_score <- src_ctx$score
  result$source_context_note  <- src_ctx$summary
  
  # cache write — in single-tweet mode we're always on main thread
  assign(key, result, envir=classify_cache)
  save_cache()
  result
}

# ── EMAIL ────────────────────────────────────────────────────────
send_alert_email <- function(officer, case_id, ncic_level, ncic_lbl,
                             county, platform, handle, category,
                             language, tweet_text, timestamp,
                             decision, note, target_group="",
                             risk_score=NA, risk_formula="",
                             signals="", legal_basis="",
                             section_13=FALSE) {
  gu <- Sys.getenv("GMAIL_USER"); gp <- Sys.getenv("GMAIL_PASS")
  oe <- Sys.getenv("OFFICER_EMAIL")
  if (any(nchar(c(gu,gp,oe))==0)) stop("Missing email credentials")
  
  ncol  <- ncic_color(ncic_level)
  subj  <- sprintf("[EWS] %s — Case %s — NCIC L%s", decision, case_id, ncic_level)
  
  s13_badge <- if (isTRUE(section_13))
    "<span style='background:#dc3545;color:#fff;border-radius:3px;padding:2px 6px;font-size:11px;'>⚖ Section 13 Threshold</span>"
  else ""
  
  rows_html <- paste0(
    "<tr><td style='padding:6px 10px;font-weight:600;width:130px;border-bottom:1px solid #dee2e6;'>Validated By</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;font-weight:700;'>", officer, "</td></tr>",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Decision</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;font-weight:700;color:",ncol,";'>",decision,"</td></tr>",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>NCIC Level</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",
    "<span style='background:",ncol,";color:#fff;border-radius:4px;padding:2px 8px;font-weight:700;font-size:12px;'>",
    "L",ncic_level," — ",ncic_lbl,"</span> ",s13_badge,"</td></tr>",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Risk Score</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",
    if(!is.na(risk_score)) paste0("<strong>",risk_score,"/ 100</strong>") else "—","</td></tr>",
    if(nchar(trimws(risk_formula))>0)
      paste0("<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Formula</td>",
             "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;font-family:monospace;font-size:10px;'>",
             risk_formula,"</td></tr>") else "",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>County</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",county,"</td></tr>",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Platform</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",platform,"</td></tr>",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Handle</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",handle,"</td></tr>",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Category</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",category,"</td></tr>",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Language</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",language,"</td></tr>",
    if(nchar(trimws(target_group))>0)
      paste0("<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Target Group</td>",
             "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",target_group,"</td></tr>") else "",
    "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Timestamp</td>",
    "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",timestamp,"</td></tr>"
  )
  
  ncic_rec <- paste0(
    "<div style='background:#fff8e1;border-left:4px solid #ffc107;border-radius:4px;",
    "padding:10px 14px;margin-top:14px;font-size:12px;'>",
    "<strong>NCIC Recommended Action (Level ",ncic_level,"):</strong><br>",
    ncic_action(ncic_level),"<br>",
    if(nchar(trimws(legal_basis))>0) paste0("<em>Legal basis: ",legal_basis,"</em>") else "",
    "</div>"
  )
  
  sig_html <- if(nchar(trimws(signals))>0)
    paste0("<div style='background:#f8f0ff;border-left:4px solid #7c3aed;border-radius:4px;",
           "padding:10px 14px;margin-top:12px;font-size:12px;'>",
           "<strong>Flagging Signals:</strong><br>",
           gsub("\\|","<br>",signals),"</div>") else ""
  
  note_html <- if(nchar(trimws(note))>0)
    paste0("<div style='background:#fff8e1;border-left:4px solid #ffc107;border-radius:4px;",
           "padding:10px 14px;margin-top:12px;font-size:12px;'>",
           "<strong>Officer Note:</strong><br>",note,"</div>") else ""
  
  html <- paste0(
    "<html><body style='font-family:Arial,sans-serif;color:#1a1a2e;'>",
    "<div style='background:",ncol,";padding:14px 20px;border-radius:8px 8px 0 0;'>",
    "<h2 style='color:#fff;margin:0;font-size:16px;'>🇰🇪 ",APP_NAME,"</h2>",
    "<p style='color:rgba(255,255,255,0.85);margin:3px 0 0;font-size:12px;'>",
    APP_SUBTITLE," — Alert Notification</p></div>",
    "<div style='background:#f8f9fa;padding:18px;border:1px solid #dee2e6;",
    "border-top:none;border-radius:0 0 8px 8px;'>",
    "<table style='width:100%;border-collapse:collapse;font-size:12px;'>",
    rows_html,"</table>",
    "<div style='margin-top:14px;'><strong>Content:</strong>",
    "<div style='background:#fff;border-left:4px solid ",ncol,";padding:10px 12px;",
    "border-radius:4px;margin-top:6px;font-size:13px;line-height:1.6;border:1px solid #dee2e6;'>",
    tweet_text,"</div></div>",
    ncic_rec, sig_html, note_html,
    "<div style='margin-top:18px;padding-top:12px;border-top:1px solid #dee2e6;",
    "font-size:10px;color:#868e96;'>Generated ",
    format(Sys.time(),"%Y-%m-%d %H:%M:%S")," EAT — ",APP_NAME," v4</div>",
    "</div></body></html>"
  )
  
  mime <- paste0("From: ",gu,"\r\nTo: ",oe,"\r\nSubject: ",subj,
                 "\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n",html)
  tmp <- tempfile(fileext=".txt")
  writeLines(mime, tmp, useBytes=FALSE)
  on.exit(unlink(tmp))
  cmd <- paste0("curl --url 'smtps://smtp.gmail.com:465' --ssl-reqd ",
                "--mail-from '",gu,"' --mail-rcpt '",oe,"' ",
                "--user '",gu,":",gp,"' --upload-file '",tmp,"' --silent --show-error")
  res <- system(cmd, intern=TRUE, ignore.stderr=FALSE)
  st  <- attr(res,"status") %||% 0
  if (!is.null(st) && st!=0) stop("curl SMTP failed, code:",st)
  invisible(TRUE)
}

# ── THEME ────────────────────────────────────────────────────────
ews_theme <- bs_theme(
  version=5, bg="#f0f4f8", fg="#1a1a2e",
  primary="#0066cc", secondary="#e9ecef",
  success="#198754", danger="#dc3545",
  warning="#fd7e14", info="#0dcaf0",
  base_font=font_google("IBM Plex Sans"),
  code_font=font_google("IBM Plex Mono"),
  heading_font=font_google("IBM Plex Sans")
) |> bs_add_rules("
  body{background:#f0f4f8!important;}
  .navbar{background:#0a1628!important;border-bottom:2px solid #0066cc;}
  .navbar-brand span strong{color:#fff!important;}
  .nav-item .nav-link{color:rgba(255,255,255,0.7)!important;font-size:13px;font-weight:500;}
  .nav-item .nav-link:hover{color:#fff!important;}
  .nav-item .nav-link.active{color:#fff!important;background:#0066cc!important;border-radius:6px;}
  .card .nav-pills .nav-link{color:#374151!important;background:#e9ecef!important;border-radius:5px;font-size:12px;font-weight:600;padding:4px 12px;margin-right:4px;}
  .card .nav-pills .nav-link:hover{color:#0066cc!important;background:#d0e4ff!important;}
  .card .nav-pills .nav-link.active{color:#fff!important;background:#0066cc!important;}
  .card .tab-content{padding:8px 4px 4px;}
  .highcharts-container,.highcharts-root{width:100%!important;}
  .highcharts-figure{margin:0;}
  .sidebar{background:#fff!important;border-right:1px solid #dee2e6!important;box-shadow:2px 0 8px rgba(0,0,0,0.06);}
  .accordion-button{background:#f8f9fa!important;color:#1a1a2e!important;font-size:13px;font-weight:600;border:none!important;box-shadow:none!important;}
  .accordion-button:not(.collapsed){background:#0066cc!important;color:#fff!important;}
  .accordion-body{background:#fff!important;padding:14px;}
  .accordion-item{border:1px solid #dee2e6!important;border-radius:8px!important;margin-bottom:6px;overflow:hidden;}
  .card{background:#fff!important;border:1px solid #dee2e6!important;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,0.06);}
  .card-header{background:#f8f9fa!important;border-bottom:1px solid #dee2e6;font-size:13px;font-weight:600;padding:10px 14px;color:#1a1a2e;}
  .value-box{border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,0.08);}
  .ncic-badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;color:#fff;}
  .ncic-0{background:#198754;} .ncic-1{background:#85b800;} .ncic-2{background:#ffc107;color:#1a1a2e!important;}
  .ncic-3{background:#fd7e14;} .ncic-4{background:#dc3545;} .ncic-5{background:#7b0000;}
  .badge-high{background:rgba(220,53,69,0.12);color:#dc3545;border:1px solid rgba(220,53,69,0.3);padding:3px 8px;border-radius:4px;font-size:11px;font-weight:700;}
  .badge-medium{background:rgba(253,126,20,0.12);color:#fd7e14;border:1px solid rgba(253,126,20,0.3);padding:3px 8px;border-radius:4px;font-size:11px;font-weight:700;}
  .badge-low{background:rgba(25,135,84,0.12);color:#198754;border:1px solid rgba(25,135,84,0.3);padding:3px 8px;border-radius:4px;font-size:11px;font-weight:700;}
  .s13-badge{background:#dc3545;color:#fff;border-radius:3px;padding:1px 6px;font-size:10px;font-weight:700;}
  .chat-container{display:flex;flex-direction:column;gap:8px;max-height:360px;overflow-y:auto;padding:4px 0;margin-bottom:10px;}
  .chat-msg{padding:9px 11px;border-radius:8px;font-size:12px;line-height:1.55;}
  .chat-user{background:#e8f0fe;border-left:3px solid #0066cc;margin-left:8px;}
  .chat-bot{background:#f8f9fa;border-left:3px solid #fd7e14;margin-right:8px;}
  .chat-thinking{color:#6c757d;font-style:italic;font-size:11px;}
  .chat-input-row{display:flex;gap:6px;align-items:flex-end;}
  .chat-textarea{flex:1;background:#f8f9fa!important;border:1px solid #ced4da!important;border-radius:6px;color:#1a1a2e!important;font-size:12px!important;resize:none;padding:8px 10px;}
  .btn-classify{background:#0066cc;color:#fff;border:none;border-radius:6px;padding:8px 14px;font-size:16px;font-weight:700;cursor:pointer;}
  .human-notice{background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:9px 12px;font-size:11px;color:#664d03;margin-top:8px;}
  .error-msg{background:#fff5f5;border:1px solid rgba(220,53,69,0.3);border-radius:6px;padding:8px 10px;color:#dc3545;font-size:11px;margin-top:6px;font-family:'IBM Plex Mono';}
  .auth-box{background:#fff;border:1px solid #dee2e6;border-radius:12px;padding:36px;width:380px;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,0.1);}
  .val-tweet-box{padding:11px 13px;border-radius:6px;font-size:13px;line-height:1.6;margin-bottom:10px;background:#f8f9fa;border:1px solid #e9ecef;}
  .btn-val-confirm{background:#dc3545;color:#fff;border:none;border-radius:5px;padding:5px 11px;font-size:11px;font-weight:600;cursor:pointer;}
  .btn-val-escalate{background:#7b0000;color:#fff;border:none;border-radius:5px;padding:5px 11px;font-size:11px;font-weight:600;cursor:pointer;}
  .btn-val-downgrade{background:#fd7e14;color:#fff;border:none;border-radius:5px;padding:5px 11px;font-size:11px;font-weight:600;cursor:pointer;}
  .btn-val-clear{background:#198754;color:#fff;border:none;border-radius:5px;padding:5px 11px;font-size:11px;font-weight:600;cursor:pointer;}
  .btn-val-email{background:#0066cc;color:#fff;border:none;border-radius:5px;padding:5px 11px;font-size:11px;font-weight:600;cursor:pointer;}
  .email-sent{display:inline-flex;align-items:center;gap:5px;background:rgba(25,135,84,0.1);color:#198754;border:1px solid rgba(25,135,84,0.3);border-radius:4px;padding:3px 8px;font-size:11px;font-weight:600;}
  .stat-mini{background:#f8f9fa;border:1px solid #dee2e6;border-radius:8px;padding:10px;text-align:center;}
  .risk-bar-wrap{background:#e9ecef;border-radius:3px;height:6px;overflow:hidden;margin-top:2px;}
  .risk-bar{height:6px;border-radius:3px;}
  .explain-signal{background:#f8f0ff;border-left:3px solid #7c3aed;border-radius:4px;padding:5px 9px;font-size:11px;margin-bottom:4px;color:#4a1d96;}
  .formula-box{background:#f0f9ff;border:1px solid #bae6fd;border-radius:4px;padding:4px 9px;font-size:10px;font-family:'IBM Plex Mono';color:#0369a1;margin-top:4px;}
  .ncic-action-box{border-radius:4px;padding:7px 10px;margin-bottom:8px;font-size:11px;}
  .conf-band{font-size:10px;font-family:'IBM Plex Mono';padding:2px 5px;border-radius:3px;font-weight:600;}
  .conf-veryhigh{background:#d1fae5;color:#065f46;} .conf-high{background:#dcfce7;color:#166534;}
  .conf-moderate{background:#fef9c3;color:#854d0e;} .conf-low{background:#fee2e2;color:#991b1b;}
  .bulk-progress{background:#e8f4ff;border:1px solid #b3d4ff;border-radius:6px;padding:10px 14px;font-size:12px;color:#0066cc;margin-bottom:10px;}
  .async-badge{display:inline-flex;align-items:center;gap:5px;background:rgba(13,202,240,0.1);color:#0369a1;border:1px solid rgba(13,202,240,0.4);border-radius:4px;padding:3px 8px;font-size:11px;font-weight:600;}
  .trend-bar-wrap{display:inline-flex;align-items:flex-end;gap:1px;height:20px;vertical-align:middle;}
  .trend-bar{width:4px;border-radius:1px;background:#0066cc;display:inline-block;}
  .officer-tag{display:inline-flex;align-items:center;gap:4px;background:#e8f0fe;color:#0066cc;border:1px solid #b3d4ff;border-radius:4px;padding:2px 8px;font-size:11px;font-weight:600;}
  .learning-flash{background:#d1fae5;border:1px solid #6ee7b7;border-radius:6px;padding:6px 10px;font-size:11px;color:#065f46;margin-top:6px;}
  .dataTables_wrapper,table.dataTable{background:#fff!important;color:#1a1a2e!important;}
  .dataTables_wrapper .dataTables_filter input,.dataTables_wrapper .dataTables_length select{background:#f8f9fa!important;color:#1a1a2e!important;border:1px solid #ced4da!important;border-radius:4px;}
  table.dataTable thead th{background:#f8f9fa!important;color:#495057!important;font-family:'IBM Plex Mono',monospace;font-size:10px;text-transform:uppercase;letter-spacing:.08em;border-bottom:2px solid #dee2e6!important;}
  table.dataTable tbody tr{background:#fff!important;} table.dataTable tbody tr:hover{background:#f0f4f8!important;}
  table.dataTable tbody td{border-bottom:1px solid #f0f0f0!important;font-size:12px;}
  .leaflet-popup-content-wrapper{background:#fff!important;color:#1a1a2e!important;border:1px solid #dee2e6!important;border-radius:8px!important;box-shadow:0 6px 24px rgba(0,0,0,0.15)!important;padding:0!important;overflow:hidden!important;}
  .leaflet-popup-content{margin:0!important;width:540px!important;max-width:90vw!important;max-height:72vh!important;overflow-y:auto!important;overflow-x:hidden!important;}
  .leaflet-popup-content::-webkit-scrollbar{width:5px;}
  .leaflet-popup-content::-webkit-scrollbar-track{background:#f0f4f8;}
  .leaflet-popup-content::-webkit-scrollbar-thumb{background:#ced4da;border-radius:3px;}
  .leaflet-popup-tip{background:#fff!important;}
  .county-tooltip{background:#1a1a2e!important;color:#fff!important;border:none!important;border-radius:5px!important;font-family:'IBM Plex Sans',Arial!important;font-size:12px!important;font-weight:600!important;padding:5px 10px!important;box-shadow:0 2px 8px rgba(0,0,0,0.3)!important;}
  .county-tooltip::before{border-top-color:#1a1a2e!important;}
  .live-badge{display:inline-flex;align-items:center;gap:5px;font-size:11px;font-family:'IBM Plex Mono',monospace;color:#90ee90;}
  .live-dot{width:7px;height:7px;border-radius:50%;background:#90ee90;animation:pulse 2s infinite;display:inline-block;}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  @keyframes wave-flag{
    0%,100%{transform:rotate(0deg) skewX(0deg);}
    15%{transform:rotate(6deg) skewX(4deg);}
    30%{transform:rotate(-3deg) skewX(-2deg);}
    45%{transform:rotate(5deg) skewX(3deg);}
    60%{transform:rotate(-2deg) skewX(-1deg);}
    75%{transform:rotate(3deg) skewX(2deg);}
  }
  .form-control,.form-select{background:#f8f9fa!important;border:1px solid #ced4da!important;color:#1a1a2e!important;}
  .form-control:focus,.form-select:focus{border-color:#0066cc!important;box-shadow:0 0 0 3px rgba(0,102,204,0.15)!important;}
  /* Accessibility: visible focus states for keyboard navigation */
  a:focus-visible,button:focus-visible,.btn:focus-visible,
  input:focus-visible,select:focus-visible,textarea:focus-visible{
    outline:3px solid #0066cc!important;outline-offset:2px!important;}
  /* Mobile: stack cards vertically on small screens */
  @media (max-width:768px){
    .bslib-grid{grid-template-columns:1fr!important;}
    .navbar-brand{font-size:13px!important;}
    .kpi-grid{grid-template-columns:repeat(2,1fr)!important;}
    .card{margin-bottom:10px;}
  }
  ::-webkit-scrollbar{width:5px;height:5px;}
  ::-webkit-scrollbar-track{background:#f0f4f8;}
  ::-webkit-scrollbar-thumb{background:#ced4da;border-radius:3px;}
")

# ── HTML HELPERS ─────────────────────────────────────────────────
ncic_badge_html <- function(lvl) {
  lvl <- as.character(lvl)
  as.character(tags$span(class=paste0("ncic-badge ncic-",lvl),
                         paste0("L",lvl," ",ncic_name(lvl))))
}
risk_badge_html <- function(level) {
  cls <- switch(level,"HIGH"="badge-high","MEDIUM"="badge-medium","badge-low")
  as.character(tags$span(class=cls, level))
}
risk_bar_html <- function(score, level) {
  col <- switch(level,HIGH="#dc3545",MEDIUM="#fd7e14",LOW="#198754","#adb5bd")
  as.character(tags$div(class="risk-bar-wrap",
                        tags$div(class="risk-bar",style=paste0("width:",score,"%;background:",col,";"))))
}
sparkline_html <- function(trend_str) {
  vals <- suppressWarnings(as.integer(strsplit(trend_str,",")[[1]]))
  if (length(vals)==0||all(is.na(vals))) return("")
  mn <- min(vals,na.rm=TRUE); mx <- max(vals,na.rm=TRUE); rng <- max(mx-mn,1)
  bars <- sapply(vals,function(v){
    ht <- max(3,round(((v-mn)/rng)*18))
    paste0('<span class="trend-bar" style="height:',ht,'px;"></span>')
  })
  paste0('<span class="trend-bar-wrap">',paste(bars,collapse=""),"</span>")
}
conf_band_html <- function(band) {
  cls <- switch(band,"Very High"="conf-band conf-veryhigh","High"="conf-band conf-high",
                "Moderate"="conf-band conf-moderate","conf-band conf-low")
  as.character(tags$span(class=cls,band))
}
officer_tag_html <- function(name) {
  if (is.na(name)||nchar(name)==0) return("<span style='color:#6c757d;font-size:11px;'>Pending</span>")
  as.character(tags$span(class="officer-tag","👤 ",name))
}

auth_wall_ui <- function() {
  tagList(useShinyjs(),
          tags$div(style="display:flex;align-items:center;justify-content:center;min-height:440px;",
                   tags$div(class="auth-box",
                            tags$div(style="font-size:48px;margin-bottom:10px;","🇰🇪"),
                            tags$h3(APP_NAME,style="font-size:18px;"),
                            tags$p("Officer access only. Enter your name and access code.",
                                   style="color:#6c757d;font-size:12px;margin-bottom:18px;"),
                            textInput("auth_name",NULL,placeholder="Your full name (e.g. Officer Mwangi)",width="100%"),
                            passwordInput("auth_password",NULL,placeholder="Access code",width="100%"),
                            tags$div(style="margin-bottom:10px;"),
                            actionButton("auth_submit","Authenticate",
                                         style="width:100%;background:#0066cc;color:#fff;border:none;font-weight:700;font-size:14px;padding:11px;border-radius:6px;"),
                            tags$div(id="auth_error",style="color:#dc3545;font-size:12px;margin-top:8px;")
                   )
          )
  )
}

date_filter_ui <- function(inputId, label="Date range") {
  dateRangeInput(inputId, label, start=DATE_MIN, end=DATE_MAX,
                 min=DATE_MIN, max=DATE_MAX, format="yyyy-mm-dd", width="100%")
}

apply_date_filter <- function(d, dr) {
  if (is.null(dr)||anyNA(dr)) return(d)
  d[as.Date(d$timestamp)>=dr[1] & as.Date(d$timestamp)<=dr[2], ]
}

# ── UI ───────────────────────────────────────────────────────────
ui <- page_navbar(
  title = tags$span(
    style="display:inline-flex;align-items:center;gap:10px;",
    tags$span(
      style="font-size:22px;flex-shrink:0;display:inline-block;animation:wave-flag 2s ease-in-out infinite;transform-origin:left center;",
      "🇰🇪"
    ),
    tags$span(
      style="display:flex;flex-direction:column;line-height:1;gap:4px;",
      tags$span(
        style="font-size:16px;font-weight:800;letter-spacing:.02em;background:linear-gradient(90deg,#fff 60%,rgba(255,255,255,0.7));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;",
        APP_NAME
      ),
      tags$span(
        style="font-size:9.5px;font-weight:400;letter-spacing:.04em;color:rgba(255,255,255,0.45);text-transform:uppercase;",
        APP_SUBTITLE
      )
    ),
    tags$span(
      class="live-badge ms-2",
      style="font-size:10px;",
      tags$span(class="live-dot"), "LIVE"
    )
  ),
  theme=ews_theme, fillable=TRUE, id="main_nav",
  
  # TAB 1: MAP
  nav_panel(title=tagList(bs_icon("map")," Where are hotspots?"), value="tab_map",
            layout_sidebar(fillable=TRUE,
                           sidebar=sidebar(id="sid_map",width=310,open=TRUE,bg="#fff",
                                           accordion(id="acc_map",open=c("acc_chat","acc_filt"),
                                                     
                                                     accordion_panel(title=tagList(bs_icon("robot")," ML Classifier"),value="acc_chat",
                                                                     tags$p(style="font-size:11px;color:#6c757d;margin-bottom:6px;",
                                                                            "Paste any post — classified under NCIC Cap 170 framework."),
                                                                     uiOutput("chat_history"),
                                                                     tags$div(class="chat-input-row",
                                                                              tags$textarea(id="chat_input",class="form-control chat-textarea",
                                                                                            placeholder="Paste post here…",rows=3),
                                                                              tags$button("→",id="chat_send",class="btn-classify",
                                                                                          onclick="Shiny.setInputValue('chat_send',Math.random())")),
                                                                     tags$div(class="human-notice",tags$strong("⚠ Human validation required")),
                                                                     tags$hr(style="border-color:#dee2e6;margin:8px 0;"),
                                                                     tags$div(style="display:flex;flex-direction:column;gap:3px;",
                                                                              actionButton("ex1","L5 Toxic — explicit violence",   class="btn btn-sm btn-outline-danger w-100",  style="font-size:11px;text-align:left;"),
                                                                              actionButton("ex2","L4 Hate speech — incitement",    class="btn btn-sm btn-outline-warning w-100", style="font-size:11px;text-align:left;"),
                                                                              actionButton("ex3","L3 Dehumanization",              class="btn btn-sm btn-outline-secondary w-100",style="font-size:11px;text-align:left;"),
                                                                              actionButton("ex4","L0 Neutral — safe content",      class="btn btn-sm btn-outline-success w-100", style="font-size:11px;text-align:left;")
                                                                     )
                                                     ),
                                                     
                                                     accordion_panel(title=tagList(bs_icon("funnel")," Filters"),value="acc_filt",
                                                                     selectInput("f_county","County",choices=c("All Counties",counties$name),selected="All Counties",width="100%"),
                                                                     selectInput("f_ncic","NCIC Level",
                                                                                 choices=c("All",setNames(0:5,paste0("L",0:5," — ",NCIC_LEVELS))),
                                                                                 selected="All",width="100%"),
                                                                     selectInput("f_platform","Platform",choices=c("All",platforms),selected="All",width="100%"),
                                                                     sliderInput("f_conf","Min. Confidence (%)",min=0,max=100,value=0,step=5,width="100%"),
                                                                     date_filter_ui("map_dr","Date window"),
                                                                     checkboxInput("show_flow","Show message flow arrows",value=TRUE)
                                                     ),
                                                     
                                                     accordion_panel(title=tagList(bs_icon("bar-chart")," Stats"),value="acc_stats",
                                                                     uiOutput("sidebar_stats")),
                                                     
                                                     accordion_panel(title=tagList(bs_icon("info-circle")," Legend"),value="acc_leg",
                                                                     tags$div(style="font-size:11px;",
                                                                              lapply(rev(as.character(0:5)),function(l)
                                                                                tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:5px;",
                                                                                         tags$div(style=paste0("width:12px;height:12px;border-radius:50%;background:",ncic_color(l),";flex-shrink:0;")),
                                                                                         paste0("L",l," — ",ncic_name(l))))
                                                                     )
                                                     )
                                           )
                           ),
                           card(full_screen=TRUE,
                                card_header(tagList(bs_icon("geo-alt")," Kenya Incident Map · NCIC Level Colouring · click for cases")),
                                leafletOutput("kenya_map",height="620px"))
            )
  ),
  
  # TAB 2: DASHBOARD
  nav_panel(title=tagList(bs_icon("speedometer2")," What are current signals?"),value="tab_dash",padding=16,
            
            # ── Ethical warning banner ───────────────────────────────
            tags$div(
              style="background:#fff8e1;border:1px solid #ffc107;border-radius:8px;padding:9px 14px;margin-bottom:12px;display:flex;align-items:flex-start;gap:10px;",
              tags$span(style="font-size:16px;flex-shrink:0;","⚠️"),
              tags$div(style="font-size:12px;color:#664d03;line-height:1.5;",
                       tags$strong("Interpret with caution. "),
                       "These signals are AI-generated proxies for potential harmful narratives — they are ",
                       tags$strong("not proof"), " of illegal activity. All L4/L5 cases require officer validation before any legal or operational action. Risk scores reflect relative patterns, not absolute severity."
              )
            ),
            
            # ── Intro + How to read (collapsible) ───────────────────
            accordion(
              id = "dash_intro_acc", open = FALSE,
              accordion_panel(
                title = tagList(bs_icon("question-circle"), " What does this dashboard show? How do I use it?"),
                value = "dash_intro",
                layout_columns(col_widths=c(7,5),
                               tags$div(style="font-size:13px;line-height:1.8;color:#374151;",
                                        tags$p(
                                          tags$strong("Radicalisation Signals"), " monitors online content in Kenya for narratives ",
                                          "associated with hate speech, ethnic incitement, and radicalisation. Posts are collected from ",
                                          "social media platforms (Twitter/X, Facebook, TikTok, Telegram, WhatsApp), classified by ",
                                          "GPT-4o-mini under the ", tags$strong("NCIC Cap 170 framework"), ", and validated by trained ",
                                          "NCIC intelligence officers before any action is taken. The dataset covers ",
                                          tags$strong("15 monitored counties"), " over a rolling ", tags$strong("30-day window."),
                                          " Intended users: NCIC officers, researchers, CSOs, and policy analysts."
                                        )
                               ),
                               tags$div(
                                 tags$div(style="font-size:12px;font-weight:700;color:#1a1a2e;margin-bottom:8px;",
                                          tagList(bs_icon("cursor"), " How to read this dashboard")),
                                 tags$ol(style="font-size:12px;color:#374151;line-height:1.8;padding-left:16px;margin:0;",
                                         tags$li("Use the ", tags$strong("date filter"), " (top right) to narrow the time window."),
                                         tags$li("KPI cards show counts for the selected period — hover for definitions."),
                                         tags$li("Click ", tags$strong("NCIC Distribution"), " to see the severity breakdown; ",
                                                 tags$strong("Platform"), " to see which channels dominate."),
                                         tags$li("Click ", tags$strong("Priority Cases"), " to review high-risk posts needing validation."),
                                         tags$li("Typical questions: ", tags$em("Has L4+ activity increased this week? Which county has the highest avg risk? Which platform is most active?"))
                                 )
                               )
                )
              )
            ),
            
            # ── Filters row ─────────────────────────────────────────
            tags$div(style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;margin-top:8px;",
                     tags$div(style="font-size:12px;color:#6c757d;",
                              tagList(bs_icon("funnel"), " Filtering all charts and tables below")),
                     tags$div(style="display:flex;gap:8px;align-items:center;",
                              date_filter_ui("dash_dr"),
                              actionButton("dash_reset", tagList(bs_icon("x-circle"), " Reset"),
                                           class="btn btn-outline-secondary btn-sm",
                                           style="white-space:nowrap;")
                     )
            ),
            uiOutput("kpi_row"),
            layout_columns(col_widths=c(6,6),
                           # Charts card with nav_pills
                           card(
                             card_header(tagList(bs_icon("bar-chart-fill")," Analytics Charts")),
                             navset_pill(
                               id = "dash_chart_pills",
                               nav_panel("NCIC Distribution",
                                         tags$div(style="height:320px;",
                                                  highchartOutput("plot_ncic", height="320px", width="100%"))),
                               nav_panel("Platform Breakdown",
                                         tags$div(style="height:320px;",
                                                  highchartOutput("plot_platform", height="320px", width="100%"))),
                               nav_panel("County Risk Map",
                                         tags$div(style="height:340px;",
                                                  tmapOutput("plot_county_map", height="340px")))
                             ),
                             # JS: trigger resize when pill tab switches so Highcharts/tmap reflow
                             tags$script(HTML('
                  $(document).on("shown.bs.tab", function(e) {
                    setTimeout(function() {
                      window.dispatchEvent(new Event("resize"));
                      if (window.Highcharts) {
                        Highcharts.charts.forEach(function(c) { if(c) c.reflow(); });
                      }
                    }, 100);
                  });
                '))
                           ),
                           # Tables card with nav_pills
                           card(
                             card_header(tagList(bs_icon("table")," Intelligence Tables")),
                             navset_pill(
                               id = "dash_table_pills",
                               nav_panel("Priority Cases",
                                         tags$div(style="padding-top:10px;min-height:200px;",
                                                  DTOutput("dash_priority"))),
                               nav_panel("Officer Activity",
                                         tags$div(style="padding-top:10px;min-height:200px;",
                                                  DTOutput("dash_officers")))
                             ),
                             tags$script(HTML('
                  $(document).on("shown.bs.tab", function(e) {
                    setTimeout(function() {
                      $($.fn.dataTable.tables(true)).DataTable().columns.adjust().draw();
                    }, 150);
                  });
                '))
                           )
            )
  ),
  
  nav_menu(title=tagList(bs_icon("grid-3x3-gap")," Intelligence"), value="menu_intel",
           # TAB 3: RAW SIGNALS
           nav_panel(title=tagList(bs_icon("broadcast")," Raw Signal Feed"),value="tab_signals",padding=16,
                     tags$div(style="background:#f8f9fa;border:1px solid #dee2e6;border-radius:8px;padding:10px 16px;margin-bottom:12px;display:flex;gap:12px;align-items:flex-start;",
                              tags$span(style="font-size:16px;flex-shrink:0;","📡"),
                              tags$div(style="font-size:12px;color:#374151;line-height:1.6;",
                                       tags$strong("Unprocessed signal feed — "), "all ingested posts before AI classification. ",
                                       "Use this view to inspect raw content, verify ingestion, or identify posts needing manual triage. ",
                                       tags$span(style="color:#dc3545;font-weight:600;","Content in this feed has not yet been classified or validated by an officer.")
                              )
                     ),
                     card(card_header(tagList(bs_icon("broadcast")," All Ingested Signals · ", uiOutput("raw_count", inline=TRUE))),
                          DTOutput("raw_table"))),
           
           # TAB 4: ML CLASSIFICATION
           nav_panel(title=tagList(bs_icon("cpu")," AI Classification"),value="tab_classify",padding=16,
                     tags$div(style="background:#f8f9fa;border:1px solid #dee2e6;border-radius:8px;padding:10px 16px;margin-bottom:12px;display:flex;gap:12px;align-items:flex-start;",
                              tags$span(style="font-size:16px;flex-shrink:0;","🤖"),
                              tags$div(style="font-size:12px;color:#374151;line-height:1.6;",
                                       tags$strong("GPT-4o-mini classification under NCIC Cap 170. "),
                                       "Each post is classified through a three-stage chain: Violence Override → Target Test → Intent Test. ",
                                       "Results are composite-scored using AI confidence, keyword weights, network exposure, and source history. ",
                                       tags$span(style="color:#664d03;font-weight:600;","All L4/L5 outputs require officer validation in the Validation & Learning tab before any action.")
                              )
                     ),
                     card(card_header(tagList(bs_icon("arrow-repeat")," Bulk Classification · GPT-4o-mini · NCIC Cap 170 · Async")),
                          tags$div(style="padding:8px 0;",
                                   tags$p(style="font-size:12px;color:#6c757d;margin-bottom:8px;",
                                          "Classifies all unprocessed cases non-blocking. Cache is reused automatically — only new or invalidated posts call the API. Exponential backoff retry on failure."),
                                   uiOutput("bulk_status_ui"),
                                   tags$div(style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;",
                                            actionButton("btn_bulk",tagList(bs_icon("arrow-repeat")," Start Bulk Classification"),class="btn btn-primary btn-sm"),
                                            actionButton("btn_retry_failed",tagList(bs_icon("exclamation-triangle")," Retry Failed"),class="btn btn-warning btn-sm"),
                                            actionButton("btn_clear_cache",tagList(bs_icon("trash")," Clear Cache"),class="btn btn-outline-secondary btn-sm"),
                                            uiOutput("cache_info"),
                                            tags$span(class="async-badge","⚡ Non-blocking")))),
                     # Failure dashboard
                     uiOutput("api_failure_ui"),
                     layout_columns(col_widths=c(3,3,3,3),
                                    value_box("Total",       uiOutput("c_total"),showcase=bs_icon("collection"),          theme="primary"),
                                    value_box("L4+L5",       uiOutput("c_th"),   showcase=bs_icon("exclamation-triangle"),theme="danger"),
                                    value_box("Validated",   uiOutput("c_val"),  showcase=bs_icon("shield-check"),        theme="success"),
                                    value_box("Pending",     uiOutput("c_pend"), showcase=bs_icon("hourglass"),           theme="warning")),
                     card(card_header(tagList(bs_icon("table")," Classified Cases",
                                              tags$div(style="float:right;display:flex;gap:5px;",
                                                       selectInput("clf_ncic",NULL,width="200px",
                                                                   choices=c("All Levels",setNames(paste0("ncic_",0:5),paste0("L",0:5," ",NCIC_LEVELS))),selected="All Levels"),
                                                       selectInput("clf_risk",NULL,width="110px",choices=c("All Risk","HIGH","MEDIUM","LOW"),selected="All Risk"),
                                                       date_filter_ui("clf_dr",NULL)))),
                          DTOutput("clf_table"))
           ),
           
           # TAB 5: MESSAGE FLOW
           nav_panel(title=tagList(bs_icon("arrow-left-right")," Message Flow"),value="tab_flow",padding=16,
                     layout_columns(col_widths=c(3,9),
                                    card(card_header(tagList(bs_icon("funnel")," Filters")),
                                         tags$p(style="font-size:11px;color:#6c757d;margin-bottom:10px;line-height:1.6;",
                                                "Shows directional flow of harmful narratives between origin and target counties. ",
                                                "Arrow colour reflects NCIC level; thickness reflects volume. Click any arrow for case details."),
                                         selectInput("fl_ncic","NCIC Level",
                                                     choices=c("All",setNames(as.character(0:5),paste0("L",0:5," — ",NCIC_LEVELS))),
                                                     selected="5",width="100%"),
                                         selectInput("fl_src","Origin County",choices=c("All",counties$name),selected="All",width="100%"),
                                         selectInput("fl_tgt","Target County",choices=c("All",counties$name),selected="All",width="100%"),
                                         date_filter_ui("fl_dr"),
                                         actionButton("fl_reset",tagList(bs_icon("x-circle")," Reset Filters"),
                                                      class="btn btn-outline-secondary btn-sm w-100",style="margin-top:8px;"),
                                         tags$hr(style="border-color:#dee2e6;margin:10px 0;"),
                                         tags$div(style="font-size:11px;color:#6c757d;line-height:1.7;",
                                                  tags$div(style="font-weight:600;margin-bottom:4px;","Legend"),
                                                  lapply(rev(as.character(0:5)), function(l)
                                                    tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:3px;",
                                                             tags$div(style=paste0("width:10px;height:10px;border-radius:50%;background:",ncic_color(l),";flex-shrink:0;")),
                                                             paste0("L",l," — ",ncic_name(l))))
                                         )
                                    ),
                                    card(full_screen=TRUE,
                                         card_header(tagList(bs_icon("arrow-left-right")," Narrative Flow: Origin County → Target County")),
                                         leafletOutput("flow_map",height="600px"))
                     )
           ),
           
           # TAB 6: NETWORK ANALYSIS
           nav_panel(title=tagList(bs_icon("diagram-3")," Who are the actors?"),value="tab_net",padding=16,
                     tags$div(style="background:#fff8e1;border:1px solid #ffc107;border-left:4px solid #fd7e14;border-radius:8px;padding:10px 16px;margin-bottom:14px;display:flex;gap:12px;align-items:flex-start;",
                              tags$span(style="font-size:16px;flex-shrink:0;","⚠️"),
                              tags$div(style="font-size:12px;color:#664d03;line-height:1.6;",
                                       tags$strong("Caution on handle attribution. "),
                                       "Handles listed here are social media usernames from ingested content — they are proxies, not confirmed identities. ",
                                       "Accounts may be automated bots, impersonators, or compromised accounts. ",
                                       tags$strong("Do not act on handle data alone without independent identity verification.")
                              )
                     ),
                     layout_columns(col_widths=c(6,6),
                                    card(card_header(tagList(bs_icon("person-fill-exclamation")," Flagged Handles · L3+ activity in past 30 days")),
                                         DTOutput("net_handles")),
                                    card(card_header(tagList(bs_icon("share")," Coordinated Inauthentic Behaviour (CIB)")),
                                         tags$p(style="font-size:11px;color:#6c757d;padding:4px 0 6px;",
                                                "Accounts posting identical or near-identical content within short time windows may indicate coordinated campaigns."),
                                         uiOutput("net_cib"))
                     ),
                     card(card_header(tagList(bs_icon("table")," High-Exposure Cases · highest network reach scores")),
                          DTOutput("net_cases"))
           ),
           
           # TAB 7: VALIDATION + LEARNING CENTRE (merged)
           nav_panel(title=tagList(bs_icon("lock")," Validation & Learning"),value="tab_val",padding=16,
                     tags$div(style="background:#f0f9ff;border:1px solid #bae6fd;border-left:4px solid #0066cc;border-radius:8px;padding:10px 16px;margin-bottom:4px;display:flex;gap:12px;align-items:flex-start;",
                              tags$span(style="font-size:16px;flex-shrink:0;","🧑‍⚖️"),
                              tags$div(style="font-size:12px;color:#0c4a6e;line-height:1.6;",
                                       tags$strong("Human-in-the-Loop (HITL) validation workflow. "),
                                       "Officer Validation — review AI classifications, override NCIC levels, add notes, confirm, downgrade, escalate, or clear cases. ",
                                       "Every decision updates keyword weights, grows the few-shot training bank, and re-scores all historical cases automatically. ",
                                       tags$strong("Learning Centre — "), "inspect adapted keyword weights, add/remove keywords, review GPT vs officer disagreements."
                              )
                     ),
                     navset_pill(
                       id = "val_learn_pills",
                       nav_panel(tagList(bs_icon("shield-check")," Officer Validation"),
                                 tags$div(style="padding-top:12px;", uiOutput("val_ui"))),
                       nav_panel(tagList(bs_icon("graph-up")," Learning Centre"),
                                 tags$div(style="padding-top:12px;", uiOutput("learn_ui")))
                     )
           ),
           
           # TAB 8: REPORTS
           nav_panel(title=tagList(bs_icon("file-earmark-bar-graph")," Reports"),value="tab_rep",padding=16,
                     uiOutput("rep_ui")),
           
  ),
  
  # TAB 9: FORECAST
  nav_panel(title=tagList(bs_icon("graph-up-arrow")," Where is risk heading?"),value="tab_forecast",padding=16,
            uiOutput("forecast_ui")),
  
  # TAB 10: DATA, METHODS & ETHICS
  # TAB: DATA, METHODS & ETHICS
  nav_panel(title=tagList(bs_icon("info-circle")," Methodology & Interpretation"),value="tab_about",padding=24,
            tags$div(style="max-width:980px;margin:0 auto;",
                     
                     # ── Header ─────────────────────────────────────────────────
                     tags$div(style="display:flex;align-items:center;gap:20px;margin-bottom:32px;padding-bottom:24px;border-bottom:2px solid #f0f4f8;",
                              tags$div(
                                style="font-size:52px;display:inline-block;animation:wave-flag 2s ease-in-out infinite;transform-origin:left center;flex-shrink:0;",
                                "🇰🇪"),
                              tags$div(
                                tags$h2(APP_NAME, style="font-weight:800;color:#1a1a2e;margin:0 0 4px;font-size:22px;letter-spacing:-.01em;"),
                                tags$p(APP_SUBTITLE, style="color:#6c757d;font-size:13px;margin:0 0 10px;"),
                                tags$div(style="display:flex;gap:8px;flex-wrap:wrap;",
                                         tags$span(style="background:#0066cc;color:#fff;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:600;","NCIC Cap 170"),
                                         tags$span(style="background:#7c3aed;color:#fff;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:600;","GPT-4o-mini"),
                                         tags$span(style="background:#198754;color:#fff;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:600;","Human-in-the-Loop"),
                                         tags$span(style="background:#fd7e14;color:#fff;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:600;","Kenya · 47 Counties")
                                )
                              )
                     ),
                     
                     # ── Critical notice ────────────────────────────────────────
                     tags$div(
                       style=paste0("background:linear-gradient(135deg,#fff8e1,#fff3cd);",
                                    "border:1px solid #ffc107;border-left:4px solid #fd7e14;",
                                    "border-radius:8px;padding:14px 18px;margin-bottom:24px;"),
                       tags$div(style="display:flex;gap:12px;align-items:flex-start;",
                                tags$span(style="font-size:20px;flex-shrink:0;","⚠️"),
                                tags$div(style="font-size:13px;color:#664d03;line-height:1.7;",
                                         tags$strong("AI classifications are analytical aids, not legal findings."),
                                         " Signals generated by this platform represent statistical patterns in language — they indicate content ",
                                         "that warrants human review, not evidence of criminal intent or activity. Every L4/L5 classification ",
                                         "requires independent officer validation and legal review before any action is taken. ",
                                         tags$strong("This platform must not be used as the sole basis for prosecution, surveillance, or enforcement.")
                                )
                       )
                     ),
                     
                     # ── About + Coverage ──────────────────────────────────────
                     layout_columns(col_widths=c(7,5),
                                    card(style="border-top:3px solid #0066cc;height:100%;",
                                         card_header(tagList(bs_icon("bullseye")," About This Platform")),
                                         tags$div(style="font-size:13px;line-height:1.85;color:#374151;",
                                                  tags$p(
                                                    tags$strong("Radicalisation Signals"), " is an AI-assisted intelligence platform supporting the ",
                                                    tags$a("National Cohesion and Integration Commission (NCIC)", href="https://www.cohesion.or.ke", target="_blank",
                                                           style="color:#0066cc;text-decoration:none;font-weight:600;"),
                                                    " of Kenya. It monitors social media for content associated with hate speech, ethnic incitement, ",
                                                    "dehumanisation, and radicalisation — classified under the ", tags$strong("NCIC Cap 170 six-level taxonomy"),
                                                    " using GPT-4o-mini and validated by trained NCIC intelligence officers."
                                                  ),
                                                  tags$p(
                                                    "The platform covers all ", tags$strong("47 Kenya counties"), " using real IEBC administrative boundaries, ",
                                                    "with signal intensity weighted by historical election-period tension, population density, and NCIC operational priorities. ",
                                                    "The analysis window rolls over ", tags$strong("30 days"), ", with all historical scores automatically ",
                                                    "updated whenever keyword weights are refined through officer feedback."
                                                  ),
                                                  tags$p(style="font-size:12px;color:#6c757d;font-style:italic;margin-top:4px;",
                                                         "Intended users: NCIC intelligence officers, researchers, civil society organisations (CSOs), and policy analysts. ",
                                                         "This is not a public-facing tool."
                                                  )
                                         )
                                    ),
                                    card(style="border-top:3px solid #198754;height:100%;",
                                         card_header(tagList(bs_icon("database")," Data & Coverage")),
                                         tags$table(style="width:100%;border-collapse:collapse;font-size:12px;",
                                                    tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                            tags$td(style="color:#6c757d;padding:7px 4px;","Counties"), tags$td(style="font-weight:600;padding:7px 4px;","47 of 47 (all Kenya)")),
                                                    tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                            tags$td(style="color:#6c757d;padding:7px 4px;","Platforms"),tags$td(style="font-weight:600;padding:7px 4px;","Twitter/X · Facebook · TikTok · Telegram · WhatsApp")),
                                                    tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                            tags$td(style="color:#6c757d;padding:7px 4px;","Languages"),tags$td(style="font-weight:600;padding:7px 4px;","Swahili · English · Sheng · Kikuyu · Luo · Kalenjin")),
                                                    tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                            tags$td(style="color:#6c757d;padding:7px 4px;","Analysis window"),tags$td(style="font-weight:600;padding:7px 4px;","30-day rolling (re-scored on weight changes)")),
                                                    tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                            tags$td(style="color:#6c757d;padding:7px 4px;","AI model"),tags$td(style="font-weight:600;padding:7px 4px;","GPT-4o-mini · temperature=0 · 3-attempt retry")),
                                                    tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                            tags$td(style="color:#6c757d;padding:7px 4px;","Legal framework"),tags$td(style="font-weight:600;padding:7px 4px;","NCIC Act Cap 170 · Section 13")),
                                                    tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                            tags$td(style="color:#6c757d;padding:7px 4px;","Persistence"),tags$td(style="font-weight:600;padding:7px 4px;","SQLite · cross-session sync (5s poll)")),
                                                    tags$tr(
                                                      tags$td(style="color:#6c757d;padding:7px 4px;","Unit tests"),tags$td(style="font-weight:600;padding:7px 4px;","24 testthat assertions · compute_risk · source_context"))
                                         )
                                    )
                     ),
                     
                     # ── NCIC Taxonomy ─────────────────────────────────────────
                     tags$div(style="margin:24px 0 8px;",
                              tags$h5(style="font-weight:700;color:#1a1a2e;margin-bottom:4px;",
                                      tagList(bs_icon("layers"), " The NCIC 6-Level Classification Taxonomy")),
                              tags$p(style="font-size:12px;color:#6c757d;margin-bottom:14px;",
                                     "All content is classified on a six-point scale defined by Cap 170. Levels 0–1 are protected speech. ",
                                     "Levels 4–5 cross the Section 13 legal threshold and require immediate officer action.")
                     ),
                     tags$div(style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:24px;",
                              lapply(rev(as.character(0:5)), function(l) {
                                col <- ncic_color(l)
                                tags$div(style=paste0("padding:12px 16px;border-radius:8px;background:",col,"10;",
                                                      "border-left:4px solid ",col,";"),
                                         tags$div(style=paste0("font-weight:800;font-size:13px;color:",col,";margin-bottom:4px;"),
                                                  paste0("L", l, " — ", ncic_name(l))),
                                         tags$div(style="font-size:11px;color:#4b5563;line-height:1.6;", ncic_action(l))
                                )
                              })
                     ),
                     
                     # ── Classification Methodology ─────────────────────────────
                     card(style="border-top:3px solid #7c3aed;margin-bottom:16px;",
                          card_header(tagList(bs_icon("diagram-3")," Classification Methodology")),
                          tags$div(style="padding:4px 0;",
                                   tags$p(style="font-size:12px;color:#6c757d;margin-bottom:16px;",
                                          "Classification applies a three-stage decision chain before assigning any NCIC level. ",
                                          "The chain is designed to protect legitimate political speech while escalating genuine threats."),
                                   layout_columns(col_widths=c(4,4,4),
                                                  tags$div(style="background:#fff5f5;border-radius:8px;padding:14px;border-top:3px solid #dc3545;",
                                                           tags$div(style="font-size:11px;font-weight:800;color:#dc3545;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;","Stage 1 · Violence Override"),
                                                           tags$p(style="font-size:12px;color:#374151;line-height:1.7;margin:0;",
                                                                  "Applied first, unconditionally. Any explicit or implicit call to kill, attack, burn, or physically harm — ",
                                                                  "regardless of target — classifies the post L4 or L5 immediately. The activism test is ", tags$strong("blocked."),
                                                                  " Examples: ", tags$em("'Kill the MP'"), " → L5. ", tags$em("'Mpige risasi'"), " → L5."
                                                           )
                                                  ),
                                                  tags$div(style="background:#fff8f0;border-radius:8px;padding:14px;border-top:3px solid #fd7e14;",
                                                           tags$div(style="font-size:11px;font-weight:800;color:#fd7e14;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;","Stage 2 · Target Test"),
                                                           tags$p(style="font-size:12px;color:#374151;line-height:1.7;margin:0;",
                                                                  "If violence is absent: is the target a ", tags$strong("protected group"),
                                                                  " (ethnic, religious, regional community) or a ",
                                                                  tags$strong("public official/institution"), "? ",
                                                                  "Criticism of officials defaults to L0/L1 — political accountability is constitutionally protected. ",
                                                                  "Targeting a protected group may qualify L2–L5."
                                                           )
                                                  ),
                                                  tags$div(style="background:#f0fff4;border-radius:8px;padding:14px;border-top:3px solid #198754;",
                                                           tags$div(style="font-size:11px;font-weight:800;color:#198754;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;","Stage 3 · Intent Test"),
                                                           tags$p(style="font-size:12px;color:#374151;line-height:1.7;margin:0;",
                                                                  "What outcome does the speaker seek? ",
                                                                  tags$strong("Accountability"), " ('vote out', 'resign', 'audit') → L0/L1. ",
                                                                  tags$strong("Exclusion"), " ('waende', 'go back') → L3/L4. ",
                                                                  tags$strong("Dehumanisation"), " ('they are vermin/disease') → L3+. ",
                                                                  tags$strong("Mobilisation"), " against a group → L4."
                                                           )
                                                  )
                                   ),
                                   tags$div(style="margin-top:16px;background:#f8f9fa;border-radius:8px;padding:14px;",
                                            tags$div(style="font-size:11px;font-weight:800;color:#0066cc;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;","Risk Formula"),
                                            tags$div(style="font-family:'IBM Plex Mono',monospace;font-size:12px;color:#1a1a2e;background:#fff;padding:10px 14px;border-radius:6px;border:1px solid #dee2e6;",
                                                     "Risk = 0.55 × composite + 0.45 × NCIC_base"),
                                            tags$div(style="font-size:11px;color:#6c757d;margin-top:6px;",
                                                     "where composite = 0.30×confidence + 0.22×keyword_weight + 0.13×network_score + 0.08×freq_spike + 0.12×context_score + 0.15×source_history"),
                                            tags$p(style="font-size:12px;color:#6c757d;margin-top:8px;margin-bottom:0;",
                                                   "Source history (capped +30) reflects handle/county posting patterns over 30 days. ",
                                                   "Risk scores are relative within this dataset — not comparable to external threat indices.")
                                   )
                          )
                     ),
                     
                     # ── Safeguards accordion ───────────────────────────────────
                     accordion(id="about_defs_acc", open=FALSE,
                               accordion_panel(
                                 title=tagList(bs_icon("shield-check")," Safeguards Against Misclassification & Hallucination"),
                                 value="safeguards",
                                 layout_columns(col_widths=c(4,4,4),
                                                tags$div(style="font-size:12px;line-height:1.75;color:#374151;padding:4px 8px;",
                                                         tags$div(style="font-size:22px;margin-bottom:8px;","🧑‍⚖️"),
                                                         tags$p(tags$strong("Mandatory Human Review"), style="margin-bottom:6px;"),
                                                         tags$p(style="margin:0;","No legal or enforcement action is triggered by AI alone. Every L2+ case requires an NCIC officer to Confirm, Downgrade, Escalate, or Clear before any action proceeds.")),
                                                tags$div(style="font-size:12px;line-height:1.75;color:#374151;padding:4px 8px;",
                                                         tags$div(style="font-size:22px;margin-bottom:8px;","🗣"),
                                                         tags$p(tags$strong("Activism Protection"), style="margin-bottom:6px;"),
                                                         tags$p(style="margin:0;","Three-stage decision chain explicitly guards against misclassifying political accountability speech as hate speech. Disagreements between AI and officer are logged and used to re-train.")),
                                                tags$div(style="font-size:12px;line-height:1.75;color:#374151;padding:4px 8px;",
                                                         tags$div(style="font-size:22px;margin-bottom:8px;","🔁"),
                                                         tags$p(tags$strong("Continuous Re-calibration"), style="margin-bottom:6px;"),
                                                         tags$p(style="margin:0;","Officer validations update keyword weights in real time. All historical cases are automatically re-scored. Validated examples form a growing few-shot training bank for GPT.")),
                                                tags$div(style="font-size:12px;line-height:1.75;color:#374151;padding:4px 8px;",
                                                         tags$div(style="font-size:22px;margin-bottom:8px;","🗄️"),
                                                         tags$p(tags$strong("Deterministic Classification"), style="margin-bottom:6px;"),
                                                         tags$p(style="margin:0;","temperature=0 ensures the same post always produces the same classification. Responses are cached by content hash — the model cannot return a different result on the same text.")),
                                                tags$div(style="font-size:12px;line-height:1.75;color:#374151;padding:4px 8px;",
                                                         tags$div(style="font-size:22px;margin-bottom:8px;","🌡️"),
                                                         tags$p(tags$strong("Confidence Signalling"), style="margin-bottom:6px;"),
                                                         tags$p(style="margin:0;","Every classification includes a 0–100% confidence score. Outputs below 60% are flagged LOW and surfaced prominently to officers as requiring extra scrutiny.")),
                                                tags$div(style="font-size:12px;line-height:1.75;color:#374151;padding:4px 8px;",
                                                         tags$div(style="font-size:22px;margin-bottom:8px;","🔬"),
                                                         tags$p(tags$strong("Unit-Tested Logic"), style="margin-bottom:6px;"),
                                                         tags$p(style="margin:0;","24 automated tests cover risk scoring, source context, violence override, and edge cases. Run with ", tags$code("testthat::test_dir('tests/testthat')"), " before deploying changes."))
                                 )
                               ),
                               accordion_panel(
                                 title=tagList(bs_icon("exclamation-circle")," Limitations & Known Biases"),
                                 value="limits",
                                 layout_columns(col_widths=c(6,6),
                                                tags$div(style="font-size:12px;line-height:1.8;color:#374151;",
                                                         tags$p(tags$strong("Language coverage: "),
                                                                "Classification accuracy is highest for Swahili and English. Mixed-language posts (Sheng, code-switching) may be under- or mis-classified. Kalenjin and Kikuyu content has the least training signal."),
                                                         tags$p(tags$strong("Identity verification: "),
                                                                "The model cannot verify the real-world identity, bot status, or geographic location of posters. Handles are proxies, not confirmed actors."),
                                                         tags$p(tags$strong("Demonstration data: "),
                                                                "The current dataset is synthetically generated for platform demonstration. Production deployment requires a live ingestion pipeline connected to real social media APIs.")
                                                ),
                                                tags$div(style="font-size:12px;line-height:1.8;color:#374151;",
                                                         tags$p(tags$strong("Model bias: "),
                                                                "GPT-4o-mini may exhibit biases toward over-classifying content from certain communities or political affiliations. Officer oversight and the disagreement log are the primary mitigations."),
                                                         tags$p(tags$strong("Risk score relativity: "),
                                                                "Scores are relative to this dataset and analysis window. They cannot be compared to external threat indices or previous versions of this platform."),
                                                         tags$p(tags$strong("Forecast limitations: "),
                                                                "The 14-day forecast uses a weighted formula, not a fitted time-series model. It is a directional indicator, not a calibrated probabilistic projection.")
                                                )
                                 )
                               ),
                               accordion_panel(
                                 title=tagList(bs_icon("lock")," Data Privacy & Ethics Statement"),
                                 value="privacy",
                                 tags$div(style="font-size:12px;line-height:1.85;color:#374151;",
                                          layout_columns(col_widths=c(6,6),
                                                         tags$div(
                                                           tags$p(tags$strong("Data handled"), ": All content processed by this platform consists of ",
                                                                  tags$strong("publicly available social media posts"), ". Processing is conducted under the ",
                                                                  tags$strong("NCIC Act Cap 170"), " and applicable Kenyan data protection legislation."),
                                                           tags$p(tags$strong("Data stored"), ": Classification results, officer validations, keyword weights, ",
                                                                  "and disagreement logs are stored in a local SQLite database accessible only to authorised NCIC officers. ",
                                                                  "No data is transmitted to third parties beyond OpenAI API calls for classification.")
                                                         ),
                                                         tags$div(
                                                           tags$p(tags$strong("API calls"), ": GPT-4o-mini API calls transmit only post text to OpenAI servers. ",
                                                                  "No officer names, case IDs, personally identifiable information, or metadata are included in API payloads."),
                                                           tags$p(tags$strong("Prohibited use"), ": ",
                                                                  tags$strong(style="color:#dc3545;","Use of AI classification outputs for prosecution, surveillance, targeted enforcement, or profiling of individuals or communities — without independent officer validation and legal review — is strictly prohibited."))
                                                         )
                                          ),
                                          tags$div(style="margin-top:12px;padding:10px 14px;background:#f8f9fa;border-radius:6px;border-left:3px solid #6c757d;font-size:11px;color:#6c757d;",
                                                   "For data corrections, ethical concerns, or feedback: contact the NCIC Intelligence Unit. ",
                                                   paste0("Platform: ", APP_NAME, " v5 · Framework: NCIC Cap 170 · Updated: March 2026 · ",
                                                          "Developed for the IEA Kenya NIRU AI Hackathon"))
                                 )
                               )
                     ),
                     
                     # ── Concept Note Download ─────────────────────────────────
                     tags$div(
                       style="margin-top:24px;padding:16px 20px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:8px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;",
                       tags$div(
                         tags$div(style="font-size:13px;font-weight:700;color:#1a1a2e;margin-bottom:3px;",
                                  tagList(bs_icon("file-earmark-pdf"), " Platform Concept Note")),
                         tags$div(style="font-size:12px;color:#6c757d;",
                                  "Full documentation: methodology, institutional framework, legal basis, safeguards, and value proposition.")
                       ),
                       downloadButton("dl_concept_pdf",
                                      tagList(bs_icon("download"), " Download PDF"),
                                      style="background:#0066cc;color:#fff;border:none;border-radius:6px;font-weight:600;padding:8px 18px;font-size:13px;white-space:nowrap;flex-shrink:0;")
                     ),
                     
                     # ── Footer ────────────────────────────────────────────────
                     tags$div(
                       style="margin-top:32px;padding:20px 0;border-top:1px solid #dee2e6;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;",
                       tags$div(style="font-size:11px;color:#9ca3af;",
                                paste0(APP_NAME, " v5 · GPT-4o-mini + Human-in-the-Loop · NCIC Cap 170 · Kenya")),
                       tags$div(style="display:flex;gap:8px;",
                                tags$span(style="background:#f0f4f8;color:#374151;border-radius:4px;padding:3px 8px;font-size:10px;font-weight:600;","47 Counties"),
                                tags$span(style="background:#f0f4f8;color:#374151;border-radius:4px;padding:3px 8px;font-size:10px;font-weight:600;","24 Unit Tests"),
                                tags$span(style="background:#f0f4f8;color:#374151;border-radius:4px;padding:3px 8px;font-size:10px;font-weight:600;","SQLite Persistence"),
                                tags$span(style="background:#f0f4f8;color:#374151;border-radius:4px;padding:3px 8px;font-size:10px;font-weight:600;","5s Cross-Session Sync")
                       )
                     )
            )
  )
)
# ── SERVER ───────────────────────────────────────────────────────
server <- function(input, output, session) {
  
  rv <- reactiveValues(
    authenticated  = FALSE,
    officer_name   = "",
    chat_history   = list(),
    cases          = all_cases,
    email_status   = list(),
    notes          = list(),
    kw_weights     = kw_weights_global,
    bulk_running   = FALSE,
    bulk_done      = 0,
    bulk_total     = 0,
    bulk_errors    = list(),
    bulk_retries   = 0L,
    cache_size     = length(ls(classify_cache)),
    learning_flash = "",
    last_db_sync   = Sys.time()   # tracks last successful DB read
  )
  
  # ── Cross-session DB sync ──────────────────────────────────────
  # Poll DB every 5 seconds. If another officer validated a case, the DB
  # row will differ from rv$cases. Merge only changed rows to avoid
  # overwriting in-progress bulk operations on this session.
  db_poll_timer <- reactiveTimer(5000)
  
  observe({
    db_poll_timer()
    if (rv$bulk_running) return()   # don't sync mid-bulk
    
    tryCatch({
      fresh <- db_load_cases()
      if (is.null(fresh) || nrow(fresh) == 0) return()
      
      cur <- isolate(rv$cases)
      
      # Find rows that differ in the DB (validated_by, ncic_level, risk_score)
      # Match by case_id; update only columns that officers write
      sync_cols <- c("ncic_level","ncic_label","section_13","validated_by",
                     "validated_at","action_taken","risk_score","risk_formula",
                     "risk_level","notes")
      
      changed <- fresh$case_id[sapply(seq_len(nrow(fresh)), function(i) {
        cid  <- fresh$case_id[i]
        crow <- cur[cur$case_id == cid, sync_cols, drop=FALSE]
        if (nrow(crow) == 0) return(FALSE)
        frow <- fresh[i, sync_cols, drop=FALSE]
        !identical(as.list(crow[1,]), as.list(frow[1,]))
      })]
      
      if (length(changed) > 0) {
        for (cid in changed) {
          fi  <- which(fresh$case_id == cid)
          ci  <- which(cur$case_id   == cid)
          if (length(fi) == 0 || length(ci) == 0) next
          for (col in sync_cols) {
            rv$cases[ci, col] <- fresh[fi, col]
          }
        }
        rv$last_db_sync <- Sys.time()
        message(sprintf("[sync] Updated %d case(s) from DB", length(changed)))
      }
    }, error=function(e) message("[sync] poll error: ", e$message))
  })
  
  # ── shared reactives (fix: not re-filtered per output) ─────
  dash_data <- reactive({
    apply_date_filter(rv$cases, input$dash_dr)
  })
  
  # Pre-aggregated summary — computed once, shared across all KPI/chart outputs
  dash_summary <- reactive({
    d <- dash_data()
    list(
      total  = nrow(d),
      toxic  = sum(d$ncic_level == 5),
      hate   = sum(d$ncic_level == 4),
      s13    = sum(d$section_13, na.rm=TRUE),
      val    = sum(!is.na(d$validated_by)),
      pend   = sum(is.na(d$validated_by) & d$ncic_level >= 2),
      lvl_counts = sapply(0:5, function(l) sum(d$ncic_level == l)),
      plat_counts= sapply(platforms, function(p) sum(d$platform == p)),
      county_risk= d |> group_by(county) |>
        summarise(avg=round(mean(risk_score),0),.groups="drop") |>
        arrange(desc(avg)) |> head(10)
    )
  })
  
  # Reset dashboard filters
  observeEvent(input$dash_reset, {
    updateDateRangeInput(session, "dash_dr",
                         start=DATE_MIN, end=DATE_MAX)
  })
  
  # Forecast reactive — builds sf_fc used by both forecast_ui and forecast_map
  clf_data <- reactive({
    d <- apply_date_filter(rv$cases, input$clf_dr)
    if (!is.null(input$clf_ncic) && input$clf_ncic!="All Levels") {
      lvl <- as.integer(sub("ncic_","",input$clf_ncic))
      d <- d[d$ncic_level==lvl,]
    }
    if (!is.null(input$clf_risk) && input$clf_risk!="All Risk")
      d <- d[d$risk_level==input$clf_risk,]
    d
  })
  
  # ── email outputs: lazy, registered once per cid ───────────
  registered_email_outputs <- new.env(hash=TRUE,parent=emptyenv())
  
  ensure_email_output <- function(cid) {
    if (exists(cid, envir=registered_email_outputs)) return()
    assign(cid, TRUE, envir=registered_email_outputs)
    local({
      cid_l <- cid
      output[[paste0("email_status_",cid_l)]] <- renderUI({
        st <- rv$email_status[[cid_l]]
        if (is.null(st)) return(NULL)
        if (st=="sending") tags$div(style="font-size:11px;color:#6c757d;margin-top:5px;","Sending…")
        else if (st=="sent") tags$div(class="email-sent",style="margin-top:5px;","✓ Alert sent")
        else tags$div(class="error-msg",style="margin-top:5px;",
                      tags$strong("Failed: "), gsub("failed: ","",st))
      })
    })
  }
  
  # ── KPIs ───────────────────────────────────────────────────
  output$kpi_row <- renderUI({
    s     <- dash_summary()
    total <- s$total; toxic <- s$toxic; hate <- s$hate
    s13   <- s$s13;   val   <- s$val;   pend <- s$pend
    
    kpi <- function(label, value, bg, icon_name, pct=NULL) {
      pct_html <- if (!is.null(pct))
        paste0("<div style='font-size:10px;opacity:0.8;margin-top:1px;'>",pct,"% of total</div>")
      else ""
      tags$div(
        style=paste0("background:",bg,";border-radius:8px;padding:10px 14px;color:#fff;",
                     "display:flex;align-items:center;gap:10px;box-shadow:0 2px 6px rgba(0,0,0,0.12);"),
        tags$div(style="font-size:22px;opacity:0.9;line-height:1;", HTML(as.character(bs_icon(icon_name)))),
        tags$div(
          tags$div(style="font-size:11px;font-weight:600;opacity:0.88;letter-spacing:.03em;text-transform:uppercase;", label),
          tags$div(style="font-size:24px;font-weight:800;line-height:1.1;", formatC(value, format="d", big.mark=",")),
          HTML(pct_html)
        )
      )
    }
    
    tags$div(
      class="kpi-grid",
      style="display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:14px;",
      kpi("Total Signals",   total, "#0066cc", "collection"),
      kpi("Toxic / L5",      toxic, "#7b0000", "exclamation-octagon", round(toxic/max(total,1)*100)),
      kpi("Hate Speech / L4",hate,  "#dc3545", "chat-square-text",    round(hate/max(total,1)*100)),
      kpi("Section 13",      s13,   "#c0392b", "file-earmark-text",   round(s13/max(total,1)*100)),
      kpi("Validated",       val,   "#198754", "shield-check",        round(val/max(total,1)*100)),
      kpi("Pending Review",  pend,  "#fd7e14", "hourglass",           round(pend/max(total,1)*100))
    )
  })
  output$kpi_total <- renderUI(nrow(dash_data()))
  output$kpi_toxic <- renderUI(sum(dash_data()$ncic_level==5))
  output$kpi_hate  <- renderUI(sum(dash_data()$ncic_level==4))
  output$kpi_s13   <- renderUI(sum(dash_data()$section_13, na.rm=TRUE))
  output$kpi_val   <- renderUI(sum(!is.na(dash_data()$validated_by)))
  output$kpi_pend  <- renderUI(sum(is.na(dash_data()$validated_by) & dash_data()$ncic_level>=2))
  
  output$c_total <- renderUI(nrow(rv$cases))
  output$c_th    <- renderUI(sum(rv$cases$ncic_level>=4))
  output$c_val   <- renderUI(sum(!is.na(rv$cases$validated_by)))
  output$c_pend  <- renderUI(sum(is.na(rv$cases$validated_by) & rv$cases$ncic_level>=2))
  
  # ── sidebar stats ──────────────────────────────────────────
  output$sidebar_stats <- renderUI({
    d <- apply_date_filter(rv$cases, input$map_dr)
    mk <- function(v,l,c)
      tags$div(class="stat-mini",
               tags$div(style=paste0("font-size:20px;font-weight:700;color:",c,";"),v),
               tags$div(style="font-size:9px;color:#6c757d;font-family:'IBM Plex Mono';",l))
    tags$div(style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;",
             mk(sum(d$ncic_level==5),"L5 TOXIC",   "#4a0000"),
             mk(sum(d$ncic_level==4),"L4 HATE",    "#dc3545"),
             mk(sum(d$ncic_level==3),"L3 DEHUMAN.", "#fd7e14"),
             mk(sum(d$ncic_level==2),"L2 PREJUDICE","#0066cc"),
             mk(sum(d$ncic_level==1),"L1 OFFENSIVE","#6c757d"),
             mk(sum(d$ncic_level==0),"L0 NEUTRAL",  "#198754"))
  })
  
  # ── Dashboard plots ────────────────────────────────────────
  # Force re-render when pill tab switches (fixes shinyapps.io blank chart bug)
  observeEvent(input$dash_chart_pills, {
    shinyjs::runjs('
      setTimeout(function() {
        window.dispatchEvent(new Event("resize"));
        if (window.Highcharts) {
          Highcharts.charts.forEach(function(c) { if(c) c.reflow(); });
        }
      }, 150);
    ')
  })
  
  observeEvent(input$dash_table_pills, {
    shinyjs::runjs('
      setTimeout(function() {
        $($.fn.dataTable.tables(true)).DataTable().columns.adjust().draw();
      }, 150);
    ')
  })
  
  output$plot_ncic <- renderHighchart({
    s     <- dash_summary()
    cnts  <- s$lvl_counts
    total <- s$total
    lbls  <- paste0("L", 0:5, ": ", unname(NCIC_LEVELS))
    cols  <- unname(NCIC_COLORS[as.character(0:5)])
    highchart() |>
      hc_chart(type="column", backgroundColor="#fff",
               style=list(fontFamily="IBM Plex Sans"),
               reflow=TRUE, animation=list(duration=400)) |>
      hc_size(height=300) |>
      hc_xAxis(categories=lbls,
               labels=list(style=list(fontSize="12px", color="#374151")),
               lineColor="#dee2e6", tickColor="#dee2e6") |>
      hc_yAxis(title=list(text="Signal count", style=list(fontSize="12px")),
               gridLineColor="#f0f0f0",
               labels=list(style=list(fontSize="12px", color="#6c757d"),
                           format="{value:,.0f}")) |>
      hc_series(list(
        name="Signals", data=as.list(cnts),
        colorByPoint=TRUE, colors=as.list(cols),
        dataLabels=list(
          enabled=TRUE,
          formatter=JS("function(){ return Highcharts.numberFormat(this.y,0); }"),
          style=list(fontSize="11px", fontWeight="600", textOutline="none")),
        borderRadius=4, borderWidth=0
      )) |>
      hc_tooltip(
        headerFormat="",
        pointFormatter=JS("function(){ var t=this.series.data.reduce(function(a,b){return a+b.y},0); return '<b>'+this.category+'</b><br/>'+Highcharts.numberFormat(this.y,0)+' signals ('+Highcharts.numberFormat(this.y/t*100,1)+'%)'; }"),
        backgroundColor="#1a1a2e", style=list(color="#fff", fontSize="12px"),
        borderWidth=0, shadow=TRUE) |>
      hc_subtitle(text="Signals are proxies, not proof. L4-L5 require officer validation.",
                  style=list(fontSize="10px", color="#9ca3af")) |>
      hc_legend(enabled=FALSE) |>
      hc_credits(enabled=FALSE) |>
      hc_exporting(enabled=FALSE)
  })
  
  output$plot_platform <- renderHighchart({
    s    <- dash_summary()
    cnts <- s$plat_counts
    highchart() |>
      hc_chart(type="column", backgroundColor="#fff",
               style=list(fontFamily="IBM Plex Sans"),
               reflow=TRUE, animation=list(duration=400)) |>
      hc_size(height=300) |>
      hc_xAxis(categories=platforms,
               labels=list(style=list(fontSize="12px", color="#374151")),
               lineColor="#dee2e6", tickColor="#dee2e6") |>
      hc_yAxis(title=list(text="Signal count", style=list(fontSize="12px")),
               gridLineColor="#f0f0f0",
               labels=list(style=list(fontSize="12px", color="#6c757d"),
                           format="{value:,.0f}")) |>
      hc_series(list(
        name="Signals", data=as.list(unname(cnts)),
        colorByPoint=TRUE,
        colors=as.list(OKABE_ITO[seq_along(platforms)]),
        dataLabels=list(
          enabled=TRUE,
          formatter=JS("function(){ return Highcharts.numberFormat(this.y,0); }"),
          style=list(fontSize="11px", fontWeight="600", textOutline="none")),
        borderRadius=4, borderWidth=0
      )) |>
      hc_tooltip(pointFormat="<b>{point.y:,.0f}</b> signals<br/>{point.category}",
                 backgroundColor="#1a1a2e", style=list(color="#fff", fontSize="12px"),
                 borderWidth=0) |>
      hc_subtitle(text="Colour-blind safe palette (Okabe-Ito)",
                  style=list(fontSize="10px", color="#9ca3af")) |>
      hc_legend(enabled=FALSE) |>
      hc_credits(enabled=FALSE) |>
      hc_exporting(enabled=FALSE)
  })
  
  
  # County Risk choropleth map (replaces bar chart)
  output$plot_county_map <- renderTmap({
    d   <- dash_data()
    agg <- d |> group_by(county) |>
      summarise(avg_risk     = round(mean(risk_score), 0),
                n            = n(),
                l4_plus      = sum(ncic_level >= 4),
                s13          = sum(section_13, na.rm=TRUE),
                avg_ncic     = round(mean(ncic_level), 1),
                top_platform = names(sort(table(platform), decreasing=TRUE))[1],
                .groups      = "drop")
    
    # Join aggregated stats onto the sf object (left join keeps all polygons)
    sf_data <- KENYA_SF |>
      left_join(agg, by=c("name"="county")) |>
      mutate(
        # Keep avg_risk as NA for unmonitored counties — tmap renders NA as grey
        n            = ifelse(is.na(n), 0L, n),
        l4_plus      = ifelse(is.na(l4_plus), 0L, l4_plus),
        s13          = ifelse(is.na(s13), 0L, s13),
        avg_ncic     = ifelse(is.na(avg_ncic), NA_real_, avg_ncic),
        top_platform = ifelse(is.na(top_platform), "—", top_platform),
        risk_band    = case_when(
          is.na(avg_risk)  ~ "No data",
          avg_risk >= 65   ~ "HIGH",
          avg_risk >= 35   ~ "MEDIUM",
          TRUE             ~ "LOW"
        ),
        tooltip_text = paste0(
          "<b>", name, "</b><br>",
          "Signals: ", n, "<br>",
          "Avg Risk: ", avg_risk, " / 100<br>",
          "Avg NCIC: L", avg_ncic, "<br>",
          "L4+ Cases: ", l4_plus, "<br>",
          "Section 13: ", s13, "<br>",
          "Top Platform: ", top_platform, "<br>",
          "Risk Band: ", risk_band
        )
      )
    
    
    tm_shape(sf_data) +
      tm_polygons(
        col           = "avg_risk",
        palette       = c("#198754","#85b800","#ffc107","#fd7e14","#dc3545","#7b0000"),
        breaks        = c(0, 20, 35, 50, 65, 80, 100),
        colorNA       = "#cccccc",
        textNA        = "No data",
        title         = "Avg Risk Score",
        border.col    = "#ffffff",
        border.lwd    = 1.5,
        border.alpha  = 1,
        alpha         = 0.82,
        id            = "name",
        popup.vars    = c(
          "Signals"       = "n",
          "Avg Risk"      = "avg_risk",
          "Avg NCIC Lvl"  = "avg_ncic",
          "L4+ Cases"     = "l4_plus",
          "Section 13"    = "s13",
          "Top Platform"  = "top_platform",
          "Risk Band"     = "risk_band"
        ),
        popup.format  = list(avg_risk=list(digits=0), avg_ncic=list(digits=1))
      ) +
      tm_layout(
        frame         = FALSE,
        legend.title.size = 0.9,
        legend.text.size  = 0.75
      ) +
      tm_basemap("CartoDB.Positron")
  })
  
  # keep stub for any residual references
  output$plot_county <- renderHighchart({ highchart() })
  
  
  output$dash_priority <- renderDT({
    d <- dash_data()
    d <- d[d$risk_level=="HIGH",][order(-d[d$risk_level=="HIGH","risk_score"]),][1:min(8,sum(d$risk_level=="HIGH")),]
    d$nb <- sapply(d$ncic_level,ncic_badge_html)
    d$rb <- sapply(d$risk_level,risk_badge_html)
    d$tw <- substr(d$tweet_text,1,55)
    datatable(d[,c("case_id","tw","county","nb","rb","risk_score")],
              rownames=FALSE,escape=FALSE,
              colnames=c("ID","Tweet","County","NCIC","Risk","Score"),
              options=list(pageLength=8,dom="t",scrollX=TRUE))
  })
  
  output$dash_officers <- renderDT({
    d  <- rv$cases[!is.na(rv$cases$validated_by),]
    if (nrow(d)==0) return(datatable(data.frame()))
    os <- d |> group_by(Officer=validated_by) |>
      summarise(
        Validations = n(),
        Confirmed   = sum(action_taken=="CONFIRMED",  na.rm=TRUE),
        Escalated   = sum(action_taken=="ESCALATED",  na.rm=TRUE),
        Cleared     = sum(action_taken=="CLEARED",    na.rm=TRUE),
        `Avg Risk`  = round(mean(risk_score),0),
        .groups="drop"
      ) |> arrange(desc(Validations))
    datatable(os,rownames=FALSE,options=list(dom="t",pageLength=10))
  })
  
  # ── KENYA MAP ──────────────────────────────────────────────
  output$kenya_map <- renderLeaflet({
    leaflet(options=leafletOptions(zoomControl=TRUE)) |>
      addProviderTiles("CartoDB.Positron") |>
      setView(lng=37.9,lat=0.02,zoom=6)
  })
  
  observe({
    d <- apply_date_filter(rv$cases, input$map_dr)
    if (!is.null(input$f_county) && input$f_county!="All Counties")
      d <- d[d$county==input$f_county,]
    if (!is.null(input$f_ncic) && input$f_ncic!="All")
      d <- d[d$ncic_level==as.integer(input$f_ncic),]
    if (!is.null(input$f_platform) && input$f_platform!="All")
      d <- d[d$platform==input$f_platform,]
    if (!is.null(input$f_conf))
      d <- d[d$conf_num>=input$f_conf,]
    
    agg <- d |> group_by(county) |>
      summarise(n=n(), avg_ncic=round(mean(ncic_level),1),
                avg_risk=round(mean(risk_score),0), .groups="drop") |>
      left_join(counties,by=c("county"="name"))
    
    proxy <- leafletProxy("kenya_map")
    proxy |> clearMarkers() |> clearShapes()
    
    for (i in seq_len(nrow(agg))) {
      n    <- agg$n[i]; co <- agg$county[i]
      lvl  <- round(agg$avg_ncic[i])
      col  <- unname(ncic_color(lvl))
      r    <- max(10, min(36, sqrt(n) * 2.8))
      co_d <- d[d$county == co, ]          # all cases for this county
      co_c <- co_d[seq_len(min(6, nrow(co_d))), ]   # top 6 for case table
      
      # ── Level distribution pills ──────────────────────────────
      level_pills <- paste0(
        sapply(as.character(0:5), function(l) {
          cnt <- sum(co_d$ncic_level == as.integer(l))
          if (cnt == 0) return("")
          bg      <- unname(ncic_color(as.integer(l)))
          txt_col <- if (l == "2") "#1a1a2e" else "#fff"
          paste0("<span style='background:",bg,";color:",txt_col,
                 ";border-radius:4px;padding:1px 6px;font-size:10px;",
                 "font-weight:700;margin-right:3px;display:inline-block;margin-bottom:3px;'>",
                 "L",l,": ",cnt,"</span>")
        }), collapse="")
      
      # ── Risk bar ──────────────────────────────────────────────
      risk_v   <- agg$avg_risk[i]
      risk_col <- if (risk_v >= 65) "#dc3545" else if (risk_v >= 35) "#fd7e14" else "#198754"
      risk_bar <- paste0(
        "<div style='height:6px;background:#e9ecef;border-radius:3px;",
        "margin:4px 0 10px;overflow:hidden;'>",
        "<div style='width:",risk_v,"%;height:6px;background:",risk_col,
        ";border-radius:3px;'></div></div>")
      
      # ── Platform breakdown mini-pills ─────────────────────────
      plat_agg <- sort(table(co_d$platform), decreasing=TRUE)
      plat_html <- paste0(
        sapply(names(plat_agg), function(p) {
          pct <- round(plat_agg[[p]] / n * 100)
          paste0("<span style='background:#e8f0fe;color:#0066cc;border-radius:4px;",
                 "padding:1px 6px;font-size:10px;font-weight:600;margin-right:3px;",
                 "display:inline-block;margin-bottom:3px;'>",p,": ",pct,"%</span>")
        }), collapse="")
      
      # ── Sub-location hotspot table ────────────────────────────
      sub_agg <- co_d |>
        group_by(sub_location) |>
        summarise(
          total    = n(),
          high_lvl = sum(ncic_level >= 4),
          avg_risk = round(mean(risk_score), 0),
          top_lvl  = max(ncic_level),
          .groups  = "drop"
        ) |>
        arrange(desc(high_lvl), desc(avg_risk))
      
      sub_rows <- paste0(
        sapply(seq_len(nrow(sub_agg)), function(j) {
          sb       <- sub_agg$sub_location[j]
          tot      <- sub_agg$total[j]
          hl       <- sub_agg$high_lvl[j]
          ar       <- sub_agg$avg_risk[j]
          tl       <- sub_agg$top_lvl[j]
          bar_pct  <- round(tot / max(sub_agg$total) * 100)
          rc       <- if (ar >= 65) "#dc3545" else if (ar >= 35) "#fd7e14" else "#198754"
          bc       <- unname(ncic_color(tl))
          tc       <- if (tl == 2) "#1a1a2e" else "#fff"
          # hotspot flame icon for high-risk subs
          flame    <- if (hl >= 2 || ar >= 60) "🔥 " else if (ar >= 40) "⚠ " else ""
          paste0(
            "<tr style='border-bottom:1px solid #f0f0f0;'>",
            "<td style='padding:5px 7px;font-size:11px;font-weight:600;color:#1a1a2e;'>",
            flame, sb, "</td>",
            "<td style='padding:5px 7px;'>",
            "<div style='display:flex;align-items:center;gap:5px;'>",
            "<div style='flex:1;height:5px;background:#e9ecef;border-radius:3px;min-width:50px;'>",
            "<div style='width:",bar_pct,"%;height:5px;background:",bc,";border-radius:3px;'></div></div>",
            "<span style='font-size:10px;color:#6c757d;min-width:20px;'>",tot,"</span></div></td>",
            "<td style='padding:5px 7px;text-align:center;'>",
            "<span style='background:",bc,";color:",tc,";border-radius:3px;",
            "padding:1px 5px;font-size:9px;font-weight:700;'>L",tl,"</span></td>",
            "<td style='padding:5px 7px;text-align:center;font-weight:700;",
            "font-size:11px;color:",rc,"'>",ar,"</td>",
            "<td style='padding:5px 7px;text-align:center;font-size:10px;color:#dc3545;font-weight:700;'>",
            if (hl > 0) paste0("⚖ ", hl) else "<span style='color:#9ca3af;'>—</span>",
            "</td>",
            "</tr>"
          )
        }), collapse="")
      
      # ── Recent cases table ────────────────────────────────────
      case_rows <- paste0(
        sapply(seq_len(nrow(co_c)), function(j) {
          row_col <- unname(ncic_color(co_c$ncic_level[j]))
          txt_c   <- if (co_c$ncic_level[j] == 2) "#1a1a2e" else "#fff"
          r_col   <- if (co_c$risk_score[j] >= 65) "#dc3545" else if (co_c$risk_score[j] >= 35) "#fd7e14" else "#198754"
          val_dot <- if (!is.na(co_c$validated_by[j]))
            "<span style='color:#198754;font-size:10px;'>✓</span>" else
              "<span style='color:#fd7e14;font-size:10px;'>⏳</span>"
          paste0(
            "<tr style='border-bottom:1px solid #f0f0f0;'>",
            "<td style='font-family:monospace;font-size:10px;color:#6c757d;padding:5px 6px;white-space:nowrap;'>",
            co_c$case_id[j], "</td>",
            "<td style='font-size:10px;color:#6c757d;padding:5px 4px;white-space:nowrap;'>",
            co_c$sub_location[j], "</td>",
            "<td style='font-size:11px;padding:5px 6px;color:#374151;'>",
            substr(co_c$tweet_text[j], 1, 35), "…</td>",
            "<td style='padding:5px 4px;text-align:center;'>",
            "<span style='background:",row_col,";color:",txt_c,";border-radius:4px;",
            "padding:2px 5px;font-size:10px;font-weight:700;'>L",co_c$ncic_level[j],"</span></td>",
            "<td style='padding:5px 4px;text-align:center;font-weight:700;font-size:11px;color:",r_col,"'>",
            co_c$risk_score[j], "</td>",
            "<td style='padding:5px 4px;text-align:center;'>", val_dot, "</td>",
            "</tr>"
          )
        }), collapse="")
      
      # section-13 count callout
      s13_count <- sum(co_d$section_13, na.rm=TRUE)
      s13_html  <- if (s13_count > 0)
        paste0("<span style='background:#dc3545;color:#fff;border-radius:4px;",
               "padding:2px 8px;font-size:10px;font-weight:700;margin-left:8px;'>",
               "⚖ ",s13_count," S13 cases</span>") else ""
      
      pop <- paste0(
        "<div style='min-width:480px;max-width:520px;font-family:\"Segoe UI\",Arial,sans-serif;",
        "color:#1a1a2e;border-radius:8px;overflow:hidden;'>",
        
        # ── Header ────────────────────────────────────────────
        "<div style='background:",col,";padding:12px 16px;'>",
        "<div style='color:#fff;font-size:15px;font-weight:800;letter-spacing:.01em;'>",
        "📍 ",co,"</div>",
        "<div style='color:rgba(255,255,255,0.88);font-size:11px;margin-top:3px;'>",
        n," signals &nbsp;·&nbsp; Avg NCIC: L",lvl," — ",ncic_name(lvl),
        " &nbsp;·&nbsp; Avg Risk: <strong style='color:#fff;'>",risk_v,"</strong>",
        s13_html,"</div>",
        "</div>",
        
        # ── Stats strip ───────────────────────────────────────
        "<div style='background:#f8f9fa;padding:8px 14px;border-bottom:1px solid #e9ecef;'>",
        "<div style='font-size:10px;color:#6c757d;font-weight:700;text-transform:uppercase;",
        "letter-spacing:.07em;margin-bottom:5px;'>Level Distribution</div>",
        level_pills,
        risk_bar,
        "<div style='font-size:10px;color:#6c757d;font-weight:700;text-transform:uppercase;",
        "letter-spacing:.07em;margin-bottom:5px;'>Top Platforms</div>",
        plat_html,
        "</div>",
        
        # ── Sub-location hotspots ─────────────────────────────
        "<div style='padding:0;'>",
        "<div style='background:#fff8f0;padding:6px 14px;border-bottom:1px solid #e9ecef;'>",
        "<span style='font-size:10px;font-weight:800;color:#fd7e14;text-transform:uppercase;",
        "letter-spacing:.07em;'>🗺 Sub-Location Hotspots</span></div>",
        "<table style='width:100%;border-collapse:collapse;'>",
        "<tr style='background:#f8f9fa;'>",
        "<th style='padding:4px 7px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;text-align:left;'>Ward / Area</th>",
        "<th style='padding:4px 7px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Signals</th>",
        "<th style='padding:4px 7px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Peak</th>",
        "<th style='padding:4px 7px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Risk</th>",
        "<th style='padding:4px 7px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>S13</th></tr>",
        sub_rows,
        "</table></div>",
        
        # ── Recent cases ──────────────────────────────────────
        "<div style='padding:0;border-top:2px solid #e9ecef;'>",
        "<div style='background:#f0f9ff;padding:6px 14px;border-bottom:1px solid #e9ecef;'>",
        "<span style='font-size:10px;font-weight:800;color:#0066cc;text-transform:uppercase;",
        "letter-spacing:.07em;'>📋 Recent Cases</span></div>",
        "<table style='width:100%;border-collapse:collapse;'>",
        "<tr style='background:#f8f9fa;'>",
        "<th style='padding:4px 6px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>ID</th>",
        "<th style='padding:4px 5px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Area</th>",
        "<th style='padding:4px 6px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Content</th>",
        "<th style='padding:4px 4px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Lvl</th>",
        "<th style='padding:4px 4px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Risk</th>",
        "<th style='padding:4px 4px;font-size:9px;text-transform:uppercase;letter-spacing:.07em;",
        "color:#6c757d;'>Val</th></tr>",
        case_rows,
        "</table></div>",
        
        # ── Footer ────────────────────────────────────────────
        "<div style='font-size:10px;color:#9ca3af;padding:6px 14px 8px;",
        "border-top:1px solid #f0f0f0;background:#fafafa;'>",
        "Showing top ",min(6,n)," of ",n," cases &nbsp;·&nbsp; 🔥 = hotspot &nbsp;·&nbsp; ",
        "⚖ = Section 13 threshold &nbsp;·&nbsp; ✓ = validated</div>",
        "</div>"
      )
      
      proxy |> addCircleMarkers(
        lat=agg$lat[i], lng=agg$lng[i], radius=r,
        color=col, fillColor=col, fillOpacity=0.75, weight=2, opacity=1,
        popup=pop, popupOptions=popupOptions(maxWidth=540, closeOnClick=FALSE),
        label=paste0(co, " · L", lvl, " avg · ", n, " signals · Risk ", agg$avg_risk[i])
      )
    }
    
    if (isTRUE(input$show_flow)) {
      td <- d[d$ncic_level>=3,]
      flows <- td |> group_by(county,target_county,ncic_level) |>
        summarise(n=n(),.groups="drop") |>
        left_join(counties,by=c("county"="name")) |>
        rename(sl=lat,slng=lng) |>
        left_join(counties,by=c("target_county"="name")) |>
        rename(tl=lat,tlng=lng)
      for (j in seq_len(nrow(flows)))
        proxy |> addPolylines(
          lat=c(flows$sl[j],flows$tl[j]),lng=c(flows$slng[j],flows$tlng[j]),
          color=unname(ncic_color(flows$ncic_level[j])),
          weight=max(1,min(5,flows$n[j]/5)),opacity=0.6,dashArray="6 4",
          label=paste0(flows$county[j]," → ",flows$target_county[j],
                       " L",flows$ncic_level[j]," (",flows$n[j],")"))
    }
  })
  
  # ── FLOW MAP ───────────────────────────────────────────────
  # When officer navigates to Message Flow tab, ensure flow map is centred
  observeEvent(input$main_nav, {
    if (isTRUE(input$main_nav == "tab_flow")) {
      leafletProxy("flow_map") |>
        setView(lng=37.9, lat=0.02, zoom=6)
    }
  })
  
  output$flow_map <- renderLeaflet({
    leaflet() |> addProviderTiles("CartoDB.Positron") |>
      setView(lng=37.9,lat=0.02,zoom=6)
  })
  observe({
    d <- apply_date_filter(rv$cases, input$fl_dr)
    if (!is.null(input$fl_ncic) && input$fl_ncic!="All")
      d <- d[d$ncic_level==as.integer(input$fl_ncic),]
    if (!is.null(input$fl_src) && input$fl_src!="All")
      d <- d[d$county==input$fl_src,]
    if (!is.null(input$fl_tgt) && input$fl_tgt!="All")
      d <- d[d$target_county==input$fl_tgt,]
    
    proxy <- leafletProxy("flow_map"); proxy |> clearMarkers() |> clearShapes()
    agg <- d |> group_by(county) |> summarise(n=n(),.groups="drop") |>
      left_join(counties,by=c("county"="name"))
    for (i in seq_len(nrow(agg))) {
      lvl <- round(mean(d[d$county==agg$county[i],"ncic_level"],na.rm=TRUE))
      proxy |> addCircleMarkers(lat=agg$lat[i],lng=agg$lng[i],
                                radius=max(6,min(24,sqrt(agg$n[i])*2)),
                                color=unname(ncic_color(lvl)),fillColor=unname(ncic_color(lvl)),
                                fillOpacity=0.5,weight=2,
                                label=paste0(agg$county[i]," (",agg$n[i],")"))
    }
    flows <- d |> group_by(county,target_county,target_group,ncic_level) |>
      summarise(n=n(),.groups="drop") |>
      left_join(counties,by=c("county"="name")) |> rename(sl=lat,slng=lng) |>
      left_join(counties,by=c("target_county"="name")) |> rename(tl=lat,tlng=lng)
    for (j in seq_len(nrow(flows)))
      proxy |> addPolylines(
        lat=c(flows$sl[j],flows$tl[j]),lng=c(flows$slng[j],flows$tlng[j]),
        color=unname(ncic_color(flows$ncic_level[j])),
        weight=max(1,min(6,flows$n[j]/3)),opacity=0.7,dashArray="7 4",
        popup=paste0("<b>",flows$county[j]," → ",flows$target_county[j],"</b><br>",
                     "NCIC L",flows$ncic_level[j]," — ",ncic_name(flows$ncic_level[j]),"<br>",
                     "Target: ",flows$target_group[j],"<br>Messages: ",flows$n[j]),
        label=paste0(flows$county[j]," → ",flows$target_county[j],
                     " L",flows$ncic_level[j]," ",flows$n[j]))
  })
  
  # ── CHATBOT ────────────────────────────────────────────────
  observeEvent(input$ex1, updateTextAreaInput(session,"chat_input",
                                              value="Kesho tutawaonyesha nguvu yetu, waambie wajiandae wachinjwe wote!"))
  observeEvent(input$ex2, updateTextAreaInput(session,"chat_input",
                                              value="Watu wa kabila hiyo hawana haki ya kuishi hapa Kenya yetu"))
  observeEvent(input$ex3, updateTextAreaInput(session,"chat_input",
                                              value="These migrants are vermin who should be exterminated from our land"))
  observeEvent(input$ex4, updateTextAreaInput(session,"chat_input",
                                              value="Let us unite as Kenyans and build this nation together with love"))
  
  output$chat_history <- renderUI({
    msgs <- rv$chat_history
    if (length(msgs)==0)
      return(tags$div(class="chat-container",
                      tags$div(class="chat-msg chat-bot",
                               tags$span(class="chat-thinking",
                                         "👋 Paste any post — classified under NCIC Cap 170 framework with risk formula."))))
    tags$div(class="chat-container",id="chat_scroll",
             lapply(msgs,function(m){
               if (m$role=="user")
                 tags$div(class="chat-msg chat-user",
                          tags$div(style="font-size:10px;color:#6c757d;font-family:'IBM Plex Mono';margin-bottom:2px;","YOU"),
                          m$text)
               else if (m$role=="thinking")
                 tags$div(class="chat-msg chat-bot",
                          tags$span(class="chat-thinking","⏳ Classifying under NCIC framework…"))
               else if (m$role=="error")
                 tags$div(class="error-msg",tags$strong("API Error: "),m$text)
               else {
                 lvl  <- m$ncic_level %||% 0
                 nc   <- ncic_color(lvl)
                 rcol <- if(m$risk_score>=65)"#dc3545" else if(m$risk_score>=35)"#fd7e14" else "#198754"
                 tags$div(class="chat-msg chat-bot",
                          tags$div(style="font-size:10px;color:#6c757d;font-family:'IBM Plex Mono';margin-bottom:3px;",
                                   paste0("NCIC ENGINE · ",OPENAI_MODEL)),
                          tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:5px;flex-wrap:wrap;",
                                   HTML(ncic_badge_html(lvl)),
                                   if (isTRUE(m$is_activism))
                                     tags$span(style="background:#0d6efd;color:#fff;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;","🗣 Activism / Political Critique") else NULL,
                                   if (isTRUE(m$section_13))
                                     tags$span(class="s13-badge","⚖ Section 13") else NULL,
                                   if (isTRUE(m$escalation_required))
                                     tags$span(style="background:#7c3aed;color:#fff;border-radius:3px;padding:1px 6px;font-size:10px;font-weight:700;","⚠ Escalate") else NULL,
                                   if (!is.null(m$target_type) && m$target_type != "none")
                                     tags$span(style="background:#e9ecef;color:#374151;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:600;",
                                               paste0("Target: ", gsub("_"," ", m$target_type))) else NULL
                          ),
                          tags$div(style="font-size:11px;",
                                   tags$strong("Confidence: "),paste0(m$conf,"% "),HTML(conf_band_html(conf_band(m$conf))),
                                   "  |  ",tags$strong("Category: "),m$category,"  |  ",
                                   tags$strong("Risk: "),tags$span(style=paste0("color:",rcol,";font-weight:700;"),m$risk_score)),
                          if (!is.null(m$formula)&&nchar(m$formula)>0)
                            tags$div(class="formula-box",m$formula) else NULL,
                          tags$div(class=paste0("ncic-action-box"),
                                   style=paste0("background:",nc,"18;border-left:3px solid ",nc,";"),
                                   tags$strong(paste0("NCIC L",lvl," Action: ")),ncic_action(lvl)),
                          # Activism reasoning box
                          if (isTRUE(m$is_activism) && nchar(m$activism_reasoning %||% "")>0)
                            tags$div(style="background:#e8f4ff;border-left:3px solid #0d6efd;border-radius:4px;padding:5px 9px;font-size:11px;margin-bottom:4px;color:#0a3d6e;",
                                     tags$strong("🗣 Activism context: "), m$activism_reasoning) else NULL,
                          if (!is.null(m$legal_basis)&&nchar(m$legal_basis %||% "")>0)
                            tags$div(style="font-size:10px;color:#6c757d;","Legal basis: ",m$legal_basis) else NULL,
                          if (length(m$signals)>0)
                            tagList(lapply(m$signals,function(s)
                              tags$div(class="explain-signal","🔎 ",s))) else NULL,
                          if (length(m$ctx_factors)>0)
                            tags$div(style="font-size:10px;color:#dc3545;margin-top:3px;font-weight:600;",
                                     "⬆ Escalation signals: ",paste(m$ctx_factors,collapse=" · ")) else NULL,
                          if (length(m$deesc_factors)>0)
                            tags$div(style="font-size:10px;color:#198754;margin-top:2px;font-weight:600;",
                                     "⬇ De-escalation signals: ",paste(m$deesc_factors,collapse=" · ")) else NULL,
                          if (!is.null(m$source_context_note) && nchar(m$source_context_note)>0)
                            tags$div(style="background:#fff8e1;border-left:3px solid #ffc107;border-radius:4px;padding:5px 9px;font-size:10px;margin-top:4px;color:#664d03;",
                                     tags$strong("📊 Source history: "), m$source_context_note) else NULL,
                          tags$div(style="font-size:11px;color:#6c757d;margin-top:3px;font-style:italic;",m$reasoning),
                          if (!is.null(m$source_region)&&m$source_region!="Unknown")
                            tags$div(style="font-size:11px;color:#0066cc;margin-top:3px;",
                                     "📍 ",m$source_region,
                                     if (!is.null(m$target_group)&&m$target_group!="None")
                                       paste0(" → 🎯 ",m$target_group) else "") else NULL
                 )
               }
             }),
             tags$script("var el=document.getElementById('chat_scroll');if(el)el.scrollTop=el.scrollHeight;")
    )
  })
  
  observeEvent(input$chat_send, {
    req(input$chat_input, nchar(trimws(input$chat_input))>0)
    tw <- trimws(input$chat_input)
    rv$chat_history <- c(rv$chat_history,
                         list(list(role="user",text=tw)),
                         list(list(role="thinking")))
    updateTextAreaInput(session,"chat_input",value="")
    
    res <- tryCatch(classify_tweet(tw, rv$kw_weights,
                                   handle="@chat_input", county="Unknown",
                                   cases_df=rv$cases),
                    error=function(e) list(role="error",text=conditionMessage(e)))
    hist <- rv$chat_history
    if (!is.null(res$role)&&res$role=="error") {
      hist[[length(hist)]] <- res
    } else {
      hist[[length(hist)]] <- list(
        role=             "bot",
        ncic_level=       res$ncic_level    %||% 0,
        label=            res$label         %||% ncic_name(res$ncic_level %||% 0),
        conf=             res$confidence    %||% 0,
        category=         res$category      %||% "Unknown",
        reasoning=        res$reasoning     %||% "",
        legal_basis=      res$legal_basis   %||% "",
        section_13=       isTRUE(res$section_13),
        escalation_required= isTRUE(res$escalation_required),
        risk_score=       res$risk_score    %||% 0,
        formula=          res$risk_formula  %||% "",
        signals=          unlist(res$signals %||% list()),
        ctx_factors=      unlist(res$contextual_factors %||% list()),
        deesc_factors=    unlist(res$deescalation_factors %||% list()),
        is_activism=      isTRUE(res$is_activism),
        activism_reasoning= res$activism_reasoning %||% "",
        target_type=      res$target_type   %||% "none",
        source_region=      res$source_region       %||% "Unknown",
        target_group=       res$target_group        %||% "None",
        source_history_score= res$source_history_score %||% 0,
        source_context_note=  res$source_context_note  %||% ""
      )
    }
    rv$chat_history <- hist
    rv$cache_size   <- length(ls(classify_cache))
  })
  
  # ── Cache controls ─────────────────────────────────────────
  output$cache_info <- renderUI({
    tags$span(style="font-size:11px;color:#198754;padding:4px 8px;background:rgba(25,135,84,0.1);border-radius:4px;",
              paste0("Cache: ",rv$cache_size," entries"))
  })
  observeEvent(input$btn_clear_cache, {
    rm(list=ls(classify_cache),envir=classify_cache)
    if(file.exists(CACHE_FILE)) file.remove(CACHE_FILE)
    rv$cache_size <- 0
    showNotification("Cache cleared.", type="message")
  })
  
  # ── Bulk async classify (cache writes on main thread) ──────
  output$bulk_status_ui <- renderUI({
    n_fail <- length(rv$bulk_errors)
    if (!rv$bulk_running && rv$bulk_done == 0 && n_fail == 0) return(NULL)
    
    if (rv$bulk_running) {
      pct  <- round(rv$bulk_done / max(rv$bulk_total, 1) * 100)
      tags$div(class="bulk-progress",
               tags$div(style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;",
                        tags$span(paste0("⚡ ", rv$bulk_done, " / ", rv$bulk_total, " (", pct, "%) — non-blocking")),
                        if (n_fail > 0)
                          tags$span(style="color:#dc3545;font-weight:700;font-size:11px;",
                                    paste0("⚠ ", n_fail, " failed"))
                        else NULL
               ),
               tags$div(style="height:4px;background:#b3d4ff;border-radius:2px;overflow:hidden;",
                        tags$div(style=paste0("width:",pct,"%;height:4px;background:#0066cc;border-radius:2px;transition:width .3s;")))
      )
    } else {
      fail_note <- if (n_fail > 0)
        tags$span(style="color:#dc3545;font-weight:700;",
                  paste0(" · ⚠ ", n_fail, " failed — see failure log below"))
      else NULL
      tags$div(class="bulk-progress", style="background:#d1fae5;border-color:#6ee7b7;color:#065f46;",
               paste0("✅ Complete — ", rv$bulk_done, " cases processed"),
               if (!is.null(fail_note)) fail_note else
                 tags$span(style="color:#065f46;"," · 0 failures")
      )
    }
  })
  
  # ── API Failure Dashboard ───────────────────────────────────────
  output$api_failure_ui <- renderUI({
    failures <- db_load_failures(unresolved_only=TRUE)
    if (nrow(failures) == 0) return(NULL)
    
    card(style="border-top:3px solid #dc3545;margin-bottom:12px;",
         card_header(
           tags$div(style="display:flex;justify-content:space-between;align-items:center;",
                    tagList(bs_icon("exclamation-triangle"),
                            tags$span(style="color:#dc3545;font-weight:700;",
                                      paste0(" API Failure Log — ", nrow(failures), " unresolved"))),
                    tags$div(style="display:flex;gap:6px;",
                             actionButton("btn_retry_failed_inline",
                                          tagList(bs_icon("arrow-repeat")," Retry All"), class="btn btn-warning btn-sm"),
                             actionButton("btn_clear_failures",
                                          tagList(bs_icon("check2-all")," Mark Resolved"), class="btn btn-outline-secondary btn-sm"))
           )
         ),
         tags$div(style="font-size:11px;color:#6c757d;padding:4px 0 8px;",
                  "These cases could not be classified. Click Retry to attempt again with exponential backoff."),
         DTOutput("failure_table")
    )
  })
  
  output$failure_table <- renderDT({
    failures <- db_load_failures(unresolved_only=TRUE)
    if (nrow(failures) == 0) return(datatable(data.frame()))
    d <- failures[, c("ts","case_id","tweet_text","error_msg","attempt")]
    d$tweet_text <- substr(d$tweet_text, 1, 60)
    d$error_msg  <- substr(d$error_msg,  1, 80)
    datatable(d, rownames=FALSE,
              colnames=c("Time","Case ID","Content","Error","Attempts"),
              options=list(dom="t", pageLength=10, scrollX=TRUE,
                           columnDefs=list(list(width="200px", targets=2),
                                           list(width="200px", targets=3))))
  })
  
  # Clear failures button
  observeEvent(input$btn_clear_failures, {
    db_clear_failures()
    rv$bulk_errors <- list()
    showNotification("All failures marked as resolved.", type="message", duration=3)
  })
  
  # ── Bulk classification with retry queue ─────────────────────────
  observeEvent(input$btn_bulk, {
    req(!rv$bulk_running)
    rv$bulk_running <- TRUE
    rv$bulk_done    <- 0
    rv$bulk_total   <- nrow(rv$cases)
    rv$bulk_errors  <- list()
    rv$bulk_retries <- 0L
    tweets_snap     <- rv$cases$tweet_text
    kw_snap         <- rv$kw_weights
    
    # API-only function — runs in future, returns NULL on any error
    call_api_only <- function(tweet, api_key) {
      fs  <- build_few_shot()
      up  <- paste0(fs, 'Apply VIOLENCE OVERRIDE then ACTIVISM TEST.\nClassify: "',
                    gsub('"', "'", tweet),
                    '"\nReturn: {"ncic_level":<0-5>,"label":"...","confidence":<0-100>,',
                    '"category":"...","is_activism":<true|false>,"reasoning":"...",',
                    '"legal_basis":"...","section_13":<true|false>,',
                    '"escalation_required":<true|false>,"action":"...",',
                    '"contextual_factors":[],"deescalation_factors":[],"signals":[],',
                    '"source_region":"...","target_group":"...","keyword_score":<0-100>,',
                    '"network_score":<0-100>}')
      
      # Retry with exponential backoff: up to 3 attempts
      max_attempts <- 3L
      for (attempt in seq_len(max_attempts)) {
        if (attempt > 1) Sys.sleep(2 ^ (attempt - 1))  # 2s, 4s backoff
        
        resp <- tryCatch(
          request("https://api.openai.com/v1/chat/completions") |>
            req_timeout(30) |>
            req_headers("Content-Type"="application/json",
                        "Authorization"=paste("Bearer", api_key)) |>
            req_body_json(list(model=OPENAI_MODEL, max_tokens=400, temperature=0,
                               messages=list(list(role="system", content=NCIC_SYSTEM_PROMPT),
                                             list(role="user",   content=up)))) |>
            req_error(is_error=\(r) FALSE) |>
            req_perform() |>
            resp_body_json(),
          error=function(e) list(.__error__=conditionMessage(e))
        )
        
        # Check for HTTP error or network error
        if (!is.null(resp$`.__error__`))
          err_msg <- resp$`.__error__`
        else if (!is.null(resp$error))
          err_msg <- resp$error$message %||% "API error"
        else
          err_msg <- NULL
        
        if (is.null(err_msg)) {
          # Parse JSON response
          raw <- gsub("```json|```|\\n", "", trimws(resp$choices[[1]]$message$content))
          result <- tryCatch(fromJSON(raw), error=function(e) NULL)
          if (!is.null(result)) return(list(result=result, attempts=attempt))
          err_msg <- "JSON parse failed"
        }
        
        # Last attempt — give up
        if (attempt == max_attempts)
          return(list(result=NULL, error=err_msg, attempts=attempt))
      }
      list(result=NULL, error="Max retries exceeded", attempts=max_attempts)
    }
    
    process_next <- function(i) {
      if (i > length(tweets_snap)) {
        rv$cases$risk_level <- with(rv$cases,
                                    ifelse(risk_score>=65,"HIGH",ifelse(risk_score>=35,"MEDIUM","LOW")))
        rv$bulk_running <- FALSE
        rv$cache_size   <- length(ls(classify_cache))
        save_cache()
        n_fail <- length(rv$bulk_errors)
        if (n_fail == 0) {
          showNotification(
            paste0("✅ Bulk complete — ", rv$bulk_done, " cases classified successfully."),
            type="message", duration=6)
        } else {
          showNotification(
            paste0("⚠ Bulk complete — ", rv$bulk_done - n_fail, " succeeded, ",
                   n_fail, " failed. See failure log."),
            type="warning", duration=8)
        }
        return(invisible(NULL))
      }
      
      tweet <- tweets_snap[i]
      cid   <- rv$cases$case_id[i]
      key   <- digest(paste0(tolower(trimws(tweet)), "|ex:", length(load_examples())))
      
      # Cache hit — skip API call
      if (exists(key, envir=classify_cache)) {
        res <- get(key, envir=classify_cache)
        if (!is.null(res)) {
          ctx_sc  <- length(res$contextual_factors %||% list()) * 8
          src_ctx <- compute_source_context(rv$cases$handle[i], rv$cases$county[i], rv$cases)
          rs <- compute_risk(tweet, res$confidence %||% 50, res$ncic_level %||% 0,
                             kw_snap, res$network_score %||% 20, 10, ctx_sc, src_ctx$score)
          rv$cases$ncic_level[i]   <- res$ncic_level   %||% rv$cases$ncic_level[i]
          rv$cases$ncic_label[i]   <- ncic_name(rv$cases$ncic_level[i])
          rv$cases$section_13[i]   <- isTRUE(res$section_13)
          rv$cases$conf_num[i]     <- res$confidence   %||% rv$cases$conf_num[i]
          rv$cases$confidence[i]   <- paste0(rv$cases$conf_num[i], "%")
          rv$cases$conf_band[i]    <- conf_band(rv$cases$conf_num[i])
          rv$cases$category[i]     <- res$category     %||% rv$cases$category[i]
          rv$cases$risk_score[i]   <- rs$score
          rv$cases$risk_formula[i] <- rs$formula
          if (!is.null(res$signals))
            rv$cases$signals[i] <- paste(unlist(res$signals), collapse="|")
          if (!is.null(res$target_group) && res$target_group != "None")
            rv$cases$target_group[i] <- res$target_group
          # resolve any prior failure for this case
          db_resolve_failure(cid)
        }
        rv$bulk_done <- i; rv$cache_size <- length(ls(classify_cache))
        process_next(i+1)
        return(invisible(NULL))
      }
      
      api_key <- Sys.getenv("OPENAI_API_KEY")
      future_promise({ call_api_only(tweet, api_key) }) %...>% (function(out) {
        if (!is.null(out$result)) {
          res <- out$result
          assign(key, res, envir=classify_cache)
          ctx_sc  <- length(res$contextual_factors %||% list()) * 8
          src_ctx <- compute_source_context(rv$cases$handle[i], rv$cases$county[i], rv$cases)
          rs <- compute_risk(tweet, res$confidence %||% 50, res$ncic_level %||% 0,
                             kw_snap, res$network_score %||% 20, 10, ctx_sc, src_ctx$score)
          rv$cases$ncic_level[i]   <- res$ncic_level   %||% rv$cases$ncic_level[i]
          rv$cases$ncic_label[i]   <- ncic_name(rv$cases$ncic_level[i])
          rv$cases$section_13[i]   <- isTRUE(res$section_13)
          rv$cases$conf_num[i]     <- res$confidence   %||% rv$cases$conf_num[i]
          rv$cases$confidence[i]   <- paste0(rv$cases$conf_num[i], "%")
          rv$cases$conf_band[i]    <- conf_band(rv$cases$conf_num[i])
          rv$cases$category[i]     <- res$category     %||% rv$cases$category[i]
          rv$cases$risk_score[i]   <- rs$score
          rv$cases$risk_formula[i] <- rs$formula
          if (!is.null(res$signals))
            rv$cases$signals[i] <- paste(unlist(res$signals), collapse="|")
          if (!is.null(res$target_group) && res$target_group != "None")
            rv$cases$target_group[i] <- res$target_group
          # resolve any prior failure
          db_resolve_failure(cid)
        } else {
          # Record failure — log to DB and rv
          err <- out$error %||% "Unknown error"
          att <- out$attempts %||% 1L
          db_log_failure(cid, tweet, err, att)
          rv$bulk_errors[[cid]] <- list(error=err, attempts=att, tweet=substr(tweet,1,60))
          message(sprintf("[bulk] FAILED case %s after %d attempt(s): %s", cid, att, err))
        }
        rv$bulk_done  <- i
        rv$cache_size <- length(ls(classify_cache))
        process_next(i+1)
      }) %...!% (function(err) {
        # Promise itself rejected (unexpected)
        err_msg <- conditionMessage(err)
        db_log_failure(cid, tweet, paste0("Promise error: ", err_msg), 1L)
        rv$bulk_errors[[cid]] <- list(error=err_msg, attempts=1L, tweet=substr(tweet,1,60))
        message(sprintf("[bulk] Promise error row %d: %s", i, err_msg))
        rv$bulk_done <- i
        process_next(i+1)
      })
    }
    process_next(1)
  })
  
  # ── Retry failed cases ──────────────────────────────────────────
  retry_handler <- function() {
    failures <- db_load_failures(unresolved_only=TRUE)
    if (nrow(failures) == 0) {
      showNotification("No unresolved failures to retry.", type="message", duration=3)
      return()
    }
    if (rv$bulk_running) {
      showNotification("Bulk classification already running. Wait for it to finish.", type="warning")
      return()
    }
    api_key <- Sys.getenv("OPENAI_API_KEY")
    n_retry <- nrow(failures)
    showNotification(paste0("Retrying ", n_retry, " failed cases…"), type="message", duration=4)
    
    for (j in seq_len(n_retry)) {
      local({
        cid   <- failures$case_id[j]
        tweet <- failures$tweet_text[j]
        att   <- as.integer(failures$attempt[j]) + 1L
        # Remove from cache so it gets re-classified
        key <- digest(paste0(tolower(trimws(tweet)), "|ex:", length(load_examples())))
        if (exists(key, envir=classify_cache)) rm(list=key, envir=classify_cache)
        
        future_promise({
          Sys.sleep(j * 0.5)  # stagger retries
          resp <- tryCatch(
            request("https://api.openai.com/v1/chat/completions") |>
              req_timeout(30) |>
              req_headers("Content-Type"="application/json",
                          "Authorization"=paste("Bearer", api_key)) |>
              req_body_json(list(model=OPENAI_MODEL, max_tokens=400, temperature=0,
                                 messages=list(
                                   list(role="system", content=NCIC_SYSTEM_PROMPT),
                                   list(role="user",   content=paste0('Classify: "',
                                                                      gsub('"',"'",tweet),
                                                                      '"\nReturn minimal JSON: {"ncic_level":<0-5>,"confidence":<0-100>,"category":"...","section_13":<true|false>,"signals":[],"target_group":"None","network_score":20}'))
                                 ))) |>
              req_error(is_error=\(r) FALSE) |> req_perform() |> resp_body_json(),
            error=function(e) list(error=list(message=conditionMessage(e))))
          if (!is.null(resp$error)) return(list(result=NULL, error=resp$error$message))
          raw    <- gsub("```json|```|\\n","",trimws(resp$choices[[1]]$message$content))
          result <- tryCatch(fromJSON(raw), error=function(e) NULL)
          list(result=result, error=if(is.null(result)) "JSON parse failed" else NULL)
        }) %...>% (function(out) {
          if (!is.null(out$result)) {
            res <- out$result
            idx <- which(rv$cases$case_id == cid)
            if (length(idx) > 0) {
              rv$cases$ncic_level[idx]  <- res$ncic_level  %||% rv$cases$ncic_level[idx]
              rv$cases$ncic_label[idx]  <- ncic_name(rv$cases$ncic_level[idx])
              rv$cases$section_13[idx]  <- isTRUE(res$section_13)
              rv$cases$conf_num[idx]    <- res$confidence  %||% rv$cases$conf_num[idx]
              rv$cases$confidence[idx]  <- paste0(rv$cases$conf_num[idx], "%")
            }
            db_resolve_failure(cid)
            rv$bulk_errors[[cid]] <- NULL
            rv$bulk_retries <- rv$bulk_retries + 1L
            message(sprintf("[retry] SUCCESS: %s", cid))
          } else {
            db_log_failure(cid, tweet, out$error %||% "Retry failed", att)
            message(sprintf("[retry] FAILED again: %s — %s", cid, out$error))
          }
        }) %...!% (function(e) {
          message(sprintf("[retry] Promise error for %s: %s", cid, conditionMessage(e)))
        })
      })
    }
  }
  
  observeEvent(input$btn_retry_failed,         retry_handler())
  observeEvent(input$btn_retry_failed_inline,  retry_handler())
  
  # ── Raw signals ────────────────────────────────────────────
  output$raw_count <- renderUI({
    tags$span(style="font-size:12px;font-weight:400;color:#6c757d;",
              formatC(nrow(rv$cases), format="d", big.mark=","), " signals")
  })
  
  # Reset Message Flow filters
  observeEvent(input$fl_reset, {
    updateSelectInput(session, "fl_ncic", selected="5")
    updateSelectInput(session, "fl_src",  selected="All")
    updateSelectInput(session, "fl_tgt",  selected="All")
    updateDateRangeInput(session, "fl_dr", start=DATE_MIN, end=DATE_MAX)
  })
  
  output$raw_table <- renderDT({
    d <- rv$cases[,c("case_id","platform","handle","tweet_text","county","timestamp_chr")]
    d$status <- "UNCLASSIFIED"
    datatable(d,rownames=FALSE,
              colnames=c("Case ID","Platform","Handle","Content","County","Timestamp","Status"),
              options=list(pageLength=15,scrollX=TRUE,dom="frtip",
                           columnDefs=list(list(width="260px",targets=3))))
  })
  
  # ── Classified table ───────────────────────────────────────
  output$clf_table <- renderDT({
    d <- clf_data()
    d$nb  <- sapply(d$ncic_level,ncic_badge_html)
    d$rb  <- sapply(d$risk_level,risk_badge_html)
    d$sb  <- mapply(risk_bar_html,d$risk_score,d$risk_level)
    d$tr  <- sapply(d$trend_data,sparkline_html)
    d$ch  <- mapply(function(n,b) paste0(n,"% ",conf_band_html(b)),d$conf_num,d$conf_band)
    d$vh  <- sapply(d$validated_by,officer_tag_html)
    d$s13 <- ifelse(d$section_13,"<span class='s13-badge'>S13</span>","")
    d$tw  <- substr(d$tweet_text,1,60)
    disp  <- d[,c("case_id","tw","platform","county","language","nb","s13","rb","sb","ch","tr","vh","timestamp_chr")]
    datatable(disp,rownames=FALSE,escape=FALSE,
              colnames=c("ID","Content","Platform","County","Lang","NCIC","S13","Risk","Score","Conf","Trend","Validated","Timestamp"),
              options=list(pageLength=15,scrollX=TRUE,dom="frtip",
                           columnDefs=list(list(width="180px",targets=1))))
  })
  
  # ── Network ────────────────────────────────────────────────
  output$net_handles <- renderDT({
    d  <- rv$cases
    hs <- do.call(rbind,lapply(handles,function(h){
      hd <- d[d$handle==h,]
      tc <- sum(hd$ncic_level>=4,na.rm=TRUE)
      data.frame(Handle=h,Posts=nrow(hd),`L4+L5`=tc,
                 `%High`=if(nrow(hd)>0) round(tc/nrow(hd)*100,0) else 0,
                 `Avg Risk`=if(nrow(hd)>0) round(mean(hd$risk_score),0) else 0,
                 check.names=FALSE)
    }))
    datatable(hs,rownames=FALSE,options=list(dom="t",pageLength=10,order=list(list(4,"desc"))))
  })
  
  output$net_cib <- renderUI({
    d <- rv$cases[rv$cases$ncic_level>=3,]
    if (nrow(d)==0) return(tags$p("No data."))
    pairs <- d |> group_by(county,target_county) |>
      summarise(n=n(),handles=n_distinct(handle),.groups="drop") |>
      filter(handles>=2) |> arrange(desc(n)) |> head(10)
    if (nrow(pairs)==0) return(tags$p(style="color:#6c757d;font-size:12px;","No CIB detected."))
    tagList(
      tags$p(style="font-size:11px;color:#6c757d;margin-bottom:8px;",
             "Routes where 2+ handles posted L3+ content — possible coordinated inauthentic behaviour."),
      lapply(seq_len(nrow(pairs)),function(i)
        tags$div(style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #f0f0f0;padding:5px 0;",
                 tags$span(style="font-size:12px;font-weight:600;",paste0(pairs$county[i]," → ",pairs$target_county[i])),
                 tags$span(style="font-size:11px;color:#7c3aed;",paste0(pairs$n[i]," msgs · ",pairs$handles[i]," handles")),
                 tags$span(class="badge-high","⚠ CIB")))
    )
  })
  
  output$net_cases <- renderDT({
    d <- rv$cases[rv$cases$ncic_level>=3,]
    d$nb <- sapply(d$ncic_level,ncic_badge_html)
    disp <- d[,c("case_id","handle","county","target_county","target_group","nb","risk_score","timestamp_chr")]
    datatable(disp,rownames=FALSE,escape=FALSE,
              colnames=c("ID","Handle","Origin","Target County","Target Group","NCIC","Risk","Timestamp"),
              options=list(pageLength=10,scrollX=TRUE,dom="frtip"))
  })
  
  # ── VALIDATION UI (paginated, full, note-persisted) ────────
  rv_val_page <- reactiveVal(1)
  observeEvent(input$val_prev, rv_val_page(max(1,rv_val_page()-1)))
  observeEvent(input$val_next, {
    pending <- rv$cases[is.na(rv$cases$validated_by)&rv$cases$ncic_level>=2,]
    rv_val_page(min(ceiling(nrow(pending)/VAL_PAGE_SIZE), rv_val_page()+1))
  })
  
  output$val_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui())
    pending <- rv$cases[is.na(rv$cases$validated_by)&rv$cases$ncic_level>=2,]
    pending <- pending[order(-pending$risk_score,-pending$ncic_level),]
    
    if (nrow(pending)==0)
      return(tags$div(style="text-align:center;padding:60px;color:#6c757d;",
                      "✅ All cases reviewed."))
    
    email_ok <- all(nchar(Sys.getenv(c("GMAIL_USER","GMAIL_PASS","OFFICER_EMAIL")))>0)
    tp <- ceiling(nrow(pending)/VAL_PAGE_SIZE)
    cp <- min(rv_val_page(), tp)
    rows <- pending[((cp-1)*VAL_PAGE_SIZE+1):min(nrow(pending),cp*VAL_PAGE_SIZE),]
    
    lapply(seq_len(nrow(rows)),function(i) ensure_email_output(rows$case_id[i]))
    
    tagList(
      tags$div(style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;",
               tags$div(
                 tags$h5(style="margin:0;",tagList(bs_icon("shield-check")," Officer Validation — NCIC Framework")),
                 tags$p(style="font-size:12px;color:#6c757d;margin:3px 0 0;",
                        paste0(nrow(pending)," pending (L2+) · sorted by risk score · officer: ",
                               tags$strong(rv$officer_name)))
               ),
               tags$div(style="display:flex;gap:8px;align-items:center;",
                        if (!rv$bulk_running && nchar(rv$learning_flash)>0)
                          tags$div(class="learning-flash","🧠 ",rv$learning_flash) else NULL,
                        if(email_ok)
                          tags$span(style="font-size:11px;color:#198754;background:rgba(25,135,84,0.1);border:1px solid rgba(25,135,84,0.3);border-radius:4px;padding:3px 10px;",
                                    paste0("📧 ",Sys.getenv("OFFICER_EMAIL")))
                        else
                          tags$span(style="font-size:11px;color:#fd7e14;background:rgba(253,126,20,0.1);border:1px solid rgba(253,126,20,0.3);border-radius:4px;padding:3px 10px;","⚠ Email not configured"),
                        downloadButton("dl_val","⬇ Export",style="background:#0066cc;color:#fff;border:none;font-weight:600;border-radius:6px;")
               )
      ),
      
      tags$div(style="display:flex;align-items:center;gap:10px;margin-bottom:12px;",
               actionButton("val_prev","← Prev",class="btn btn-sm btn-outline-secondary",disabled=(cp<=1)),
               tags$span(style="font-size:12px;color:#6c757d;",
                         paste0("Page ",cp," / ",tp," · ",nrow(pending)," total")),
               actionButton("val_next","Next →",class="btn btn-sm btn-outline-secondary",disabled=(cp>=tp))
      ),
      
      layout_columns(col_widths=c(6,6),
                     lapply(seq_len(nrow(rows)),function(i){
                       row  <- rows[i,]
                       cid  <- row$case_id
                       lvl  <- row$ncic_level
                       nc   <- ncic_color(lvl)
                       rc   <- switch(row$risk_level,HIGH="#dc3545",MEDIUM="#fd7e14","#198754")
                       sigs <- strsplit(row$signals %||% "","\\|")[[1]]
                       cur_note <- rv$notes[[cid]] %||% ""
                       
                       card(style=paste0("border-top:3px solid ",nc,";"),
                            card_header(tags$div(style="display:flex;justify-content:space-between;align-items:center;",
                                                 tags$span(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;",cid),
                                                 tags$div(HTML(ncic_badge_html(lvl)),
                                                          if(isTRUE(row$section_13)) tags$span(class="s13-badge ms-1","⚖ S13") else NULL)
                            )),
                            
                            tags$div(class="val-tweet-box",style=paste0("border-left:3px solid ",nc,";"),row$tweet_text),
                            
                            # NCIC action block
                            tags$div(class="ncic-action-box",
                                     style=paste0("background:",nc,"18;border-left:3px solid ",nc,";"),
                                     tags$strong(paste0("NCIC L",lvl," — ",ncic_name(lvl),": ")),
                                     ncic_action(lvl)),
                            
                            # Meta badges
                            tags$div(style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:8px;",
                                     tags$span(style="background:#e9ecef;color:#495057;border-radius:4px;padding:2px 7px;font-size:11px;",row$county),
                                     tags$span(style="background:#e9ecef;color:#495057;border-radius:4px;padding:2px 7px;font-size:11px;",row$platform),
                                     tags$span(style="background:#e9ecef;color:#495057;border-radius:4px;padding:2px 7px;font-size:11px;",row$language),
                                     tags$span(style="background:#e9ecef;color:#495057;border-radius:4px;padding:2px 7px;font-size:11px;",row$category),
                                     if(!is.na(row$target_group)&&nchar(row$target_group)>0)
                                       tags$span(style="background:rgba(220,53,69,0.1);color:#dc3545;border-radius:4px;padding:2px 7px;font-size:11px;font-weight:600;",
                                                 paste0("🎯 ",row$target_group)) else NULL,
                                     HTML(conf_band_html(row$conf_band))
                            ),
                            
                            # Risk bar + formula
                            tags$div(style="margin-bottom:7px;",
                                     tags$div(style="font-size:10px;color:#6c757d;margin-bottom:2px;",
                                              paste0("Risk: ",row$risk_score," / 100")),
                                     tags$div(style="height:6px;background:#e9ecef;border-radius:3px;overflow:hidden;",
                                              tags$div(style=paste0("width:",row$risk_score,"%;height:6px;background:",rc,";border-radius:3px;")))),
                            if(nchar(row$risk_formula)>0) tags$div(class="formula-box",row$risk_formula) else NULL,
                            
                            # Signals
                            if(length(sigs)>0&&nchar(sigs[1])>0)
                              tags$div(style="margin-bottom:7px;",
                                       tags$div(style="font-size:10px;color:#7c3aed;font-weight:700;margin-bottom:3px;","🧠 WHY FLAGGED:"),
                                       tagList(lapply(sigs,function(s) tags$div(class="explain-signal","🔎 ",s)))) else NULL,
                            
                            # NCIC level override
                            tags$div(style="margin-bottom:7px;",
                                     tags$div(style="font-size:10px;color:#374151;font-weight:600;margin-bottom:3px;",
                                              "Officer NCIC Assessment:"),
                                     selectInput(paste0("ncic_ov_",cid),NULL,width="100%",
                                                 choices=c("Agree with GPT"="",
                                                           setNames(as.character(0:5),
                                                                    paste0("Override → L",0:5," ",NCIC_LEVELS))),
                                                 selected="")),
                            
                            # Note — value restored from rv$notes
                            textAreaInput(paste0("note_",cid),"Officer note",value=cur_note,
                                          placeholder="Justification, context, or instructions…",rows=2,width="100%"),
                            
                            # Buttons
                            tags$div(style="display:flex;gap:4px;margin-top:8px;flex-wrap:wrap;",
                                     actionButton(paste0("confirm_",cid), "✓ Confirm",    class="btn-val-confirm"),
                                     actionButton(paste0("escalate_",cid),"⬆ Escalate",   class="btn-val-escalate"),
                                     actionButton(paste0("dgrade_",cid),  "↓ Downgrade",  class="btn-val-downgrade"),
                                     actionButton(paste0("clear_",cid),   "✓ Clear",      class="btn-val-clear"),
                                     if(email_ok) actionButton(paste0("email_",cid),"📧 Alert",class="btn-val-email") else NULL
                            ),
                            uiOutput(paste0("email_status_",cid))
                       )
                     })
      )
    )
  })
  
  # note persistence: save as officer types
  observe({
    pend <- rv$cases[is.na(rv$cases$validated_by)&rv$cases$ncic_level>=2,]
    for (i in seq_len(nrow(pend))) {
      local({
        cid  <- pend$case_id[i]
        nid  <- paste0("note_",cid)
        if (!exists(paste0("note_obs_",cid),envir=observer_registry)) {
          assign(paste0("note_obs_",cid),TRUE,envir=observer_registry)
          observeEvent(input[[nid]], {
            rv$notes[[cid]] <- input[[nid]]
            tryCatch(db_update_case(cid, list(notes=input[[nid]] %||% "")),
                     error=function(e) NULL)
          }, ignoreInit=TRUE, ignoreNULL=FALSE)
        }
      })
    }
  })
  
  # ── Validation observers — observer registry prevents duplication ──
  observe({
    pend <- rv$cases[is.na(rv$cases$validated_by)&rv$cases$ncic_level>=2,]
    pend <- pend[order(-pend$risk_score),]
    for (i in seq_len(nrow(pend))) {
      local({
        cid <- pend$case_id[i]
        if (exists(cid,envir=observer_registry)) return()
        assign(cid,TRUE,envir=observer_registry)
        
        do_validate <- function(action_str, new_ncic=NULL, new_label=NULL) {
          officer    <- rv$officer_name
          ov         <- isolate(input[[paste0("ncic_ov_",cid)]])
          note_val   <- rv$notes[[cid]] %||% ""
          tweet_val  <- rv$cases$tweet_text[rv$cases$case_id==cid][1]
          cur_ncic   <- rv$cases$ncic_level[rv$cases$case_id==cid][1]
          final_ncic <- if (!is.null(new_ncic)) new_ncic else cur_ncic
          if (!is.null(ov) && nchar(ov)>0) {
            off_lvl <- as.integer(ov)
            if (off_lvl != cur_ncic)
              log_disagreement(officer,cid,tweet_val,cur_ncic,off_lvl)
            final_ncic <- off_lvl
          }
          
          rv$cases$ncic_level[rv$cases$case_id==cid]    <- final_ncic
          rv$cases$ncic_label[rv$cases$case_id==cid]    <- ncic_name(final_ncic)
          rv$cases$section_13[rv$cases$case_id==cid]    <- ncic_s13(final_ncic)
          rv$cases$validated_by[rv$cases$case_id==cid]  <- officer
          rv$cases$validated_at[rv$cases$case_id==cid]  <- format(Sys.time(),"%Y-%m-%d %H:%M")
          rv$cases$action_taken[rv$cases$case_id==cid]  <- action_str
          
          # HITL RL: update keyword weights on main thread
          new_weights <- update_kw_weights(tweet_val, final_ncic, action_str, rv$kw_weights)
          rv$kw_weights <- new_weights
          save_kw_weights(new_weights)
          
          # HITL: add to few-shot example bank
          add_example(tweet_val, final_ncic,
                      rv$cases$category[rv$cases$case_id==cid][1],
                      rv$cases$signals[rv$cases$case_id==cid][1],
                      action_str)
          
          # invalidate cache for this tweet so next classify uses new prompt
          key_old <- digest(tolower(trimws(tweet_val)))
          if (exists(key_old,envir=classify_cache)) rm(list=key_old,envir=classify_cache)
          
          # recalculate risk score with updated weights + source context
          src_ctx_val <- compute_source_context(
            rv$cases$handle[rv$cases$case_id==cid][1],
            rv$cases$county[rv$cases$case_id==cid][1],
            rv$cases)
          new_rs <- compute_risk(tweet_val,
                                 rv$cases$conf_num[rv$cases$case_id==cid][1],
                                 final_ncic, new_weights,
                                 rv$cases$network_score[rv$cases$case_id==cid][1],
                                 10, 0, src_ctx_val$score)
          rv$cases$risk_score[rv$cases$case_id==cid]   <- new_rs$score
          rv$cases$risk_formula[rv$cases$case_id==cid] <- new_rs$formula
          rv$cases$risk_level[rv$cases$case_id==cid]   <- if(new_rs$score>=65)"HIGH" else if(new_rs$score>=35)"MEDIUM" else "LOW"
          
          # ── Persist validation to SQLite (atomic single-row update) ──
          tryCatch({
            updated_row <- rv$cases[rv$cases$case_id == cid, ]
            updated_row$section_13  <- as.integer(updated_row$section_13)
            updated_row$timestamp   <- format(updated_row$timestamp, "%Y-%m-%d %H:%M:%S")
            fields <- as.list(updated_row[1, c(
              "ncic_level","ncic_label","section_13","validated_by","validated_at",
              "action_taken","risk_score","risk_formula","risk_level")])
            db_update_case(cid, fields)
          }, error=function(e) message("[db] validation write failed: ", e$message))
          
          # ── Re-score ALL cases with updated weights & persist ─────────
          # Run asynchronously so UI doesn't block
          later::later(function() {
            tryCatch({
              rv$cases <- rescore_all_cases(rv$cases, rv$kw_weights)
              db_save_cases(rv$cases)
              message(sprintf("[rescore] All %d cases rescored with new weights", nrow(rv$cases)))
            }, error=function(e) message("[rescore] failed: ", e$message))
          }, delay=0.5)
          
          n_examples <- length(load_examples())
          rv$learning_flash <- paste0("Weights updated · rescoring all cases · ",n_examples," training examples")
          later::later(function() rv$learning_flash <- "", 5)
        }
        
        observeEvent(input[[paste0("confirm_",cid)]], {
          do_validate("CONFIRMED")
        }, ignoreInit=TRUE)
        
        observeEvent(input[[paste0("escalate_",cid)]], {
          do_validate("ESCALATED", new_ncic=5)
        }, ignoreInit=TRUE)
        
        observeEvent(input[[paste0("dgrade_",cid)]], {
          cur <- rv$cases$ncic_level[rv$cases$case_id==cid][1]
          do_validate("DOWNGRADED", new_ncic=max(0L,as.integer(cur)-1L))
        }, ignoreInit=TRUE)
        
        observeEvent(input[[paste0("clear_",cid)]], {
          do_validate("CLEARED", new_ncic=0L)
        }, ignoreInit=TRUE)
        
        observeEvent(input[[paste0("email_",cid)]], {
          row <- rv$cases[rv$cases$case_id==cid,]
          rv$email_status[[cid]] <- "sending"
          result <- tryCatch({
            send_alert_email(
              officer      = rv$officer_name,
              case_id      = cid,
              ncic_level   = row$ncic_level[1],
              ncic_lbl     = row$ncic_label[1],
              county       = row$county[1],
              platform     = row$platform[1],
              handle       = row$handle[1],
              category     = row$category[1],
              language     = row$language[1],
              tweet_text   = row$tweet_text[1],
              timestamp    = row$timestamp_chr[1],
              decision     = row$action_taken[1] %||% row$ncic_label[1],
              note         = rv$notes[[cid]] %||% "",
              target_group = row$target_group[1] %||% "",
              risk_score   = row$risk_score[1],
              risk_formula = row$risk_formula[1] %||% "",
              signals      = row$signals[1] %||% "",
              legal_basis  = "",
              section_13   = isTRUE(row$section_13[1])
            )
            "sent"
          }, error=function(e) paste0("failed: ",conditionMessage(e)))
          rv$email_status[[cid]] <- result
        }, ignoreInit=TRUE)
      })
    }
  })
  
  # ── Learning Centre ────────────────────────────────────────
  output$learn_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui())
    ex  <- load_examples()
    dis <- load_disagreements()
    w   <- rv$kw_weights
    tagList(
      # KPIs
      layout_columns(col_widths=c(3,3,3,3),
                     value_box("Training Examples", length(ex),  showcase=bs_icon("journal-code"),      theme="primary"),
                     value_box("Active Keywords",   length(w),   showcase=bs_icon("sliders"),            theme="success"),
                     value_box("GPT Disagreements", nrow(dis),   showcase=bs_icon("exclamation-circle"), theme="warning"),
                     value_box("Cases Rescored",    nrow(rv$cases), showcase=bs_icon("arrow-repeat"),    theme="info")
      ),
      
      # ── Keyword Editor + Weights ───────────────────────────────
      layout_columns(col_widths=c(5,7),
                     
                     card(style="border-top:3px solid #0066cc;",
                          card_header(tagList(bs_icon("pencil-square")," Keyword Editor")),
                          tags$div(style="padding:4px 0;",
                                   tags$p(style="font-size:11px;color:#6c757d;margin-bottom:10px;",
                                          "Add new keywords that should escalate risk scoring. ",
                                          "Weight 1–100: 30=moderate, 50=high, 80+=critical. ",
                                          "Changes trigger a full rescore of all historical cases."),
                                   tags$div(style="display:flex;gap:6px;margin-bottom:8px;",
                                            textInput("kw_new_word", NULL, placeholder="keyword or phrase (Swahili/English)", width="55%"),
                                            numericInput("kw_new_weight", NULL, value=30, min=1, max=100, step=5, width="20%"),
                                            actionButton("kw_add_btn", tagList(bs_icon("plus-circle")," Add"),
                                                         class="btn btn-primary btn-sm", style="margin-top:1px;")
                                   ),
                                   tags$hr(style="border-color:#f0f0f0;margin:8px 0;"),
                                   tags$p(style="font-size:11px;color:#6c757d;margin-bottom:6px;",
                                          "Edit or remove existing keywords:"),
                                   tags$div(style="max-height:280px;overflow-y:auto;",
                                            lapply(sort(names(w), decreasing=TRUE, na.last=TRUE)[seq_len(min(40,length(w)))], function(kw) {
                                              score <- round(w[[kw]])
                                              bar_c <- if(score>=70)"#dc3545" else if(score>=40)"#fd7e14" else "#6c757d"
                                              tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:5px;",
                                                       tags$div(style="flex:1;font-size:11px;font-family:'IBM Plex Mono';color:#374151;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;", kw),
                                                       tags$div(style="flex:1;height:5px;background:#e9ecef;border-radius:3px;overflow:hidden;",
                                                                tags$div(style=paste0("width:",min(100,score),"%;height:5px;background:",bar_c,";border-radius:3px;"))),
                                                       tags$div(style=paste0("font-size:11px;font-weight:700;min-width:26px;color:",bar_c,";"), score),
                                                       actionButton(paste0("kw_del_",digest(kw)),
                                                                    tagList(bs_icon("trash")), class="btn btn-outline-danger btn-sm",
                                                                    style="padding:1px 5px;font-size:10px;",
                                                                    onclick=sprintf("Shiny.setInputValue('kw_delete_target','%s',{priority:'event'})", kw))
                                              )
                                            })
                                   ),
                                   tags$hr(style="border-color:#f0f0f0;margin:10px 0;"),
                                   actionButton("kw_rescore_btn",
                                                tagList(bs_icon("arrow-repeat")," Rescore All Cases Now"),
                                                class="btn btn-warning btn-sm w-100")
                          )
                     ),
                     
                     card(style="border-top:3px solid #7c3aed;",
                          card_header(tagList(bs_icon("exclamation-circle")," GPT vs Officer Disagreements")),
                          if (nrow(dis)==0)
                            tags$p(style="color:#6c757d;font-size:12px;padding:8px;","No disagreements recorded yet.")
                          else {
                            dis$gpt_n <- sapply(dis$gpt_level, ncic_name)
                            dis$off_n <- sapply(dis$officer_level, ncic_name)
                            datatable(dis[,c("ts","officer","gpt_n","off_n","tweet")],
                                      rownames=FALSE,
                                      colnames=c("Time","Officer","GPT Said","Officer Said","Content"),
                                      options=list(dom="t",pageLength=8,scrollX=TRUE,
                                                   columnDefs=list(list(width="140px",targets=4))))
                          }
                     )
      ),
      
      # ── Few-shot examples ─────────────────────────────────────
      card(card_header(tagList(bs_icon("journal-text")," Recent Training Examples (Few-Shot Bank)")),
           if (length(ex)==0)
             tags$p(style="color:#6c757d;font-size:12px;padding:8px;",
                    "No validated examples yet. Each validation automatically adds to the bank.")
           else {
             ex_df <- do.call(rbind, lapply(tail(ex,15), function(e)
               data.frame(Time=format(e$ts,"%Y-%m-%d %H:%M"),
                          Level=paste0("L",e$ncic_level," ",ncic_name(e$ncic_level)),
                          Outcome=e$outcome, Content=substr(e$tweet,1,70),
                          stringsAsFactors=FALSE)))
             datatable(ex_df, rownames=FALSE,
                       options=list(dom="t",pageLength=10,scrollX=TRUE,
                                    columnDefs=list(list(width="200px",targets=3))))
           }
      )
    )
  })
  
  # ── Keyword editor observers ───────────────────────────────────
  observeEvent(input$kw_add_btn, {
    kw  <- trimws(tolower(input$kw_new_word %||% ""))
    wt  <- as.numeric(input$kw_new_weight %||% 30)
    req(nchar(kw) >= 2, !is.na(wt), wt >= 1, wt <= 100)
    new_w <- rv$kw_weights
    new_w[[kw]] <- wt
    rv$kw_weights <- new_w
    save_kw_weights(new_w)
    updateTextInput(session, "kw_new_word", value="")
    later::later(function() {
      rv$cases <- rescore_all_cases(rv$cases, rv$kw_weights)
      db_save_cases(rv$cases)
    }, delay=0.3)
    showNotification(paste0("Added '",kw,"' (weight ",wt,") · rescoring cases…"),
                     type="message", duration=4)
  })
  
  observeEvent(input$kw_delete_target, {
    kw <- input$kw_delete_target
    req(nchar(kw) > 0)
    new_w <- rv$kw_weights
    new_w[[kw]] <- NULL
    rv$kw_weights <- new_w
    save_kw_weights(new_w)
    later::later(function() {
      rv$cases <- rescore_all_cases(rv$cases, rv$kw_weights)
      db_save_cases(rv$cases)
    }, delay=0.3)
    showNotification(paste0("Removed '",kw,"' · rescoring cases…"), type="warning", duration=4)
  })
  
  observeEvent(input$kw_rescore_btn, {
    rv$cases <- rescore_all_cases(rv$cases, rv$kw_weights)
    db_save_cases(rv$cases)
    showNotification(sprintf("Rescored all %d cases with current keyword weights.", nrow(rv$cases)),
                     type="message", duration=5)
  })
  
  # ── Forecast ───────────────────────────────────────────────
  output$forecast_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui())
    
    d   <- rv$cases
    now <- Sys.time()
    
    # ── Build per-county forecast ──────────────────────────────
    # Uses last 30 days to project next 14-day risk trajectory
    window_days  <- 30
    forecast_days <- 14
    cutoff       <- now - as.difftime(window_days, units="days")
    recent       <- d[!is.na(d$timestamp) & d$timestamp >= cutoff, ]
    
    county_fc <- do.call(rbind, lapply(counties$name, function(co) {
      h  <- recent[recent$county == co, ]
      all_co <- d[d$county == co, ]
      
      # Historical baseline
      n_recent    <- nrow(h)
      avg_risk    <- if (n_recent > 0) round(mean(h$risk_score, na.rm=TRUE), 0) else 0
      avg_ncic    <- if (n_recent > 0) round(mean(h$ncic_level,  na.rm=TRUE), 1) else 0
      n_high      <- sum(h$ncic_level >= 4, na.rm=TRUE)
      n_s13       <- sum(isTRUE(h$section_13), na.rm=TRUE)
      top_platform<- if (n_recent > 0) names(sort(table(h$platform), decreasing=TRUE))[1] else "—"
      top_category<- if (n_recent > 0) names(sort(table(h$category), decreasing=TRUE))[1] else "—"
      
      # Trend: compare last 7 days vs prior 7 days
      w1 <- h[h$timestamp >= now - as.difftime(7,  units="days"), ]
      w2 <- h[h$timestamp >= now - as.difftime(14, units="days") &
                h$timestamp <  now - as.difftime(7,  units="days"), ]
      risk_w1 <- if (nrow(w1) > 0) mean(w1$risk_score, na.rm=TRUE) else NA
      risk_w2 <- if (nrow(w2) > 0) mean(w2$risk_score, na.rm=TRUE) else NA
      trend   <- if (!is.na(risk_w1) && !is.na(risk_w2) && risk_w2 > 0)
        round((risk_w1 - risk_w2) / risk_w2 * 100, 0)
      else 0
      
      # Velocity: avg new signals per day in last 7d
      velocity <- if (nrow(w1) > 0) round(nrow(w1) / 7, 1) else 0
      
      # Escalation score: weighted combination for forecast
      escalation_score <- min(100, round(
        avg_risk * 0.35 +
          avg_ncic * 8   +
          n_high   * 3   +
          max(0, trend)  * 0.5 +
          velocity       * 4
      ))
      
      # Forecast level
      forecast_level <- if (escalation_score >= 70) "CRITICAL"  else
        if (escalation_score >= 50) "HIGH"       else
          if (escalation_score >= 30) "ELEVATED"   else
            if (escalation_score >  0)  "MONITORED"  else "STABLE"
      
      # Drivers
      drivers <- c(
        if (avg_ncic >= 3.5)  "High avg NCIC level"        else NULL,
        if (n_high >= 3)      paste0(n_high," L4+ cases")  else NULL,
        if (trend  > 20)      paste0("↑",trend,"% trend")  else NULL,
        if (velocity > 2)     paste0(velocity," signals/day") else NULL,
        if (n_s13  > 0)       paste0(n_s13," S13 cases")   else NULL
      )
      
      data.frame(
        county=co, n_recent=n_recent, avg_risk=avg_risk, avg_ncic=avg_ncic,
        n_high=n_high, n_s13=n_s13, trend=trend, velocity=velocity,
        escalation_score=escalation_score, forecast_level=forecast_level,
        top_platform=top_platform, top_category=top_category,
        drivers=if(length(drivers)>0) paste(drivers,collapse=" · ") else "No strong signals",
        stringsAsFactors=FALSE
      )
    }))
    
    # ── Colour palette for forecast levels ────────────────────
    fc_col <- function(lvl) switch(lvl,
                                   CRITICAL  = "#7b0000",
                                   HIGH      = "#dc3545",
                                   ELEVATED  = "#fd7e14",
                                   MONITORED = "#ffc107",
                                   STABLE    = "#198754",
                                   "#dee2e6"
    )
    
    fc_pal <- colorNumeric(
      palette = c("#198754","#ffc107","#fd7e14","#dc3545","#7b0000"),
      domain  = c(0, 100), na.color="#dee2e6"
    )
    
    # ── Build sf_data for tmap ────────────────────────────────
    sf_fc <- KENYA_SF |>
      left_join(county_fc, by=c("name"="county")) |>
      mutate(
        escalation_score = ifelse(is.na(escalation_score), 0L, escalation_score),
        forecast_level   = ifelse(is.na(forecast_level), "STABLE", forecast_level),
        n_recent         = ifelse(is.na(n_recent), 0L, n_recent),
        avg_risk         = ifelse(is.na(avg_risk),  0L, avg_risk),
        drivers          = ifelse(is.na(drivers), "Insufficient data", drivers)
      )
    
    # ── Summary KPIs ─────────────────────────────────────────
    n_critical <- sum(county_fc$forecast_level == "CRITICAL",  na.rm=TRUE)
    n_high_fc  <- sum(county_fc$forecast_level == "HIGH",      na.rm=TRUE)
    n_elevated <- sum(county_fc$forecast_level == "ELEVATED",  na.rm=TRUE)
    top_county <- county_fc$county[which.max(county_fc$escalation_score)]
    
    tagList(
      # Header
      tags$div(style="margin-bottom:16px;",
               tags$h5(style="font-weight:700;color:#1a1a2e;margin-bottom:4px;",
                       tagList(bs_icon("graph-up-arrow")," 14-Day Risk Forecast")),
               tags$p(style="font-size:12px;color:#6c757d;margin:0;",
                      paste0("Based on signals from last 30 days · Generated ",
                             format(now,"%d %b %Y %H:%M")," EAT · County-level escalation model"))
      ),
      
      # KPI row
      tags$div(style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px;",
               tags$div(style="background:#7b0000;border-radius:8px;padding:10px 14px;color:#fff;",
                        tags$div(style="font-size:10px;font-weight:700;text-transform:uppercase;opacity:.85;","Critical Risk"),
                        tags$div(style="font-size:28px;font-weight:800;", n_critical),
                        tags$div(style="font-size:10px;opacity:.75;","counties in next 14d")),
               tags$div(style="background:#dc3545;border-radius:8px;padding:10px 14px;color:#fff;",
                        tags$div(style="font-size:10px;font-weight:700;text-transform:uppercase;opacity:.85;","High Risk"),
                        tags$div(style="font-size:28px;font-weight:800;", n_high_fc),
                        tags$div(style="font-size:10px;opacity:.75;","counties")),
               tags$div(style="background:#fd7e14;border-radius:8px;padding:10px 14px;color:#fff;",
                        tags$div(style="font-size:10px;font-weight:700;text-transform:uppercase;opacity:.85;","Elevated"),
                        tags$div(style="font-size:28px;font-weight:800;", n_elevated),
                        tags$div(style="font-size:10px;opacity:.75;","counties")),
               tags$div(style="background:#0066cc;border-radius:8px;padding:10px 14px;color:#fff;",
                        tags$div(style="font-size:10px;font-weight:700;text-transform:uppercase;opacity:.85;","Top Watch County"),
                        tags$div(style="font-size:16px;font-weight:800;line-height:1.3;margin-top:4px;", top_county),
                        tags$div(style="font-size:10px;opacity:.75;","highest escalation score"))
      ),
      
      layout_columns(col_widths=c(8,4),
                     
                     # ── Forecast map ────────────────────────────────────────
                     card(full_screen=TRUE,
                          card_header(tagList(bs_icon("map")," Forecast Risk Map · hover for details · click for full analysis")),
                          tags$div(style="height:460px;",
                                   tmapOutput("forecast_map", height="460px")
                          )
                     ),
                     
                     # ── County ranking table ─────────────────────────────────
                     card(
                       card_header(tagList(bs_icon("sort-down")," County Risk Ranking")),
                       tags$div(style="max-height:480px;overflow-y:auto;",
                                lapply(seq_len(nrow(county_fc[order(-county_fc$escalation_score),])), function(i) {
                                  row <- county_fc[order(-county_fc$escalation_score),][i,]
                                  col <- fc_col(row$forecast_level)
                                  tags$div(
                                    style=paste0("display:flex;align-items:center;gap:8px;padding:7px 10px;",
                                                 "border-bottom:1px solid #f0f0f0;"),
                                    tags$div(style=paste0("min-width:22px;font-size:11px;font-weight:700;color:#6c757d;"),
                                             paste0("#",i)),
                                    tags$div(style="flex:1;",
                                             tags$div(style="font-size:12px;font-weight:700;color:#1a1a2e;", row$county),
                                             tags$div(style="font-size:10px;color:#6c757d;", row$drivers)
                                    ),
                                    tags$div(
                                      tags$span(style=paste0("background:",col,";color:",
                                                             if(row$forecast_level=="MONITORED") "#1a1a2e" else "#fff",
                                                             ";border-radius:4px;padding:2px 7px;font-size:10px;font-weight:700;"),
                                                row$forecast_level),
                                      tags$div(style=paste0("font-size:13px;font-weight:800;color:",col,
                                                            ";text-align:right;margin-top:1px;"),
                                               row$escalation_score)
                                    )
                                  )
                                })
                       )
                     )
      ),
      
      # ── Methodology note ──────────────────────────────────────
      tags$div(style="margin-top:12px;background:#f8f9fa;border-radius:8px;padding:10px 14px;font-size:11px;color:#6c757d;border-left:3px solid #0066cc;",
               tags$strong("Forecast Methodology: "),
               "Escalation score = 0.35×avg_risk + 8×avg_NCIC + 3×L4+_count + 0.5×7d_trend% + 4×velocity(signals/day). ",
               "Forecast window: 14 days. Training window: 30 days of historical signals. ",
               "This is a statistical projection — all forecasts require officer review before operational action."
      )
    )
  })
  
  # ── Forecast map renderTmap (cannot nest inside renderUI) ──────
  output$forecast_map <- renderTmap({
    d      <- rv$cases
    now    <- Sys.time()
    cutoff <- now - as.difftime(30, units="days")
    recent <- d[!is.na(d$timestamp) & d$timestamp >= cutoff, ]
    
    county_fc <- do.call(rbind, lapply(counties$name, function(co) {
      h            <- recent[recent$county == co, ]
      n_recent     <- nrow(h)
      avg_risk     <- if (n_recent>0) round(mean(h$risk_score, na.rm=TRUE),0) else 0
      avg_ncic     <- if (n_recent>0) round(mean(h$ncic_level,  na.rm=TRUE),1) else 0
      n_high       <- sum(h$ncic_level >= 4, na.rm=TRUE)
      n_s13        <- sum(isTRUE(h$section_13), na.rm=TRUE)
      top_platform <- if (n_recent>0) names(sort(table(h$platform),decreasing=TRUE))[1] else "—"
      top_category <- if (n_recent>0) names(sort(table(h$category),decreasing=TRUE))[1] else "—"
      w1      <- h[h$timestamp >= now - as.difftime(7,  units="days"), ]
      w2      <- h[h$timestamp >= now - as.difftime(14, units="days") &
                     h$timestamp <  now - as.difftime(7,  units="days"), ]
      r1      <- if (nrow(w1)>0) mean(w1$risk_score,na.rm=TRUE) else NA
      r2      <- if (nrow(w2)>0) mean(w2$risk_score,na.rm=TRUE) else NA
      trend   <- if (!is.na(r1)&&!is.na(r2)&&r2>0) round((r1-r2)/r2*100,0) else 0
      velocity<- if (nrow(w1)>0) round(nrow(w1)/7,1) else 0
      esc     <- min(100,round(avg_risk*0.35+avg_ncic*8+n_high*3+max(0,trend)*0.5+velocity*4))
      lvl     <- if(esc>=70)"CRITICAL" else if(esc>=50)"HIGH" else
        if(esc>=30)"ELEVATED" else if(esc>0)"MONITORED" else "STABLE"
      drv <- paste(c(if(avg_ncic>=3.5)"High avg NCIC" else NULL,
                     if(n_high>=3) paste0(n_high," L4+") else NULL,
                     if(trend>20)  paste0("\u2191",trend,"% trend") else NULL,
                     if(velocity>2)paste0(velocity," sig/day") else NULL,
                     if(n_s13>0)  paste0(n_s13," S13") else NULL),
                   collapse=" \u00b7 ")
      if(nchar(drv)==0) drv <- "No strong signals"
      data.frame(county=co,n_recent=n_recent,avg_risk=avg_risk,avg_ncic=avg_ncic,
                 n_high=n_high,n_s13=n_s13,trend=trend,velocity=velocity,
                 escalation_score=esc,forecast_level=lvl,
                 top_platform=top_platform,top_category=top_category,
                 drivers=drv,stringsAsFactors=FALSE)
    }))
    
    sf_fc <- KENYA_SF |>
      left_join(county_fc, by=c("name"="county")) |>
      mutate(
        escalation_score = ifelse(is.na(escalation_score), 0L, escalation_score),
        forecast_level   = ifelse(is.na(forecast_level), "STABLE", forecast_level),
        n_recent         = ifelse(is.na(n_recent), 0L, n_recent),
        avg_risk         = ifelse(is.na(avg_risk),  0L, avg_risk),
        drivers          = ifelse(is.na(drivers), "Insufficient data", drivers)
      )
    
    tm_shape(sf_fc) +
      tm_polygons(
        col        = "escalation_score",
        palette    = c("#198754","#ffc107","#fd7e14","#dc3545","#7b0000"),
        breaks     = c(0, 20, 40, 60, 80, 100),
        labels     = c("Stable","Monitored","Elevated","High","Critical"),
        colorNA    = "#dee2e6",
        textNA     = "Insufficient data",
        title      = "14-Day Forecast Risk",
        border.col = "#ffffff",
        border.lwd = 1.2,
        alpha      = 0.85,
        id         = "name",
        popup.vars = c(
          "Forecast Level"   = "forecast_level",
          "Escalation Score" = "escalation_score",
          "Signals (30d)"    = "n_recent",
          "Avg Risk Score"   = "avg_risk",
          "Avg NCIC Level"   = "avg_ncic",
          "L4+ Cases"        = "n_high",
          "Section 13"       = "n_s13",
          "Top Platform"     = "top_platform",
          "Top Category"     = "top_category",
          "Key Drivers"      = "drivers"
        )
      ) +
      tm_basemap("CartoDB.Positron")
  })
  
  output$rep_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui())
    d        <- rv$cases
    n_total  <- nrow(d)
    n_s13    <- sum(d$section_13, na.rm=TRUE)
    n_val    <- sum(!is.na(d$validated_by))
    n_county <- length(unique(d$county))
    tagList(
      # Header + live summary
      tags$div(style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px;flex-wrap:wrap;gap:12px;",
               tags$div(
                 tags$h5(tagList(bs_icon("file-earmark-bar-graph")," Intelligence Reports"),
                         style="margin:0 0 4px;font-weight:700;color:#1a1a2e;"),
                 tags$p(style="font-size:12px;color:#6c757d;margin:0;",
                        paste0("Live dataset: ", formatC(n_total,format="d",big.mark=","), " signals · ",
                               n_s13, " Section 13 cases · ", n_val, " validated · ", n_county, " counties"))
               ),
               tags$div(style="font-size:11px;color:#6c757d;font-style:italic;padding-top:6px;",
                        "All exports reflect the current live dataset as at ", format(Sys.time(), "%d %b %Y %H:%M"), " EAT")
      ),
      layout_columns(col_widths=c(3,3,3,3),
                     card(style="border-top:3px solid #0066cc;",
                          card_header(tagList(bs_icon("lightning")," Quick Briefing")),
                          tags$p(style="font-size:12px;color:#6c757d;margin-bottom:12px;",
                                 "24-hour snapshot: top incidents, county summary, and NCIC level distribution."),
                          downloadButton("dl_quick",tagList(bs_icon("download")," Export Excel"),
                                         style="background:#0066cc;color:#fff;border:none;border-radius:6px;font-weight:600;width:100%;")),
                     card(style="border-top:3px solid #fd7e14;",
                          card_header(tagList(bs_icon("table")," Detailed Report")),
                          tags$p(style="font-size:12px;color:#6c757d;margin-bottom:12px;",
                                 "Full case breakdown with NCIC levels, risk formulas, signals, and officer notes."),
                          downloadButton("dl_det",tagList(bs_icon("download")," Export Excel"),
                                         style="background:#fd7e14;color:#fff;border:none;border-radius:6px;font-weight:600;width:100%;")),
                     card(style="border-top:3px solid #dc3545;",
                          card_header(tagList(bs_icon("file-earmark-text")," NCIC Legal Report")),
                          tags$p(style="font-size:12px;color:#6c757d;margin-bottom:12px;",
                                 paste0("Section 13 cases and escalation queue for DCI/prosecution. Currently: ", n_s13, " S13 cases.")),
                          downloadButton("dl_ncic",tagList(bs_icon("download")," Export Excel"),
                                         style="background:#dc3545;color:#fff;border:none;border-radius:6px;font-weight:600;width:100%;")),
                     card(style="border-top:3px solid #198754;",
                          card_header(tagList(bs_icon("map")," County Report")),
                          tags$p(style="font-size:12px;color:#6c757d;margin-bottom:12px;",
                                 paste0("Per-county signal distribution, avg NCIC level, and risk bands. ", n_county, " counties covered.")),
                          downloadButton("dl_co",tagList(bs_icon("download")," Export Excel"),
                                         style="background:#198754;color:#fff;border:none;border-radius:6px;font-weight:600;width:100%;"))
      ),
      # Live validated export
      card(style="border-top:3px solid #7c3aed;margin-top:4px;",
           card_header(tagList(bs_icon("shield-check")," Validated Cases Export")),
           tags$div(style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;",
                    tags$p(style="font-size:12px;color:#6c757d;margin:0;",
                           paste0(n_val, " officer-validated cases — includes NCIC override, action taken, officer name, and notes.")),
                    downloadButton("dl_val",tagList(bs_icon("download")," Export Validated Cases"),
                                   style="background:#7c3aed;color:#fff;border:none;border-radius:6px;font-weight:600;padding:8px 14px;")
           )
      )
    )
  })
  
  # ── Validated cases download ─────────────────────────────────
  
  output$dl_val <- downloadHandler(
    filename=function() paste0("EWS_Validated_",Sys.Date(),".xlsx"),
    content=function(file) {
      d <- rv$cases[!is.na(rv$cases$validated_by),
                    c("case_id","county","platform","tweet_text","ncic_level","ncic_label",
                      "section_13","confidence","risk_score","risk_level","risk_formula",
                      "category","language","target_group","validated_by","validated_at",
                      "action_taken","timestamp_chr")]
      names(d) <- c("Case ID","County","Platform","Content","NCIC Level","NCIC Label",
                    "Section 13","Confidence","Risk Score","Risk Level","Risk Formula",
                    "Category","Language","Target Group","Validated By","Validated At",
                    "Action Taken","Timestamp")
      write_xlsx(d,file)
    }
  )
  
  output$dl_quick <- downloadHandler(
    filename=function() paste0("EWS_QuickBrief_",Sys.Date(),".xlsx"),
    content=function(file) {
      d <- rv$cases[rv$cases$ncic_level>=3,
                    c("case_id","county","platform","tweet_text","ncic_level","ncic_label",
                      "section_13","risk_score","category","target_group","timestamp_chr")]
      names(d) <- c("ID","County","Platform","Content","NCIC Level","NCIC Label",
                    "S13","Risk","Category","Target","Timestamp")
      s <- rv$cases |> group_by(county) |>
        summarise(Total=n(),L4_Hate=sum(ncic_level==4),L5_Toxic=sum(ncic_level==5),
                  AvgRisk=round(mean(risk_score),0),S13=sum(section_13,na.rm=TRUE),
                  .groups="drop") |>
        left_join(counties,by=c("county"="name")) |>
        select(County=county,Lat=lat,Lng=lng,Total,L4_Hate,L5_Toxic,AvgRisk,S13)
      write_xlsx(list(Incidents=d,`County Summary`=s),file)
    }
  )
  
  output$dl_det <- downloadHandler(
    filename=function() paste0("EWS_Detailed_",Sys.Date(),".xlsx"),
    content=function(file) {
      d <- rv$cases[,c("case_id","county","platform","handle","tweet_text","ncic_level",
                       "ncic_label","section_13","confidence","risk_score","risk_level",
                       "risk_formula","category","language","target_group","signals",
                       "validated_by","validated_at","action_taken","timestamp_chr")]
      names(d) <- c("ID","County","Platform","Handle","Content","NCIC Level","NCIC Label",
                    "S13","Confidence","Risk Score","Risk Level","Risk Formula","Category",
                    "Language","Target Group","Signals","Validated By","Validated At",
                    "Action Taken","Timestamp")
      write_xlsx(list(`All Cases`=d),file)
    }
  )
  
  output$dl_ncic <- downloadHandler(
    filename=function() paste0("EWS_NCIC_Legal_",Sys.Date(),".xlsx"),
    content=function(file) {
      # Sheet 1: S13 cases ready for legal action
      s13 <- rv$cases[rv$cases$section_13==TRUE & !is.na(rv$cases$validated_by),
                      c("case_id","county","platform","handle","tweet_text",
                        "ncic_level","ncic_label","risk_score","category",
                        "target_group","validated_by","action_taken","timestamp_chr")]
      names(s13) <- c("ID","County","Platform","Handle","Content","NCIC Level",
                      "NCIC Label","Risk","Category","Target","Validated By","Action","Timestamp")
      
      # Sheet 2: Escalation queue (L4+ not yet validated)
      esc <- rv$cases[rv$cases$ncic_level>=4 & is.na(rv$cases$validated_by),
                      c("case_id","county","ncic_level","ncic_label","tweet_text",
                        "target_group","risk_score","timestamp_chr")]
      esc <- esc[order(-esc$ncic_level,-esc$risk_score),]
      names(esc) <- c("ID","County","NCIC Level","NCIC Label","Content",
                      "Target Group","Risk","Timestamp")
      
      # Sheet 3: Officer disagreements log
      dis <- load_disagreements()
      
      # Sheet 4: Officer activity
      act <- rv$cases[!is.na(rv$cases$validated_by),] |>
        group_by(`Officer`=validated_by) |>
        summarise(Total=n(),Confirmed=sum(action_taken=="CONFIRMED",na.rm=TRUE),
                  Escalated=sum(action_taken=="ESCALATED",na.rm=TRUE),
                  Cleared=sum(action_taken=="CLEARED",na.rm=TRUE),
                  `Avg Risk Confirmed`=round(mean(risk_score[action_taken=="CONFIRMED"],na.rm=TRUE),0),
                  .groups="drop")
      
      write_xlsx(list(`Section 13 Cases`=s13,`Escalation Queue`=esc,
                      `Officer Corrections`=dis,`Officer Activity`=act),file)
    }
  )
  
  output$dl_co <- downloadHandler(
    filename=function() paste0("EWS_County_",Sys.Date(),".xlsx"),
    content=function(file) {
      d <- rv$cases |> group_by(county) |>
        summarise(Total=n(),L3_Dehuman=sum(ncic_level==3),L4_Hate=sum(ncic_level==4),
                  L5_Toxic=sum(ncic_level==5),S13=sum(section_13,na.rm=TRUE),
                  AvgRisk=round(mean(risk_score),0),
                  Validated=sum(!is.na(validated_by)),.groups="drop") |>
        left_join(counties,by=c("county"="name")) |>
        mutate(RiskBand=case_when(AvgRisk>=65~"HIGH",AvgRisk>=35~"MEDIUM",TRUE~"LOW")) |>
        select(County=county,Lat=lat,Lng=lng,Total,L3_Dehuman,L4_Hate,L5_Toxic,
               S13,AvgRisk,RiskBand,Validated)
      write_xlsx(list(`County Analysis`=d),file)
    }
  )
  
  # ── Concept note download — renders PDF server-side ────────────
  output$dl_concept_pdf <- downloadHandler(
    filename = function() "Radicalisation_Signals_Concept_Note.pdf",
    content  = function(file) {
      # Locate the Rmd source next to app.R
      rmd_src <- file.path(getwd(), "concept_note.Rmd")
      if (!file.exists(rmd_src))
        stop("concept_note.Rmd not found in app directory")
      
      # Render into a temp directory (never write into getwd())
      tmp_dir <- tempdir()
      tmp_rmd <- file.path(tmp_dir, "concept_note.Rmd")
      file.copy(rmd_src, tmp_rmd, overwrite=TRUE)
      
      out_pdf <- rmarkdown::render(
        input       = tmp_rmd,
        output_format = rmarkdown::pdf_document(
          toc            = FALSE,
          number_sections= FALSE,
          latex_engine   = "xelatex"
        ),
        output_file = "concept_note.pdf",
        output_dir  = tmp_dir,
        quiet       = TRUE
      )
      
      file.copy(out_pdf, file)
    },
    contentType = "application/pdf"
  )
  
  # ── Auth ───────────────────────────────────────────────────
  observeEvent(input$auth_submit, {
    nm <- trimws(input$auth_name %||% "")
    if (nchar(nm)==0) {
      shinyjs::html("auth_error","❌ Please enter your name.")
      return()
    }
    if (isTRUE(input$auth_password==OFFICER_PASSWORD)) {
      rv$authenticated <- TRUE
      rv$officer_name  <- nm
    } else {
      shinyjs::html("auth_error","❌ Invalid access code.")
    }
  })
  
} # end server

shinyApp(ui, server)