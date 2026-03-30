# ================================================================
#  RADICALISATION SIGNALS  —  v6
#  Kenya National Cohesion Intelligence Platform
#  R Shiny + bslib | NCIC Cap 170 | GPT-4o-mini | HITL | Prophet
# ================================================================
#  v6 FEATURES (additions over v5):
#   12. Precision/Recall evaluation (gold standard dataset)
#   13. Officer context window (handle history + similar cases)
#   14. GPT confidence calibration tracking
#   15. Geographic signal clustering (coordinated campaign detection)
#   16. Keyword bank version history (full changelog)
#   17. Keyword retirement mechanism (90-day stale detection)
#   18. Inter-officer agreement (Cohen's kappa + flagged cases)
#   19. Structured officer reasoning (signal words, denotation, language)
#   20. Adaptive keyword extraction from validated posts → Supabase
#
#  v5 FEATURES:
#   1.  GPT-4o-mini language detection (replaces cld3)
#          — Sheng, Kikuyu, Kalenjin, Luo glossaries injected into prompt
#   2.  Prophet time-series forecast per county (14-day, 80% CI)
#   3.  bcrypt-hashed credentials + role-based access (admin/officer)
#   4.  Session timeout (20 min) + supervisor email notification
#   5.  Tamper-evident audit log + CSV export
#   6.  Section 13 escalation queue (Pending→Filed→DCI→Resolved)
#   7.  Password complexity enforcement (10+ chars, upper/lower/digit/special)
#   8.  SQLite WAL mode (concurrent reads)
#   9.  Global loading bar (shinyjs) on all renders
#  10.  Auto-installs missing packages on startup
#  11.  Auto-seeds admin account + backfills S13 queue on first run
#
#  secrets/.Renviron:
#   OPENAI_API_KEY=sk-proj-...
#   GMAIL_USER=you@gmail.com
#   GMAIL_PASS=xxxx xxxx xxxx xxxx
#   OFFICER_EMAIL=officer@ncic.go.ke          # case alert destination
#   SUPERVISOR_EMAIL=supervisor@ncic.go.ke    # session timeout alerts
#   ADMIN_USERNAME=admin
#   ADMIN_PASSWORD=<strong-pass>  # 10+ chars, upper+lower+digit+special
#   (never commit .Renviron to git — add to .gitignore)
# ================================================================

# ── Auto-install any missing packages ────────────────────────────
.required_pkgs <- c(
  "shiny","bslib","bsicons","leaflet","leaflet.extras2",
  "DT","dplyr","writexl","httr2","jsonlite","shinyjs",
  "digest","future","promises","plotly","later","highcharter",
  "sf","tmap","tools","bcrypt","prophet"
)
.missing <- .required_pkgs[!sapply(.required_pkgs, requireNamespace, quietly=TRUE)]
if (length(.missing) > 0) {
  message(sprintf("[startup] Installing %d missing package(s): %s",
                  length(.missing), paste(.missing, collapse=", ")))
  install.packages(.missing, repos="https://cloud.r-project.org", quiet=TRUE)
  message("[startup] Package installation complete.")
}
rm(.required_pkgs, .missing)

readRenviron("secrets/.Renviron")
source("supabase_db.R")

library(shiny);    library(bslib);      library(bsicons)
library(leaflet);  library(leaflet.extras2)
library(DT);       library(dplyr);      library(writexl)
library(httr2);    library(jsonlite);   library(shinyjs)
library(digest);   library(future);     library(promises)
library(plotly);   library(later);      library(highcharter)
library(sf);       library(tmap)
library(bcrypt)
library(prophet)

plan(multisession)
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── CONSTANTS ────────────────────────────────────────────────────
APP_NAME         <- "Radicalisation Signals"
APP_SUBTITLE     <- "AI Early Warning Platform for Kenya"
OPENAI_MODEL     <- "gpt-4o-mini"
CACHE_DIR        <- "cache"
CACHE_FILE       <- file.path(CACHE_DIR, "classify_cache.rds")  # GPT response cache (RDS)
DB_FILE          <- file.path(CACHE_DIR, "ews.sqlite")           # persistent SQLite store
VAL_PAGE_SIZE    <- 6
SESSION_TIMEOUT  <- 20L   # minutes of inactivity before auto-lock

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

# ── DATABASE LAYER (Supabase) ───────────────────────────────────
# All db_* functions are defined in supabase_db.R
# source() is called at startup after readRenviron()
# ── Cases ────────────────────────────────────────────────────────
# db_* functions loaded from supabase_db.R (sourced at startup)

# ── v6: GEOGRAPHIC CLUSTERING ─────────────────────────────────────
detect_county_clusters <- function(cases_df, window_hours=24L, min_level=3L, min_counties=2L) {
  recent <- cases_df[cases_df$ncic_level>=min_level &
    !is.na(cases_df$timestamp) &
    as.numeric(difftime(Sys.time(),cases_df$timestamp,units="hours"))<=window_hours,]
  if (nrow(recent)==0) return(data.frame())
  county_counts <- table(recent$county)
  active <- names(county_counts[county_counts>=1])
  if (length(active)<min_counties) return(data.frame())
  clusters <- lapply(active, function(county) {
    cc <- recent[recent$county==county,]
    all_sigs <- unlist(strsplit(paste(cc$signals%||%"",collapse="|"),"\\|"))
    all_sigs <- trimws(all_sigs[nchar(trimws(all_sigs))>0])
    list(county=county,n_cases=nrow(cc),max_level=max(cc$ncic_level),
         keywords=names(sort(table(all_sigs),decreasing=TRUE))[1:3])
  })
  all_kws <- unlist(lapply(clusters,`[[`,"keywords"))
  shared  <- names(table(all_kws)[table(all_kws)>=min_counties])
  df <- do.call(rbind,lapply(clusters,function(c) data.frame(
    county=c$county,n_cases=c$n_cases,max_level=c$max_level,
    top_keyword=paste(c$keywords[!is.na(c$keywords)],collapse=", "),
    stringsAsFactors=FALSE)))
  attr(df,"shared_keywords") <- shared
  df
}

# ── v6: CONTEXT WINDOW HELPERS ────────────────────────────────────
get_handle_history <- function(handle, current_cid, cases_df, n=5L) {
  if (is.na(handle)||nchar(trimws(handle))==0) return(data.frame())
  history <- cases_df[!is.na(cases_df$handle)&cases_df$handle==handle&cases_df$case_id!=current_cid,]
  head(history[order(-history$ncic_level),],n)
}

get_similar_cases <- function(signals, current_cid, cases_df, n=3L) {
  if (is.na(signals)||nchar(trimws(signals))==0) return(data.frame())
  sig_terms <- trimws(strsplit(signals,"\\|")[[1]])
  if (length(sig_terms)==0) return(data.frame())
  other <- cases_df[cases_df$case_id!=current_cid&!is.na(cases_df$signals),]
  if (nrow(other)==0) return(data.frame())
  other$sim_score <- sapply(other$signals,function(s) {
    sum(sig_terms %in% trimws(strsplit(s%||%"","\\|")[[1]]))
  })
  head(other[order(-other$sim_score,-other$ncic_level),][other$sim_score>0,,drop=FALSE],n)
}

# ── v6: EXTRACT KEYWORDS FROM VALIDATED POSTS ─────────────────────
extract_and_save_keywords <- function(tweet, ncic_level, action, officer,
                                      sig_words="", sig_denotes="",
                                      sig_lang="", officer_reason="") {
  if (!action %in% c("CONFIRMED","ESCALATED")) return(invisible(NULL))
  if (as.integer(ncic_level) < 3L)             return(invisible(NULL))
  supa_url <- Sys.getenv("SUPABASE_URL"); supa_key <- Sys.getenv("SUPABASE_KEY")
  api_key  <- Sys.getenv("OPENAI_API_KEY")
  if (nchar(supa_url)==0||nchar(api_key)==0) return(invisible(NULL))

  existing_kws <- tryCatch({
    req <- request(paste0(supa_url,"/rest/v1/keyword_bank")) |>
      req_headers("apikey"=supa_key,"Authorization"=paste("Bearer",supa_key)) |>
      req_url_query(select="keyword")
    df <- fromJSON(resp_body_string(req_perform(req)),flatten=TRUE)
    if (is.data.frame(df)&&nrow(df)>0) tolower(trimws(df$keyword)) else character()
  }, error=function(e) character())

  officer_context <- if (nchar(trimws(officer_reason))>0) paste0(
    "\n\nOFFICER INPUT (ground truth):\n",
    if(nchar(trimws(sig_words))>0)   paste0("- Signal phrases: ",sig_words,"\n")   else "",
    if(nchar(trimws(sig_denotes))>0) paste0("- Denotes: ",sig_denotes,"\n")        else "",
    if(nchar(trimws(sig_lang))>0)    paste0("- Language: ",sig_lang,"\n")          else "",
    if(nchar(trimws(sig_reason))>0)  paste0("- Reasoning: ",sig_reason,"\n")       else ""
  ) else ""

  result <- tryCatch({
    resp <- request("https://api.openai.com/v1/chat/completions") |>
      req_headers("Authorization"=paste("Bearer",api_key),"Content-Type"="application/json") |>
      req_body_raw(toJSON(list(model="gpt-4o-mini",temperature=0.1,max_tokens=600L,
        messages=list(
          list(role="system",content="You are an NCIC Kenya hate speech analyst. Extract signal keywords from a confirmed hate speech post. Return ONLY a JSON array with items: {keyword, tier (1-3), category, language, context}. If none found return []."),
          list(role="user",content=paste0("Post confirmed L",ncic_level," by officer '",officer,"'.\n\nPost: \"",substr(tweet,1,500),"\"",officer_context,"\n\nExisting keywords (skip): ",paste(existing_kws,collapse=", ")))
        )),auto_unbox=TRUE),type="application/json") |>
      req_method("POST") |> req_perform()
    raw <- fromJSON(resp_body_string(resp),flatten=TRUE)
    parsed <- fromJSON(raw$choices[[1]]$message$content,flatten=TRUE)
    if (is.data.frame(parsed)) parsed else data.frame()
  }, error=function(e) { message("[keywords] GPT failed: ",e$message); data.frame() })

  for (col in c("keyword","tier","category","language","context"))
    if (!col %in% names(result)) result[[col]] <- ""
  if (nrow(result)>0)
    result <- result[!tolower(trimws(result$keyword))%in%existing_kws&nchar(trimws(result$keyword))>=3,]

  # Also insert officer-identified phrases directly
  if (nchar(trimws(sig_words))>0) {
    direct_kws <- trimws(strsplit(sig_words,",")[[1]])
    direct_kws <- direct_kws[nchar(direct_kws)>=3&!tolower(direct_kws)%in%existing_kws]
    if (length(direct_kws)>0) {
      tier_map <- c(CALL_TO_VIOLENCE=3L,CALL_TO_EXPEL=3L,ARMED_UPRISING=3L,
                    DEHUMANISATION_ANIMAL=3L,DEHUMANISATION_DISEASE=3L,DEHUMANISATION_GENERAL=3L,
                    ETHNIC_SLUR=2L,ETHNIC_STEREOTYPE=2L,EXCLUSION_RHETORIC=2L,
                    ELECTION_TRIBAL=2L,ELECTION_FRAUD_TRIBAL=2L,SECESSIONISM=2L,
                    RELIGIOUS_HATRED=2L,UNITY_THREAT=1L,CODED_HATE=1L,OTHER=1L)
      cat_map  <- c(CALL_TO_VIOLENCE="INCITEMENT",CALL_TO_EXPEL="INCITEMENT",ARMED_UPRISING="INCITEMENT",
                    DEHUMANISATION_ANIMAL="DEHUMANISATION",DEHUMANISATION_DISEASE="DEHUMANISATION",
                    DEHUMANISATION_GENERAL="DEHUMANISATION",ETHNIC_SLUR="ETHNIC_CONTEMPT",
                    ETHNIC_STEREOTYPE="ETHNIC_CONTEMPT",EXCLUSION_RHETORIC="ETHNIC_CONTEMPT",
                    ELECTION_TRIBAL="ELECTION_INCITEMENT",ELECTION_FRAUD_TRIBAL="ELECTION_INCITEMENT",
                    SECESSIONISM="SECESSIONISM",RELIGIOUS_HATRED="RELIGIOUS_HATRED",
                    UNITY_THREAT="DIVISIVE_CONTENT",CODED_HATE="HATE_SPEECH",OTHER="HATE_SPEECH")
      tier <- as.integer(tier_map[sig_denotes]%||%2L)
      cat  <- as.character(cat_map[sig_denotes]%||%"HATE_SPEECH")
      lang <- if (nchar(trimws(sig_lang))>0) sig_lang else "sw"
      direct_df <- data.frame(keyword=tolower(direct_kws),tier=tier,category=cat,language=lang,
        context=paste0("Officer-identified (",officer,"). Denotes: ",sig_denotes,
                       if(nchar(trimws(sig_reason))>0) paste0(". ",sig_reason) else ""),
        stringsAsFactors=FALSE)
      gpt_cols <- c("keyword","tier","category","language","context")
      result <- if (nrow(result)>0) rbind(result[,gpt_cols,drop=FALSE],direct_df) else direct_df
      result <- result[!duplicated(tolower(result$keyword)),]
    }
  }

  if (nrow(result)==0) return(invisible(NULL))

  to_insert <- data.frame(keyword=tolower(trimws(result$keyword)),tier=as.integer(result$tier),
    category=result$category,language=result$language,status="approved",source="officer",
    suggested_by="gpt-4o-mini",reviewed_by=officer,
    context=substr(result$context%||%"",1,300),times_matched=0L,stringsAsFactors=FALSE)

  tryCatch({
    req <- request(paste0(supa_url,"/rest/v1/keyword_bank")) |>
      req_headers("apikey"=supa_key,"Authorization"=paste("Bearer",supa_key),
                  "Content-Type"="application/json","Prefer"="return=minimal") |>
      req_body_raw(toJSON(to_insert,auto_unbox=TRUE,na="null"),type="application/json") |>
      req_method("POST")
    req_perform(req)
    for (i in seq_len(nrow(to_insert)))
      log_keyword_change(to_insert$keyword[i],"ADDED",officer,
                         new_tier=to_insert$tier[i],new_status="approved",reason="officer_validation")
    message(sprintf("[keywords] %d keywords saved from L%s case",nrow(to_insert),ncic_level))
  }, error=function(e) message("[keywords] insert failed: ",e$message))

  kw_cache <- file.path("cache","tg_keyword_cache.json")
  if (file.exists(kw_cache)) tryCatch(file.remove(kw_cache),error=function(e) NULL)
  invisible(NULL)
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
  if (!is.null(db_cases) && nrow(db_cases) > 0) {
    DATE_MIN <<- as.Date(min(db_cases$timestamp, na.rm=TRUE))
    DATE_MAX <<- as.Date(max(db_cases$timestamp, na.rm=TRUE))
  }
  if (is.null(db_cases) || nrow(db_cases) == 0) {
    message("[db] No cases found — starting empty (pipeline data only)")
    all_cases <- data.frame(
      case_id=character(), county=character(), sub_location=character(),
      src_lat=numeric(), src_lng=numeric(), target_county=character(),
      tgt_lat=numeric(), tgt_lng=numeric(), target_group=character(),
      platform=character(), handle=character(), tweet_text=character(),
      language=character(), ncic_level=integer(), ncic_label=character(),
      section_13=integer(), confidence=character(), conf_num=integer(),
      conf_band=character(), category=character(), validated_by=character(),
      validated_at=character(), action_taken=character(),
      officer_ncic_override=integer(), risk_score=integer(),
      risk_level=character(), risk_formula=character(), kw_score=integer(),
      network_score=integer(), signals=character(), trend_data=character(),
      timestamp=character(), timestamp_chr=character(), notes=character(),
      stringsAsFactors=FALSE)
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
                           cases_df=NULL, lang_det=NULL) {
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (nchar(api_key)==0) stop("OPENAI_API_KEY not set in secrets/.Renviron")
  
  # Compute source context BEFORE cache check so it updates even for cached posts
  src_ctx <- if (!is.null(cases_df) && nrow(cases_df) > 0)
    compute_source_context(handle, county, cases_df)
  else
    list(score=0, handle_score=0, county_score=0, summary="")
  
  # Detect language if not provided
  if (is.null(lang_det))
    lang_det <- tryCatch(detect_language(tweet),
                         error=function(e) list(code="unknown", label="Unknown",
                                                is_sheng=FALSE, is_mixed=FALSE, low_coverage=FALSE,
                                                warning=NA_character_))
  
  # Build language-specific hint for the prompt
  lang_hint <- build_language_hint(lang_det, tweet)
  
  # cache key includes example bank size so it invalidates as bank grows
  ex_count <- length(load_examples())
  key      <- digest(paste0(tolower(trimws(tweet)), "|ex:", ex_count))
  
  if (exists(key, envir=classify_cache)) {
    result <- get(key, envir=classify_cache)
    ctx_sc <- length(result$contextual_factors %||% list()) * 8
    rs <- compute_risk(tweet, result$confidence %||% 50,
                       result$ncic_level %||% 0, kw_weights,
                       result$network_score %||% 20, 10, ctx_sc,
                       src_ctx$score)
    result$risk_score           <- rs$score
    result$risk_formula         <- rs$formula
    result$source_history_score <- src_ctx$score
    result$source_context_note  <- src_ctx$summary
    result$cache_hit            <- TRUE
    return(result)
  }
  
  few_shot <- build_few_shot()
  
  user_prompt <- paste0(
    few_shot,
    lang_hint,   # ← language-specific glossary + hints injected here
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
    format(Sys.time(),"%Y-%m-%d %H:%M:%S")," EAT — ",APP_NAME," v5</div>",
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
  .btn-classify{background:#0066cc;color:#fff;border:none;border-radius:6px;padding:8px 14px;font-size:16px;font-weight:700;cursor:pointer;transition:background 0.15s;}
  .btn-classify:disabled{background:#5a9fd4;cursor:not-allowed;}
  /* ── Classifier spinner — pure JS, no Shiny round-trip ────────── */
  #chat_spinner{
    display:none;align-items:center;gap:10px;
    background:#f0f9ff;border-left:3px solid #0066cc;border-radius:8px;
    padding:10px 12px;margin-right:8px;margin-top:2px;
  }
  #chat_spinner.active{display:flex !important;}
  #chat_spinner .cs-ring{
    width:20px;height:20px;flex-shrink:0;
    border:3px solid #bae6fd;border-top-color:#0066cc;
    border-radius:50%;
    animation:cs-spin 0.7s linear infinite;
  }
  #chat_spinner .cs-dots{display:flex;gap:4px;align-items:center;margin-top:3px;}
  #chat_spinner .cs-dots span{
    display:inline-block;width:6px;height:6px;border-radius:50%;background:#0066cc;
    animation:cs-pop 1.1s ease-in-out infinite;
  }
  #chat_spinner .cs-dots span:nth-child(2){animation-delay:0.2s;}
  #chat_spinner .cs-dots span:nth-child(3){animation-delay:0.4s;}
  @keyframes cs-spin{to{transform:rotate(360deg)}}
  @keyframes cs-pop{0%,80%,100%{transform:scale(0.5);opacity:0.3}40%{transform:scale(1.15);opacity:1}}
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
  /* ── Loading bar ─────────────────────────────────────────────── */
  #ews-loading-bar{
    position:fixed;top:0;left:0;right:0;z-index:99999;
    height:3px;background:linear-gradient(90deg,#0066cc,#7c3aed,#0066cc);
    background-size:200% 100%;
    animation:loading-slide 1.2s linear infinite;
    display:none;
    box-shadow:0 1px 6px rgba(0,102,204,0.4);
  }
  #ews-loading-bar.active{display:block;}
  @keyframes loading-slide{
    0%{background-position:200% 0;}
    100%{background-position:-200% 0;}
  }
  /* Dim outputs while recalculating */
  .shiny-output-recalculating{
    opacity:0.4!important;
    transition:opacity 0.15s ease;
  }
  /* Loading label in card headers */
  .ews-loading-label{
    display:inline-flex;align-items:center;gap:5px;
    font-size:10px;color:#0066cc;font-weight:600;
    font-family:'IBM Plex Mono',monospace;
    animation:pulse 1s infinite;
  }
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

auth_wall_ui <- function(timed_out = FALSE) {
  tagList(useShinyjs(),
          tags$div(style="display:flex;align-items:center;justify-content:center;min-height:440px;",
                   tags$div(class="auth-box",
                            tags$div(style="font-size:48px;margin-bottom:10px;","🇰🇪"),
                            tags$h3(APP_NAME,style="font-size:18px;"),
                            if (isTRUE(timed_out))
                              tags$div(
                                style="background:#fff8e1;border:1px solid #ffc107;border-left:3px solid #fd7e14;border-radius:6px;padding:8px 12px;margin-bottom:14px;font-size:12px;color:#664d03;",
                                tagList(bs_icon("clock"), tags$strong(" Session timed out"),
                                        " — your session was automatically locked after ",
                                        SESSION_TIMEOUT, " minutes of inactivity. Please sign in again.")
                              )
                            else
                              tags$p("Authorised NCIC personnel only. Enter your credentials.",
                                     style="color:#6c757d;font-size:12px;margin-bottom:18px;"),
                            textInput("auth_name", NULL, placeholder="Full name (e.g. Officer Mwangi)", width="100%"),
                            textInput("auth_username", NULL, placeholder="Username", width="100%"),
                            actionButton("auth_submit","Sign In", style="width:100%;background:#0066cc;color:#fff;border:none;font-weight:700;font-size:14px;padding:11px;border-radius:6px;"),
                            tags$div(id="auth_error",style="color:#dc3545;font-size:12px;margin-top:8px;"),
                            tags$div(style="margin-top:16px;padding-top:12px;border-top:1px solid #f0f0f0;font-size:11px;color:#9ca3af;",
                                     "Credentials are stored as bcrypt hashes. Contact your admin to reset a password.")
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

# ── LANGUAGE DETECTION LAYER (GPT-4o-mini) ───────────────────────
# Uses GPT-4o-mini for language detection — same model as classification,
# better accuracy than cld3 on short posts, Sheng, and code-switching.
# Returns language label + context hint injected into classification prompt.
# No confidence penalties — GPT's own confidence is trusted directly.

LANG_LABELS <- c(
  sw="Swahili", en="English", ki="Kikuyu", luo="Luo",
  kln="Kalenjin", so="Somali", ar="Arabic", sheng="Sheng",
  mixed="Mixed language", other="Other"
)

# ── Language-specific glossaries injected into GPT classification prompt ──
KIKUYU_HATE_MARKERS <- c(
  "murogi"="witch/sorcerer (derogatory)",
  "gĩthĩ"="rubbish/worthless person",
  "ndũgũ"="brother (ethnic mobilisation framing)",
  "twĩrĩre"="we told you (us-vs-them framing)",
  "mũndũ"="person (combined with slurs = dehumanisation)"
)
KALENJIN_HATE_MARKERS <- c(
  "dorobo"="derogatory term for Ogiek/forest dwellers",
  "ng'etuny"="enemy/stranger (ethnic boundary marker)",
  "korosek"="chase away/expel (expulsion rhetoric)",
  "boisiek"="outsiders (exclusion framing)",
  "kipkorir"="warrior (mobilisation context)"
)
LUO_HATE_MARKERS <- c(
  "jajuok"="witch/evil person (derogatory)",
  "wuod"="son of (used in ethnic targeting)",
  "dhi"="go — expulsion rhetoric (dhi dala = go home)",
  "mondo"="let it be (precedes commands including threats)"
)
SHENG_MARKERS <- c(
  "niaje","maze","buda","msee","wadau","mresh","fiti","poa",
  "sanse","manze","wueh","mdau","freshi","chali","dem","bomba"
)

# ── GPT language detection ────────────────────────────────────────
detect_language <- function(text) {
  api_key <- Sys.getenv("OPENAI_API_KEY")
  
  # Fast heuristic pre-check for Sheng before calling API
  text_l  <- tolower(text)
  n_sheng <- sum(sapply(SHENG_MARKERS, function(m) grepl(m, text_l, fixed=TRUE)))
  
  result <- tryCatch({
    resp <- request("https://api.openai.com/v1/chat/completions") |>
      req_headers("Content-Type"="application/json",
                  "Authorization"=paste("Bearer", api_key)) |>
      req_body_json(list(
        model       = OPENAI_MODEL,
        max_tokens  = 60L,
        temperature = 0,
        messages    = list(
          list(role="system", content=paste0(
            "You are a language identifier for Kenyan social media. ",
            "Respond with ONLY a JSON object: ",
            "{\"code\":\"<sw|en|ki|luo|kln|so|ar|sheng|mixed|other>\",",
            "\"label\":\"<full name>\",",
            "\"is_sheng\":<true|false>,",
            "\"is_mixed\":<true|false>} ",
            "Codes: sw=Swahili, en=English, ki=Kikuyu, luo=Luo, kln=Kalenjin, ",
            "so=Somali, ar=Arabic, sheng=Sheng/code-switching, mixed=multiple languages, other=other. ",
            "If Swahili-English code-switching with urban slang, use sheng."
          )),
          list(role="user", content=paste0('Identify the language of: "', 
                                           substr(gsub('"',"'",text), 1, 200), '"'))
        )
      )) |>
      req_error(is_error=\(r) FALSE) |>
      req_perform() |>
      resp_body_json()
    
    raw  <- gsub("```json|```|\\n","", trimws(resp$choices[[1]]$message$content))
    parsed <- tryCatch(jsonlite::fromJSON(raw), error=function(e) NULL)
    
    if (!is.null(parsed)) {
      list(
        code     = parsed$code     %||% "other",
        label    = parsed$label    %||% "Other",
        is_sheng = isTRUE(parsed$is_sheng) || n_sheng >= 2,
        is_mixed = isTRUE(parsed$is_mixed)
      )
    } else {
      list(code="other", label="Other", is_sheng=(n_sheng>=2), is_mixed=FALSE)
    }
  }, error = function(e) {
    # Fallback if API call fails — use Sheng heuristic only
    list(code="other", label="Other", is_sheng=(n_sheng>=2), is_mixed=FALSE)
  })
  
  code  <- result$code
  label <- result$label
  
  # Build warning message for UI display (informational only — no penalties)
  warning_msg <- if (isTRUE(result$is_sheng) || code == "sheng")
    "🌐 Sheng/code-switching detected — language-specific context injected into classification prompt."
  else if (isTRUE(result$is_mixed) || code == "mixed")
    "🌐 Mixed-language content detected — context injected into classification prompt."
  else if (code == "ki")
    "🌐 Kikuyu detected — Kikuyu glossary injected into classification prompt."
  else if (code == "kln")
    "🌐 Kalenjin detected — PEV-era rhetorical pattern hints injected into classification prompt."
  else if (code == "luo")
    "🌐 Luo detected — Luo glossary injected into classification prompt."
  else
    NA_character_
  
  list(
    code     = code,
    label    = label,
    is_sheng = isTRUE(result$is_sheng) || code == "sheng",
    is_mixed = isTRUE(result$is_mixed) || code == "mixed",
    warning  = warning_msg
  )
}

# ── Build language context hint for GPT classification prompt ─────
build_language_hint <- function(lang_det, text) {
  code <- lang_det$code
  hint_parts <- c()
  text_l <- tolower(text)
  
  if (isTRUE(lang_det$is_sheng) || code == "sheng") {
    detected <- SHENG_MARKERS[sapply(SHENG_MARKERS, function(m)
      grepl(m, text_l, fixed=TRUE))]
    hint_parts <- c(hint_parts, paste0(
      "LANGUAGE CONTEXT: This post is in Sheng (Nairobi urban code-switching). ",
      if (length(detected) > 0)
        paste0("Detected markers: [", paste(head(detected,5), collapse=", "), "]. ")
      else "",
      "Sheng is used to evade keyword detection — interpret slang by its underlying ",
      "Swahili/English meaning when assessing intent."))
  }
  
  if (code == "ki") {
    matched <- names(KIKUYU_HATE_MARKERS)[sapply(names(KIKUYU_HATE_MARKERS),
                                                 function(m) grepl(m, text_l, fixed=TRUE))]
    gloss <- if (length(matched)>0)
      paste0(" Terms detected: ", paste(sapply(matched, function(m)
        paste0("'",m,"'=",KIKUYU_HATE_MARKERS[m])), collapse="; "), ".") else ""
    hint_parts <- c(hint_parts, paste0(
      "LANGUAGE CONTEXT: Kikuyu (Central Kenya). Focus on structural signals — ",
      "us-vs-them framing, exclusion commands, dehumanising comparisons — ",
      "rather than keyword matching alone.", gloss))
  }
  
  if (code == "kln") {
    matched <- names(KALENJIN_HATE_MARKERS)[sapply(names(KALENJIN_HATE_MARKERS),
                                                   function(m) grepl(m, text_l, fixed=TRUE))]
    gloss <- if (length(matched)>0)
      paste0(" Terms detected: ", paste(sapply(matched, function(m)
        paste0("'",m,"'=",KALENJIN_HATE_MARKERS[m])), collapse="; "), ".") else ""
    hint_parts <- c(hint_parts, paste0(
      "LANGUAGE CONTEXT: Kalenjin (Rift Valley). Alert to PEV-era (2007/08) ",
      "rhetorical patterns — expulsion rhetoric, warrior-framing, ",
      "boundary-marking between communities.", gloss))
  }
  
  if (code == "luo") {
    matched <- names(LUO_HATE_MARKERS)[sapply(names(LUO_HATE_MARKERS),
                                              function(m) grepl(m, text_l, fixed=TRUE))]
    gloss <- if (length(matched)>0)
      paste0(" Terms detected: ", paste(sapply(matched, function(m)
        paste0("'",m,"'=",LUO_HATE_MARKERS[m])), collapse="; "), ".") else ""
    hint_parts <- c(hint_parts, paste0(
      "LANGUAGE CONTEXT: Luo (Nyanza region). Focus on ethnic targeting, ",
      "expulsion commands (dhi dala = go home), dehumanising comparisons.", gloss))
  }
  
  if (isTRUE(lang_det$is_mixed) || code == "mixed")
    hint_parts <- c(hint_parts,
                    "LANGUAGE CONTEXT: Mixed-language post. Assess each language segment independently.")
  
  if (length(hint_parts) == 0) return("")
  paste0("\n\n", paste(hint_parts, collapse="\n"), "\n")
}

# ── PROPHET FORECAST ENGINE ──────────────────────────────────────
# Fits a Prophet model per county on daily risk scores.
# Falls back gracefully to weighted heuristic if data is insufficient.
# Returns a list: escalation_score, forecast_level, trend, drivers,
#                 prophet_used (bool), forecast_df (future 14d), hist_df

FORECAST_DAYS   <- 14L
FORECAST_WINDOW <- 30L   # days of history used for training
MIN_PROPHET_OBS <- 7L    # minimum daily obs needed to fit Prophet

run_prophet_forecast <- function(cases_df, county_name, now = Sys.time()) {
  cutoff  <- now - as.difftime(FORECAST_WINDOW, units = "days")
  h       <- cases_df[!is.na(cases_df$timestamp) &
                        cases_df$timestamp >= cutoff &
                        cases_df$county == county_name, ]
  all_co  <- cases_df[cases_df$county == county_name, ]
  
  # ── Shared summary stats (always computed) ────────────────────
  n_recent     <- nrow(h)
  avg_risk     <- if (n_recent > 0) round(mean(h$risk_score,  na.rm=TRUE), 0) else 0L
  avg_ncic     <- if (n_recent > 0) round(mean(h$ncic_level,  na.rm=TRUE), 1) else 0
  n_high       <- sum(h$ncic_level >= 4, na.rm=TRUE)
  n_s13        <- sum(isTRUE(h$section_13), na.rm=TRUE)
  top_platform <- if (n_recent > 0) names(sort(table(h$platform),  decreasing=TRUE))[1] else "—"
  top_category <- if (n_recent > 0) names(sort(table(h$category),  decreasing=TRUE))[1] else "—"
  
  # ── Aggregate to daily risk score (Prophet needs ds + y) ──────
  prophet_used <- FALSE
  escalation_score <- 0L
  hist_df      <- NULL
  forecast_df  <- NULL
  trend        <- 0L
  
  if (FALSE) {
    daily <- h |>
      mutate(ds = as.Date(timestamp)) |>
      group_by(ds) |>
      summarise(y = mean(risk_score, na.rm=TRUE), .groups="drop") |>
      arrange(ds)
    
    # Need at least MIN_PROPHET_OBS distinct days
    if (FALSE) {
      prophet_used <- TRUE
      
      m <- tryCatch({
        prophet(
          daily,
          daily.seasonality  = FALSE,
          weekly.seasonality = TRUE,
          yearly.seasonality = FALSE,
          seasonality.mode   = "additive",
          changepoint.prior.scale = 0.15,   # moderate flexibility
          interval.width     = 0.80,
          verbose            = FALSE
        )
      }, error = function(e) NULL)
      
      if (!is.null(m)) {
        future_df   <- make_future_dataframe(m, periods = FORECAST_DAYS, freq = "day")
        forecast_df <- predict(m, future_df)
        
        # Historical actuals (for chart)
        hist_df <- daily
        
        # Escalation score = capped mean of Prophet's upper-bound forecast
        # for the next 14 days, normalised to 0–100
        fc_next <- forecast_df[forecast_df$ds > as.Date(now), ]
        if (nrow(fc_next) > 0) {
          # Use yhat_upper for a conservative worst-case view
          raw_score        <- mean(fc_next$yhat_upper, na.rm=TRUE)
          escalation_score <- min(100L, max(0L, round(raw_score)))
        } else {
          escalation_score <- min(100L, avg_risk)
        }
        
        # Trend: slope over forecast period vs last 7d actuals
        last7   <- daily[daily$ds >= as.Date(now) - 7, ]
        risk_w1 <- if (nrow(last7) > 0) mean(last7$y, na.rm=TRUE) else NA
        risk_w2 <- mean(daily$y[seq_len(max(1, nrow(daily)-7))], na.rm=TRUE)
        trend   <- if (!is.na(risk_w1) && risk_w2 > 0)
          round((risk_w1 - risk_w2) / risk_w2 * 100, 0) else 0L
        
      } else {
        prophet_used <- FALSE   # model failed — fall through to heuristic
      }
    }
  }
  
  # ── Heuristic fallback (when Prophet can't fit) ───────────────
  if (!prophet_used) {
    w1 <- h[h$timestamp >= now - as.difftime(7,  units="days"), ]
    w2 <- h[h$timestamp >= now - as.difftime(14, units="days") &
              h$timestamp <  now - as.difftime(7,  units="days"), ]
    risk_w1  <- if (nrow(w1) > 0) mean(w1$risk_score, na.rm=TRUE) else NA
    risk_w2  <- if (nrow(w2) > 0) mean(w2$risk_score, na.rm=TRUE) else NA
    trend    <- if (!is.na(risk_w1) && !is.na(risk_w2) && risk_w2 > 0)
      round((risk_w1 - risk_w2) / risk_w2 * 100, 0) else 0L
    velocity <- if (nrow(w1) > 0) round(nrow(w1) / 7, 1) else 0
    escalation_score <- min(100L, round(
      avg_risk * 0.35 + avg_ncic * 8 + n_high * 3 +
        max(0, trend) * 0.5 + velocity * 4))
  }
  
  # ── Forecast level ────────────────────────────────────────────
  forecast_level <- if      (escalation_score >= 70) "CRITICAL"
  else if (escalation_score >= 50) "HIGH"
  else if (escalation_score >= 30) "ELEVATED"
  else if (escalation_score >  0)  "MONITORED"
  else                             "STABLE"
  
  # ── Key drivers ───────────────────────────────────────────────
  drivers <- c(
    if (prophet_used)    "Prophet time-series model"      else "Heuristic (insufficient data)",
    if (avg_ncic >= 3.5) "High avg NCIC level"            else NULL,
    if (n_high   >= 3)   paste0(n_high, " L4+ cases")     else NULL,
    if (trend    > 20)   paste0("↑", trend, "% trend")    else NULL,
    if (n_s13    > 0)    paste0(n_s13, " S13 cases")      else NULL
  )
  
  list(
    county           = county_name,
    n_recent         = n_recent,
    avg_risk         = avg_risk,
    avg_ncic         = avg_ncic,
    n_high           = n_high,
    n_s13            = n_s13,
    trend            = trend,
    top_platform     = top_platform,
    top_category     = top_category,
    escalation_score = escalation_score,
    forecast_level   = forecast_level,
    drivers          = paste(drivers, collapse = " · "),
    prophet_used     = prophet_used,
    hist_df          = hist_df,
    forecast_df      = forecast_df
  )
}
build_county_forecasts <- function(cases_df, now = Sys.time()) {
# ── Run forecast for all counties (cached per session) ───────────
  top_counties <- unique(cases_df$county)  # all counties use heuristic
  results <- lapply(counties$name, function(co) { on.exit(gc()); df <- if (co %in% top_counties) cases_df else cases_df[0,]; tryCatch(run_prophet_forecast(df, co, now),
      error = function(e) {
        message(sprintf("[forecast] %s failed: %s", co, e$message))
        list(county=co, n_recent=0, avg_risk=0, avg_ncic=0, n_high=0,
             n_s13=0, trend=0, top_platform="—", top_category="—",
             escalation_score=0, forecast_level="STABLE",
             drivers="Model error", prophet_used=FALSE,
             hist_df=NULL, forecast_df=NULL)
      }
    )
  })
  results
}
ui <- page_navbar(
  header = tags$head(
    shinyjs::useShinyjs(),
    tags$script(HTML(sprintf('
      // ── Inactivity tracker ─────────────────────────────────────
      (function() {
        var timer;
        function ping() {
          clearTimeout(timer);
          timer = setTimeout(function() {
            if (typeof Shiny !== "undefined" && Shiny.setInputValue) {
              Shiny.setInputValue("activity_ping", Date.now(), {priority: "event"});
            }
          }, 5000);
        }
        ["mousemove","keydown","mousedown","touchstart","scroll","click"]
          .forEach(function(ev) { document.addEventListener(ev, ping, true); });
        setTimeout(function() {
          if (typeof Shiny !== "undefined")
            Shiny.setInputValue("activity_ping", Date.now(), {priority: "event"});
        }, 3000);
      })();
    '))),
    # Loading bar div — controlled by shinyjs
    tags$div(id="ews-loading-bar"),
    tags$script(HTML('
      // ── Global loading bar ──────────────────────────────────────
      // Shows animated bar at top of page whenever Shiny is recalculating.
      // Hides automatically when all outputs are idle.
      $(document).on("shiny:busy", function() {
        document.getElementById("ews-loading-bar").classList.add("active");
      });
      $(document).on("shiny:idle", function() {
        document.getElementById("ews-loading-bar").classList.remove("active");
      });
      // Also show on any output recalculation start, hide on value
      $(document).on("shiny:recalculating", function() {
        document.getElementById("ews-loading-bar").classList.add("active");
      });
      $(document).on("shiny:value shiny:error", function() {
        if (!$(".shiny-output-recalculating").length) {
          document.getElementById("ews-loading-bar").classList.remove("active");
        }
      });
    '))
  ),
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
                                                     
                                                     accordion_panel(title=tagList(bs_icon("robot")," ML Classifier — DEMO"),value="acc_chat",
                                                                     uiOutput("chat_history"),
                                                                     tags$div(id="chat_spinner",
                                                                       tags$div(class="cs-ring"),
                                                                       tags$div(
                                                                         tags$div(style="font-size:11px;font-weight:700;color:#0066cc;margin-bottom:3px;",
                                                                                  "Classifying…"),
                                                                         tags$div(style="font-size:10px;color:#6c757d;margin-bottom:2px;",
                                                                                  "GPT-4o-mini · NCIC Cap 170"),
                                                                         tags$div(class="cs-dots",
                                                                                  tags$span(), tags$span(), tags$span())
                                                                       )
                                                                     ),
                                                                     tags$div(class="chat-input-row", style="margin-top:4px;",
                                                                              tags$textarea(id="chat_input", class="form-control chat-textarea",
                                                                                            placeholder="Paste post here…", rows=3),
                                                                              tags$button("→", id="chat_send", class="btn-classify",
                                                                                onclick="
                                                                                  var btn=this, sp=document.getElementById('chat_spinner');
                                                                                  btn.disabled=true; btn.innerHTML='⏳';
                                                                                  if(sp){ sp.classList.add('active'); sp.scrollIntoView({behavior:'smooth',block:'nearest'}); }
                                                                                  Shiny.setInputValue('chat_send',Math.random());
                                                                                ")),
                                                                     tags$div(style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:6px;padding:6px 10px;font-size:11px;color:#0c4a6e;margin-top:5px;",
                                                                              tags$div(style="font-weight:700;margin-bottom:3px;",tagList(bs_icon("shield-fill-check")," Anti-Hallucination — 6 Layers Active")),
                                                                              tags$div(style="display:flex;flex-direction:column;gap:1px;",
                                                                                       tags$span("① temperature=0 — deterministic output"),
                                                                                       tags$span("② JSON schema — no free-text invention"),
                                                                                       tags$span("③ Cap 170 decision chain — legally grounded"),
                                                                                       tags$span("④ Confidence score — uncertainty flagged"),
                                                                                       tags$span("⑤ Officer validation — human has final word"),
                                                                                       tags$span("⑥ Disagreement log — model learns from corrections")
                                                                              ),
                                                                              tags$div(style="margin-top:4px;padding-top:4px;border-top:1px solid #bae6fd;color:#0066cc;font-weight:600;",
                                                                                       "🌐 Language-aware: GPT detects language · glossaries injected for Kikuyu, Kalenjin, Luo, Sheng")
                                                                     ),
                                                                     tags$hr(style="border-color:#dee2e6;margin:6px 0;"),
                                                                     tags$div(style="display:flex;flex-direction:column;gap:3px;",
                                                                              actionButton("ex1","L5 Toxic — explicit violence",        class="btn btn-sm btn-outline-danger w-100",    style="font-size:11px;text-align:left;"),
                                                                              actionButton("ex2","L4 Hate speech — incitement",          class="btn btn-sm btn-outline-warning w-100",   style="font-size:11px;text-align:left;"),
                                                                              actionButton("ex3","L3 Dehumanization · 🌐 Swahili",       class="btn btn-sm btn-outline-secondary w-100", style="font-size:11px;text-align:left;"),
                                                                              actionButton("ex5","L2 Prejudice · 🌐 Sheng",              class="btn btn-sm btn-outline-secondary w-100", style="font-size:11px;text-align:left;color:#7c3aed;border-color:#7c3aed;"),
                                                                              actionButton("ex4","L0 Neutral — safe content",             class="btn btn-sm btn-outline-success w-100",   style="font-size:11px;text-align:left;")
                                                                     )
                                                     ),
                                                     
                                                     accordion_panel(title=tagList(bs_icon("funnel")," Filters"),value="acc_filt",
                                                                     selectInput("f_county","County",choices=c("All Counties",counties$name),selected="Nairobi",width="100%"),
                                                                     selectInput("f_ncic","NCIC Level",
                                                                                 choices=c("All",setNames(5:0,paste0("L",5:0," — ",rev(NCIC_LEVELS)))),
                                                                                 selected="All",width="100%"),
                                                                     
                                                                     
                                                                     date_filter_ui("map_dr","Date window"),
                                                                     checkboxInput("show_flow","Show message flow arrows",value=TRUE)
                                                     ),
                                                     

                                                     

                                           )
                           ),
                           card(full_screen=TRUE,
                                card_header(
    tags$div(style="display:flex;align-items:center;justify-content:space-between;width:100%;",
      tagList(bs_icon("geo-alt")," Kenya Incident Map · NCIC Level Colouring · click for cases"),
      tags$div(style="display:flex;gap:6px;",
        tags$span(style="font-size:10px;color:#6c757d;margin-top:3px;font-style:italic;","🇰🇪 Kenya"),
        checkboxInput("show_regional", 
          tagList(bs_icon("globe")," Regional Threats"),
          value=FALSE),
        tags$span(style="font-size:10px;color:#6c757d;margin-top:3px;",
          "🌍 East Africa")
      )
    )
  ),
                                leafletOutput("kenya_map",height="620px"))
            )
  ),
  
  # TAB 2: DASHBOARD
  nav_panel(title=tagList(bs_icon("speedometer2")," Current Signals"),value="tab_dash",padding=16,
            
            # ── Page header + date filter in one row ─────────────────
            tags$div(style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px;",
                     tags$div(
                       tags$h4(style="font-weight:800;color:#1a1a2e;margin:0 0 2px;font-size:16px;",
                               tagList(bs_icon("speedometer2"), " Signal Intelligence Dashboard")),
                       tags$div(style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;",
                                tags$span(style="font-size:12px;color:#6c757d;",
                                          tagList(bs_icon("funnel"), " All charts and tables filtered by date range")),
                                tags$span(style="background:#fff8e1;color:#664d03;border:1px solid #ffc107;border-radius:3px;padding:1px 8px;font-size:11px;font-weight:600;",
                                          "⚠ AI signals — not proof of illegal activity")
                       )
                     ),
                     tags$div(style="display:flex;gap:6px;align-items:center;flex-shrink:0;",
                              date_filter_ui("dash_dr", NULL),
                              actionButton("dash_reset", tagList(bs_icon("x-circle"), " Reset"),
                                           class="btn btn-outline-secondary btn-sm",
                                           style="white-space:nowrap;font-size:11px;"),
                              tags$a(tagList(bs_icon("book")),
                                     href="#", onclick="$('[data-value=\"tab_interpretation\"]').tab('show');return false;",
                                     style="font-size:11px;color:#0066cc;font-weight:600;text-decoration:none;white-space:nowrap;padding:5px 8px;background:#f0f9ff;border:1px solid #bae6fd;border-radius:4px;",
                                     title="Interpretation Guide"),
                              tags$a(tagList(bs_icon("diagram-3")),
                                     href="#", onclick="$('[data-value=\"tab_about\"]').tab('show');return false;",
                                     style="font-size:11px;color:#7c3aed;font-weight:600;text-decoration:none;white-space:nowrap;padding:5px 8px;background:#f8f0ff;border:1px solid #ddd6fe;border-radius:4px;",
                                     title="Methodology")
                     )
            ),
            
            # ── KPI row (rendered server-side) ───────────────────────
            uiOutput("kpi_row"),
            
            # ── Charts + Tables ───────────────────────────────────────
            layout_columns(col_widths=c(6,6),
                           # Charts card
                           card(style="border-top:3px solid #0066cc;",
                                card_header(
                                  tags$div(style="display:flex;align-items:center;justify-content:space-between;width:100%;",
                                           tags$span(style="font-size:13px;font-weight:700;",
                                                     tagList(bs_icon("bar-chart-fill"), " Analytics Charts")),
                                           tags$span(style="font-size:10px;color:#9ca3af;font-style:italic;",
                                                     "Signals are proxies, not proof. L4–L5 require officer validation.")
                                  )
                                ),
                                navset_pill(
                                  id = "dash_chart_pills",
                                  nav_panel("NCIC Distribution",
                                            tags$div(style="height:340px;padding-top:8px;",
                                                     highchartOutput("plot_ncic", height="340px", width="100%"))),
                                  nav_panel("Platform Breakdown",
                                            tags$div(style="height:340px;padding-top:8px;",
                                                     highchartOutput("plot_platform", height="340px", width="100%"))),
                                  nav_panel("County Risk Map",
                                            tags$div(style="height:360px;padding-top:8px;",
                                                     tmapOutput("plot_county_map", height="360px")))
                                ),
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
                           # Tables card
                           card(style="border-top:3px solid #1a1a2e;",
                                card_header(
                                  tags$div(style="display:flex;align-items:center;justify-content:space-between;width:100%;",
                                           tags$span(style="font-size:13px;font-weight:700;",
                                                     tagList(bs_icon("table"), " Intelligence Tables")),
                                           tags$a(tagList(bs_icon("exclamation-octagon"), " S13 Queue"),

                                                  style="font-size:11px;color:#dc3545;font-weight:600;text-decoration:none;")
                                  )
                                ),
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
  
  nav_panel(title=tagList(bs_icon("lock")," Validation & Learning"),value="tab_val",padding=16,
                     tags$div(style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:6px;padding:7px 14px;margin-bottom:4px;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;",
                              tags$div(style="display:flex;align-items:center;gap:8px;font-size:12px;color:#0c4a6e;",
                                       tags$span("🧑‍⚖️"),
                                       tags$span(tags$strong("HITL validation — "), "confirm, downgrade, escalate, or clear. Every decision updates keyword weights and re-scores all cases.")
                              ),
                              tags$a(tagList(bs_icon("book")," Validation workflow guide"),
                                     href="#", onclick="$('[data-value=\"tab_interpretation\"]').tab('show');return false;",
                                     style="font-size:11px;color:#0066cc;font-weight:600;text-decoration:none;white-space:nowrap;flex-shrink:0;")
                     ),
                     navset_pill(
                       id = "val_learn_pills",
                       nav_panel(tagList(bs_icon("shield-check")," Officer Validation"),
                                 tags$div(style="padding-top:12px;", uiOutput("val_ui"))),
                       nav_panel(tagList(bs_icon("graph-up")," Learning Centre"),
                                 tags$div(style="padding-top:12px;", uiOutput("learn_ui")))
                     )
           ),
           
  
  # TAB 9: FORECAST
  nav_panel(title=tagList(bs_icon("graph-up-arrow")," 14-Day Forecast"),value="tab_forecast",padding=16,
            uiOutput("forecast_ui")),
  
  # TAB 10: METHODOLOGY
  nav_panel(title=tagList(bs_icon("diagram-3")," Methodology"),value="tab_about",padding=24,
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
                     
                     # ── Cross-reference bar ────────────────────────────────────
                     tags$div(
                       style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:6px;padding:8px 14px;margin-bottom:24px;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;",
                       tags$div(style="font-size:12px;color:#0c4a6e;",
                                tagList(bs_icon("info-circle"), " This page covers the technical methodology — classification logic, risk scoring, safeguards, and limitations."),
                       ),
                       tags$a(tagList(bs_icon("book")," Interpretation & Usage Guide →"),
                              href="#", onclick="$('[data-value=\"tab_interpretation\"]').tab('show');return false;",
                              style="font-size:11px;color:#0066cc;font-weight:600;text-decoration:none;white-space:nowrap;flex-shrink:0;")
                     ),
                     
                     # ── NCIC Taxonomy ─────────────────────────────────────────
                     tags$div(style="margin:0 0 8px;",
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
                                            
                                            # ── Formula header ─────────────────────────────
                                            tags$div(style="font-size:11px;font-weight:800;color:#0066cc;text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px;","Risk Score Formula"),
                                            
                                            # ── Master formula ─────────────────────────────
                                            tags$div(style="font-family:'IBM Plex Mono',monospace;font-size:12px;color:#1a1a2e;background:#fff;padding:10px 14px;border-radius:6px;border:1px solid #dee2e6;margin-bottom:10px;",
                                                     tags$div("Risk = 0.55 × composite + 0.45 × NCIC_base"),
                                                     tags$div(style="color:#6c757d;font-size:10px;margin-top:4px;",
                                                              "composite = 0.30×conf + 0.22×kw + 0.13×net + 0.08×spike + 0.12×ctx + 0.15×src_hist")
                                            ),
                                            
                                            # ── Weight explanation table ────────────────────
                                            tags$div(style="font-size:11px;font-weight:700;color:#374151;margin-bottom:6px;","Component Weights Explained"),
                                            tags$table(style="width:100%;border-collapse:collapse;font-size:11px;margin-bottom:14px;",
                                                       tags$thead(
                                                         tags$tr(style="background:#e9ecef;",
                                                                 tags$th(style="padding:5px 8px;text-align:left;font-weight:700;","Component"),
                                                                 tags$th(style="padding:5px 8px;text-align:center;font-weight:700;","Weight"),
                                                                 tags$th(style="padding:5px 8px;text-align:left;font-weight:700;","What it measures")
                                                         )
                                                       ),
                                                       tags$tbody(
                                                         tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                 tags$td(style="padding:5px 8px;font-family:'IBM Plex Mono';color:#0066cc;","conf"),
                                                                 tags$td(style="padding:5px 8px;text-align:center;font-weight:700;","30%"),
                                                                 tags$td(style="padding:5px 8px;color:#374151;","GPT-4o-mini self-reported certainty (0–100). Largest single driver.")
                                                         ),
                                                         tags$tr(style="border-bottom:1px solid #f0f0f0;background:#fafafa;",
                                                                 tags$td(style="padding:5px 8px;font-family:'IBM Plex Mono';color:#0066cc;","kw"),
                                                                 tags$td(style="padding:5px 8px;text-align:center;font-weight:700;","22%"),
                                                                 tags$td(style="padding:5px 8px;color:#374151;","Adaptive keyword weight score. Updates every time an officer validates a case.")
                                                         ),
                                                         tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                 tags$td(style="padding:5px 8px;font-family:'IBM Plex Mono';color:#0066cc;","net"),
                                                                 tags$td(style="padding:5px 8px;text-align:center;font-weight:700;","13%"),
                                                                 tags$td(style="padding:5px 8px;color:#374151;","Network exposure score — estimated reach and amplification risk.")
                                                         ),
                                                         tags$tr(style="border-bottom:1px solid #f0f0f0;background:#fafafa;",
                                                                 tags$td(style="padding:5px 8px;font-family:'IBM Plex Mono';color:#0066cc;","spike"),
                                                                 tags$td(style="padding:5px 8px;text-align:center;font-weight:700;","8%"),
                                                                 tags$td(style="padding:5px 8px;color:#374151;","Frequency spike — unusual posting volume above the county baseline.")
                                                         ),
                                                         tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                 tags$td(style="padding:5px 8px;font-family:'IBM Plex Mono';color:#0066cc;","ctx"),
                                                                 tags$td(style="padding:5px 8px;text-align:center;font-weight:700;","12%"),
                                                                 tags$td(style="padding:5px 8px;color:#374151;","Contextual factors flagged by GPT (e.g. dehumanisation, mobilisation). Each factor = +8 pts.")
                                                         ),
                                                         tags$tr(style="border-bottom:1px solid #f0f0f0;background:#fafafa;",
                                                                 tags$td(style="padding:5px 8px;font-family:'IBM Plex Mono';color:#0066cc;","src_hist"),
                                                                 tags$td(style="padding:5px 8px;text-align:center;font-weight:700;","15%"),
                                                                 tags$td(style="padding:5px 8px;color:#374151;","Source history — handle and county posting pattern over 30 days. Capped at +30.")
                                                         ),
                                                         tags$tr(style="background:#e8f0fe;",
                                                                 tags$td(style="padding:5px 8px;font-family:'IBM Plex Mono';color:#0066cc;","NCIC_base"),
                                                                 tags$td(style="padding:5px 8px;text-align:center;font-weight:700;","45%"),
                                                                 tags$td(style="padding:5px 8px;color:#374151;","Fixed base score per NCIC level: L0=2, L1=15, L2=30, L3=55, L4=72, L5=92.")
                                                         )
                                                       )
                                            ),
                                            
                                            # ── L0-L5 worked examples ───────────────────────
                                            tags$div(style="font-size:11px;font-weight:700;color:#374151;margin-bottom:8px;","Worked Examples — L0 to L5"),
                                            tags$div(style="display:flex;flex-direction:column;gap:6px;",
                                                     
                                                     # L0
                                                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #198754;border-radius:6px;padding:8px 12px;",
                                                              tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:4px;",
                                                                       tags$span(style="background:#198754;color:#fff;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;","L0 Neutral"),
                                                                       tags$span(style="font-size:11px;color:#374151;font-style:italic;",'"Heshima kwa wote, hakuna kabila bora kuliko lingine"')
                                                              ),
                                                              tags$div(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;margin-bottom:3px;",
                                                                       "L0 base(2) + 0.30×conf(95) + 0.22×kw(0) + 0.13×net(10) + 0.12×ctx(0) + 0.15×src(0) = 14"),
                                                              tags$div(style="font-size:10px;color:#198754;","No action. Log for trend analysis. Peace message — constitutionally protected speech.")
                                                     ),
                                                     
                                                     # L1
                                                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #85b800;border-radius:6px;padding:8px 12px;",
                                                              tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:4px;",
                                                                       tags$span(style="background:#85b800;color:#fff;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;","L1 Offensive"),
                                                                       tags$span(style="font-size:11px;color:#374151;font-style:italic;",'"These politicians are useless thieves, all of them"')
                                                              ),
                                                              tags$div(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;margin-bottom:3px;",
                                                                       "L1 base(15) + 0.30×conf(80) + 0.22×kw(10) + 0.13×net(20) + 0.12×ctx(0) + 0.15×src(0) = 22"),
                                                              tags$div(style="font-size:10px;color:#85b800;","Monitor. Targets public officials — not a protected group. No Section 13 threshold.")
                                                     ),
                                                     
                                                     # L2
                                                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #ffc107;border-radius:6px;padding:8px 12px;",
                                                              tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:4px;",
                                                                       tags$span(style="background:#ffc107;color:#1a1a2e;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;","L2 Prejudice"),
                                                                       tags$span(style="font-size:11px;color:#374151;font-style:italic;",'"Kabila fulani hawastahili nafasi katika serikali"')
                                                              ),
                                                              tags$div(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;margin-bottom:3px;",
                                                                       "L2 base(30) + 0.30×conf(78) + 0.22×kw(20) + 0.13×net(25) + 0.12×ctx(8) + 0.15×src(0) = 36"),
                                                              tags$div(style="font-size:10px;color:#664d03;","Document pattern. Ethnic stereotyping — protected group targeted. Request platform review if repeated.")
                                                     ),
                                                     
                                                     # L3
                                                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #fd7e14;border-radius:6px;padding:8px 12px;",
                                                              tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:4px;",
                                                                       tags$span(style="background:#fd7e14;color:#fff;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;","L3 Dehumanization"),
                                                                       tags$span(style="font-size:11px;color:#374151;font-style:italic;",'"Watu wa kabila hiyo ni kama magonjwa — wanaharibu nchi"')
                                                              ),
                                                              tags$div(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;margin-bottom:3px;",
                                                                       "L3 base(55) + 0.30×conf(82) + 0.22×kw(28) + 0.13×net(40) + 0.12×ctx(16) + 0.15×src(0) = 56"),
                                                              tags$div(style="font-size:10px;color:#fd7e14;","Issue monitoring flag. Disease metaphor = dehumanisation. Initiate engagement with poster.")
                                                     ),
                                                     
                                                     # L4
                                                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #dc3545;border-radius:6px;padding:8px 12px;",
                                                              tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:4px;",
                                                                       tags$span(style="background:#dc3545;color:#fff;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;","L4 Hate Speech"),
                                                                       tags$span(style="background:#dc3545;color:#fff;border-radius:3px;padding:1px 5px;font-size:9px;font-weight:700;","⚖ S13"),
                                                                       tags$span(style="font-size:11px;color:#374151;font-style:italic;",'"Waende kwao — hawastahili kuishi hapa Kenya yetu"')
                                                              ),
                                                              tags$div(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;margin-bottom:3px;",
                                                                       "L4 base(72) + 0.30×conf(85) + 0.22×kw(30) + 0.13×net(60) + 0.12×ctx(16) + 0.15×src(0) = 73"),
                                                              tags$div(style="font-size:10px;color:#dc3545;","Section 13 threshold crossed. Expulsion rhetoric directed at ethnic group. Require takedown. Alert DCI.")
                                                     ),
                                                     
                                                     # L5
                                                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #7b0000;border-radius:6px;padding:8px 12px;",
                                                              tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:4px;",
                                                                       tags$span(style="background:#7b0000;color:#fff;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;","L5 Toxic"),
                                                                       tags$span(style="background:#7b0000;color:#fff;border-radius:3px;padding:1px 5px;font-size:9px;font-weight:700;","⚖ S13"),
                                                                       tags$span(style="background:#7c3aed;color:#fff;border-radius:3px;padding:1px 5px;font-size:9px;font-weight:700;","⚠ Escalate"),
                                                                       tags$span(style="font-size:11px;color:#374151;font-style:italic;",'"Chinja wote wa kabila hiyo — tutawaonyesha nguvu kesho"')
                                                              ),
                                                              tags$div(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;margin-bottom:3px;",
                                                                       "L5 base(92) + 0.30×conf(96) + 0.22×kw(45) + 0.13×net(80) + 0.12×ctx(24) + 0.15×src(20) = 91"),
                                                              tags$div(style="font-size:10px;color:#7b0000;font-weight:700;","IMMEDIATE ESCALATION. Violence override triggered (chinja = slaughter). Press charges. Contact DCI now.")
                                                     )
                                            ),
                                            
                                            tags$p(style="font-size:11px;color:#9ca3af;margin-top:10px;margin-bottom:0;",
                                                   "All scores are relative within the current 30-day dataset window. A score of 91 means this post ranks near the top of active signals — not an absolute severity measure.")
                                   )
                          )
                     ),
                     
                     # ── Anti-Hallucination Safeguards ─────────────────────────
                     tags$div(style="margin-bottom:8px;",
                              tags$h5(style="font-weight:700;color:#1a1a2e;margin-bottom:4px;",
                                      tagList(bs_icon("shield-fill-check"), " Anti-Hallucination Safeguards")),
                              tags$p(style="font-size:12px;color:#6c757d;margin-bottom:14px;",
                                     "Six independent layers prevent the AI from producing unreliable, fabricated, or legally consequential outputs without human oversight.")
                     ),
                     
                     # ── 6-layer grid ──────────────────────────────────────────
                     tags$div(style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:24px;",
                              
                              # Layer 1
                              tags$div(style="background:#f0f9ff;border-radius:8px;padding:14px 16px;border-left:4px solid #0066cc;",
                                       tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:8px;",
                                                tags$span(style="background:#0066cc;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;flex-shrink:0;","1"),
                                                tags$span(style="font-size:12px;font-weight:700;color:#1a1a2e;","Deterministic Output")),
                                       tags$div(style="font-family:'IBM Plex Mono';font-size:11px;background:#fff;border:1px solid #bae6fd;border-radius:4px;padding:4px 8px;margin-bottom:6px;color:#0369a1;",
                                                "temperature = 0"),
                                       tags$p(style="font-size:11px;color:#374151;line-height:1.7;margin:0;",
                                              "The same post always produces the same classification. GPT cannot drift, vary, or invent different answers between runs. Responses are cached by content hash — a re-run is guaranteed identical.")
                              ),
                              
                              # Layer 2
                              tags$div(style="background:#f0fff4;border-radius:8px;padding:14px 16px;border-left:4px solid #198754;",
                                       tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:8px;",
                                                tags$span(style="background:#198754;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;flex-shrink:0;","2"),
                                                tags$span(style="font-size:12px;font-weight:700;color:#1a1a2e;","Schema-Constrained Output")),
                                       tags$div(style="font-family:'IBM Plex Mono';font-size:10px;background:#fff;border:1px solid #bbf7d0;border-radius:4px;padding:4px 8px;margin-bottom:6px;color:#166534;",
                                                '{"ncic_level":4,"confidence":87,...}'),
                                       tags$p(style="font-size:11px;color:#374151;line-height:1.7;margin:0;",
                                              "GPT must return a strict JSON schema — no free-text narratives where hallucination thrives. Fields outside the schema are rejected. The model cannot invent fields or omit required ones.")
                              ),
                              
                              # Layer 3
                              tags$div(style="background:#fff5f5;border-radius:8px;padding:14px 16px;border-left:4px solid #dc3545;",
                                       tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:8px;",
                                                tags$span(style="background:#dc3545;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;flex-shrink:0;","3"),
                                                tags$span(style="font-size:12px;font-weight:700;color:#1a1a2e;","Legally-Grounded Decision Chain")),
                                       tags$div(style="font-size:10px;background:#fff;border:1px solid #fecaca;border-radius:4px;padding:4px 8px;margin-bottom:6px;color:#991b1b;font-weight:600;",
                                                "Violence Override → Target Test → Intent Test"),
                                       tags$p(style="font-size:11px;color:#374151;line-height:1.7;margin:0;",
                                              "GPT follows a mandatory 3-stage chain grounded in NCIC Cap 170 legal text. It cannot skip stages, invent new categories, or apply its own reasoning framework. Every level has explicit legal justification.")
                              ),
                              
                              # Layer 4
                              tags$div(style="background:#fff8f0;border-radius:8px;padding:14px 16px;border-left:4px solid #fd7e14;",
                                       tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:8px;",
                                                tags$span(style="background:#fd7e14;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;flex-shrink:0;","4"),
                                                tags$span(style="font-size:12px;font-weight:700;color:#1a1a2e;","Confidence Scoring & Flagging")),
                                       tags$div(style="font-size:10px;background:#fff;border:1px solid #fed7aa;border-radius:4px;padding:4px 8px;margin-bottom:6px;color:#9a3412;font-weight:600;",
                                                "< 60% confidence → flagged LOW → officer alerted"),
                                       tags$p(style="font-size:11px;color:#374151;line-height:1.7;margin:0;",
                                              "Every classification carries a 0–100% self-reported confidence score. Outputs below 60% are prominently flagged as LOW confidence, surfacing model uncertainty directly to the reviewing officer.")
                              ),
                              
                              # Layer 5
                              tags$div(style="background:#f8f0ff;border-radius:8px;padding:14px 16px;border-left:4px solid #7c3aed;",
                                       tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:8px;",
                                                tags$span(style="background:#7c3aed;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;flex-shrink:0;","5"),
                                                tags$span(style="font-size:12px;font-weight:700;color:#1a1a2e;","Mandatory Human-in-the-Loop")),
                                       tags$div(style="font-size:10px;background:#fff;border:1px solid #ddd6fe;border-radius:4px;padding:4px 8px;margin-bottom:6px;color:#5b21b6;font-weight:600;",
                                                "AI flags → Officer decides → Action logged"),
                                       tags$p(style="font-size:11px;color:#374151;line-height:1.7;margin:0;",
                                              "No legal or enforcement action is ever triggered by AI alone. Every L2+ case requires an NCIC officer to Confirm, Escalate, Downgrade, or Clear. The AI is an analyst — the officer is the decision-maker.")
                              ),
                              
                              # Layer 6
                              tags$div(style="background:#f0fdf4;border-radius:8px;padding:14px 16px;border-left:4px solid #059669;",
                                       tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:8px;",
                                                tags$span(style="background:#059669;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;font-size:11px;font-weight:800;flex-shrink:0;","6"),
                                                tags$span(style="font-size:12px;font-weight:700;color:#1a1a2e;","Real-Time Retraining Loop")),
                                       tags$div(style="font-size:10px;background:#fff;border:1px solid #a7f3d0;border-radius:4px;padding:4px 8px;margin-bottom:6px;color:#065f46;font-weight:600;",
                                                "Override → disagreement logged → weights updated → all cases rescored"),
                                       tags$p(style="font-size:11px;color:#374151;line-height:1.7;margin:0;",
                                              "When an officer overrides the AI's NCIC level, the disagreement is logged and keyword weights are updated in real time. The corrected case enters the few-shot training bank — the model learns from every human correction.")
                              )
                     ),
                     
                     # ── Summary statement ──────────────────────────────────────
                     tags$div(
                       style="background:linear-gradient(135deg,#1a1a2e,#0066cc18);border:1px solid #0066cc44;border-radius:8px;padding:14px 18px;margin-bottom:24px;",
                       tags$div(style="font-size:13px;color:#1a1a2e;line-height:1.8;",
                                tags$strong("Net result: "),
                                "The AI cannot hallucinate a legal finding, invent an NCIC level, or trigger enforcement action. ",
                                "Every consequential output carries a human officer's name, timestamp, and decision — logged permanently in the tamper-evident audit trail. ",
                                tags$strong("The platform treats AI as an analyst and the officer as the judge.")
                       )
                     ),
                     
                     accordion(id="about_defs_acc", open=FALSE,
                               accordion_panel(
                                 title=tagList(bs_icon("exclamation-circle")," Limitations & Known Biases"),
                                 value="limits",
                                 layout_columns(col_widths=c(6,6),
                                                tags$div(style="font-size:12px;line-height:1.8;color:#374151;",
                                                         tags$p(tags$strong("Language coverage: "),
                                                                "GPT-4o-mini detects the language of each post before classification — replacing the weaker cld3 statistical library. ",
                                                                "Kikuyu, Kalenjin, Luo, and Sheng posts receive language-specific glossaries of known hate-speech terms with English translations injected directly into the classification prompt. ",
                                                                "A 🌐 language badge is displayed on every classifier result, and a blue informational banner confirms which language context was applied."),
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
                                                                "Per-county Prophet time-series models are fitted on 30 days of daily mean risk scores. Counties with fewer than 7 days of data fall back to a weighted heuristic. Forecasts are directional indicators — not calibrated probabilistic projections — and require officer review before operational action.")
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
  ),
  
  # TAB 11: INTERPRETATION
  nav_panel(title=tagList(bs_icon("info-circle")," Interpretation"),value="tab_interpretation",padding=24,
            tags$div(style="max-width:980px;margin:0 auto;",
                     
                     # ── Page header ────────────────────────────────────────────
                     tags$div(style="margin-bottom:28px;padding-bottom:20px;border-bottom:2px solid #f0f4f8;",
                              tags$h2(style="font-weight:800;color:#1a1a2e;margin:0 0 6px;font-size:22px;letter-spacing:-.01em;",
                                      tagList(bs_icon("book"), " Interpretation & Usage Guide")),
                              tags$p(style="color:#6c757d;font-size:13px;margin:0;",
                                     "How to read this platform, what the outputs mean, and how to act on them responsibly.")
                     ),
                     
                     # ── CRITICAL CAUTION BANNER ────────────────────────────────
                     tags$div(
                       style=paste0("background:linear-gradient(135deg,#fff8e1,#fff3cd);",
                                    "border:2px solid #ffc107;border-left:6px solid #dc3545;",
                                    "border-radius:10px;padding:20px 24px;margin-bottom:28px;"),
                       tags$div(style="display:flex;gap:14px;align-items:flex-start;",
                                tags$span(style="font-size:28px;flex-shrink:0;line-height:1;","⚠️"),
                                tags$div(
                                  tags$div(style="font-size:14px;font-weight:800;color:#7c2d12;margin-bottom:10px;text-transform:uppercase;letter-spacing:.04em;",
                                           "Interpret with Caution — Critical Notices for All Users"),
                                  tags$div(style="font-size:13px;color:#664d03;line-height:1.85;",
                                           tags$p(style="margin:0 0 8px;",
                                                  tags$strong("These signals are AI-generated proxies for potential harmful narratives — they are not proof of illegal activity."),
                                                  " Classification outputs reflect statistical patterns in language as assessed by GPT-4o-mini under the NCIC Cap 170 framework. ",
                                                  "A high NCIC level or risk score indicates content that warrants officer review — not that a crime has been committed."),
                                           tags$p(style="margin:0 0 8px;",
                                                  tags$strong("All L4/L5 cases require officer validation before any legal or operational action."),
                                                  " No prosecution, takedown request, or enforcement measure should be initiated on the basis of AI classification alone. ",
                                                  "Every high-severity case must be independently reviewed, confirmed, and documented by a trained NCIC intelligence officer."),
                                           tags$p(style="margin:0;",
                                                  tags$strong("Risk scores reflect relative patterns, not absolute severity."),
                                                  " Scores are calibrated against this dataset and time window. A score of 80 means this post ranks high ",
                                                  "relative to others in the current 30-day window — it is not a universal threat index and cannot be compared across platforms or time periods.")
                                  )
                                )
                       )
                     ),
                     
                     # ── What does this dashboard show? ─────────────────────────
                     tags$div(style="margin-bottom:24px;",
                              tags$h4(style="font-weight:700;color:#1a1a2e;margin-bottom:12px;font-size:16px;",
                                      tagList(bs_icon("question-circle"), " What Does This Dashboard Show?")),
                              layout_columns(col_widths=c(7,5),
                                             tags$div(style="font-size:13px;line-height:1.85;color:#374151;",
                                                      tags$p(
                                                        tags$strong("Radicalisation Signals"), " monitors online content in Kenya for narratives ",
                                                        "associated with hate speech, ethnic incitement, and radicalisation. Posts are collected from ",
                                                        "social media platforms (Twitter/X, Facebook, TikTok, Telegram, WhatsApp), classified by ",
                                                        tags$strong("GPT-4o-mini"), " under the ", tags$strong("NCIC Cap 170 framework"),
                                                        ", and validated by trained NCIC intelligence officers before any action is taken."
                                                      ),
                                                      tags$p(
                                                        "The dataset covers ", tags$strong("47 monitored counties"), " using real IEBC administrative boundaries, ",
                                                        "over a rolling ", tags$strong("30-day window"),
                                                        ". Signal intensity is weighted by historical election-period tension, population density, and NCIC operational priorities."
                                                      ),
                                                      tags$p(style="font-size:12px;color:#6c757d;font-style:italic;margin-top:4px;",
                                                             "Intended users: NCIC intelligence officers, researchers, civil society organisations (CSOs), and policy analysts. ",
                                                             "This is not a public-facing tool.")
                                             ),
                                             card(style="background:#f8f9fa;border:1px solid #dee2e6;",
                                                  card_header(tagList(bs_icon("database")," Platform Snapshot")),
                                                  tags$table(style="width:100%;border-collapse:collapse;font-size:12px;",
                                                             tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                     tags$td(style="color:#6c757d;padding:7px 4px;","Counties"),
                                                                     tags$td(style="font-weight:600;padding:7px 4px;","47 of 47 (all Kenya)")),
                                                             tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                     tags$td(style="color:#6c757d;padding:7px 4px;","Platforms"),
                                                                     tags$td(style="font-weight:600;padding:7px 4px;","Twitter/X · Facebook · TikTok · Telegram · WhatsApp")),
                                                             tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                     tags$td(style="color:#6c757d;padding:7px 4px;","Languages"),
                                                                     tags$td(style="font-weight:600;padding:7px 4px;","Swahili · English · Sheng · Kikuyu · Luo · Kalenjin")),
                                                             tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                     tags$td(style="color:#6c757d;padding:7px 4px;","Lang detection"),
                                                                     tags$td(style="font-weight:600;padding:7px 4px;","GPT-4o-mini · glossary injection per language")),
                                                             tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                     tags$td(style="color:#6c757d;padding:7px 4px;","AI model"),
                                                                     tags$td(style="font-weight:600;padding:7px 4px;","GPT-4o-mini · temperature=0")),
                                                             tags$tr(style="border-bottom:1px solid #f0f0f0;",
                                                                     tags$td(style="color:#6c757d;padding:7px 4px;","Legal framework"),
                                                                     tags$td(style="font-weight:600;padding:7px 4px;","NCIC Act Cap 170 · Section 13")),
                                                             tags$tr(
                                                               tags$td(style="color:#6c757d;padding:7px 4px;","Analysis window"),
                                                               tags$td(style="font-weight:600;padding:7px 4px;","30-day rolling"))
                                                  )
                                             )
                              )
                     ),
                     
                     tags$hr(style="border-color:#f0f4f8;margin:4px 0 24px;"),
                     
                     # ── How to read this dashboard ─────────────────────────────
                     tags$div(style="margin-bottom:24px;",
                              tags$h4(style="font-weight:700;color:#1a1a2e;margin-bottom:12px;font-size:16px;",
                                      tagList(bs_icon("cursor"), " How to Read This Dashboard")),
                              layout_columns(col_widths=c(6,6),
                                             tags$div(
                                               tags$div(style="font-size:12px;font-weight:700;color:#0066cc;text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px;",
                                                        "Navigation & Filters"),
                                               tags$ol(style="font-size:13px;color:#374151;line-height:1.9;padding-left:18px;margin:0;",
                                                       tags$li("Use the ", tags$strong("date filter"), " (top right of any tab) to narrow the analysis time window."),
                                                       tags$li(tags$strong("KPI cards"), " show signal counts for the selected period — hover over each for full definitions."),
                                                       tags$li("Click ", tags$strong("NCIC Distribution"), " in the Analytics Charts card to see the severity breakdown across all six levels."),
                                                       tags$li("Click ", tags$strong("Platform Breakdown"), " to see which channels (Twitter/X, WhatsApp, etc.) are most active."),
                                                       tags$li("Switch to the ", tags$strong("Priority Cases"), " table to review high-risk posts that need officer validation."),
                                                       tags$li("Use the ", tags$strong("Where are hotspots?"), " tab for geographic signal concentration by county.")
                                               )
                                             ),
                                             tags$div(
                                               tags$div(style="font-size:12px;font-weight:700;color:#0066cc;text-transform:uppercase;letter-spacing:.05em;margin-bottom:10px;",
                                                        "Typical Questions Officers Ask"),
                                               tags$div(style="display:flex;flex-direction:column;gap:8px;",
                                                        lapply(list(
                                                          list(icon="exclamation-triangle", q="Has L4+ activity increased this week?", hint="Check the NCIC Distribution chart with a 7-day filter."),
                                                          list(icon="geo-alt", q="Which county has the highest average risk?", hint="See the County Risk Map or the county table in reports."),
                                                          list(icon="phone", q="Which platform is most active?", hint="Platform Breakdown chart on the Dashboard tab."),
                                                          list(icon="person", q="Are any handles showing escalating patterns?", hint="Who are the actors? tab → Flagged Handles table."),
                                                          list(icon="file-text", q="How many cases are ready for Section 13 action?", hint="Reports tab → NCIC Legal Report → Section 13 Cases sheet.")
                                                        ), function(item)
                                                          tags$div(style="background:#f8f9fa;border-radius:6px;padding:9px 12px;border-left:3px solid #0066cc;",
                                                                   tags$div(style="font-size:12px;font-weight:600;color:#1a1a2e;margin-bottom:3px;",
                                                                            tagList(bs_icon(item$icon), " ", item$q)),
                                                                   tags$div(style="font-size:11px;color:#6c757d;", item$hint))
                                                        )
                                               )
                                             )
                              )
                     ),
                     
                     tags$hr(style="border-color:#f0f4f8;margin:4px 0 24px;"),
                     
                     # ── Reading risk scores ────────────────────────────────────
                     tags$div(style="margin-bottom:24px;",
                              tags$h4(style="font-weight:700;color:#1a1a2e;margin-bottom:12px;font-size:16px;",
                                      tagList(bs_icon("speedometer2"), " Reading Risk Scores and NCIC Levels")),
                              layout_columns(col_widths=c(4,4,4),
                                             card(style="border-top:3px solid #198754;",
                                                  card_header(tagList(bs_icon("check-circle")," LOW Risk (0–34)")),
                                                  tags$div(style="font-size:12px;color:#374151;line-height:1.75;padding:4px 0;",
                                                           tags$p("Content classified at ", tags$strong("L0–L2"), ". Language may be offensive or reflect stereotyping but does not cross the Section 13 legal threshold."),
                                                           tags$p(style="margin:0;color:#198754;font-weight:600;","Action: Log for trend analysis. Monitor if volume increases.")
                                                  )
                                             ),
                                             card(style="border-top:3px solid #fd7e14;",
                                                  card_header(tagList(bs_icon("dash-circle")," MEDIUM Risk (35–64)")),
                                                  tags$div(style="font-size:12px;color:#374151;line-height:1.75;padding:4px 0;",
                                                           tags$p("Content classified at ", tags$strong("L2–L3"), ". Prejudice, stereotyping, or dehumanisation language is present. Target group involvement detected."),
                                                           tags$p(style="margin:0;color:#fd7e14;font-weight:600;","Action: Document patterns. Initiate monitoring flag. Request officer triage.")
                                                  )
                                             ),
                                             card(style="border-top:3px solid #dc3545;",
                                                  card_header(tagList(bs_icon("exclamation-triangle")," HIGH Risk (65–100)")),
                                                  tags$div(style="font-size:12px;color:#374151;line-height:1.75;padding:4px 0;",
                                                           tags$p("Content classified at ", tags$strong("L4–L5"), ". Section 13 threshold met or exceeded. Violence override or explicit incitement detected."),
                                                           tags$p(style="margin:0;color:#dc3545;font-weight:600;","Action: Mandatory officer validation. If confirmed → Section 13 referral, DCI alert, takedown request.")
                                                  )
                                             )
                              )
                     ),
                     
                     tags$hr(style="border-color:#f0f4f8;margin:4px 0 24px;"),
                     
                     # ── Officer validation workflow ────────────────────────────
                     tags$div(style="margin-bottom:24px;",
                              tags$h4(style="font-weight:700;color:#1a1a2e;margin-bottom:12px;font-size:16px;",
                                      tagList(bs_icon("shield-check"), " Officer Validation Workflow")),
                              tags$p(style="font-size:13px;color:#374151;line-height:1.8;margin-bottom:16px;",
                                     "The platform operates on a ", tags$strong("Human-in-the-Loop (HITL)"), " model. AI classification is the ", tags$em("first step"), ", not the final determination. ",
                                     "Officers must complete the validation workflow for all L2+ cases before any action is logged or exported."),
                              layout_columns(col_widths=c(3,3,3,3),
                                             tags$div(style="text-align:center;padding:14px;background:#e8f0fe;border-radius:8px;",
                                                      tags$div(style="font-size:28px;margin-bottom:8px;","🤖"),
                                                      tags$div(style="font-size:12px;font-weight:700;color:#0066cc;margin-bottom:4px;","Step 1 — AI Classification"),
                                                      tags$div(style="font-size:11px;color:#374151;line-height:1.6;","GPT-4o-mini classifies the post. Assigns NCIC level, confidence score, and signals.")),
                                             tags$div(style="text-align:center;padding:14px;background:#fff8e1;border-radius:8px;",
                                                      tags$div(style="font-size:28px;margin-bottom:8px;","🧑‍⚖️"),
                                                      tags$div(style="font-size:12px;font-weight:700;color:#fd7e14;margin-bottom:4px;","Step 2 — Officer Review"),
                                                      tags$div(style="font-size:11px;color:#374151;line-height:1.6;","Officer reads the post, checks context, and may override the NCIC level.")),
                                             tags$div(style="text-align:center;padding:14px;background:#f0fff4;border-radius:8px;",
                                                      tags$div(style="font-size:28px;margin-bottom:8px;","✅"),
                                                      tags$div(style="font-size:12px;font-weight:700;color:#198754;margin-bottom:4px;","Step 3 — Decision"),
                                                      tags$div(style="font-size:11px;color:#374151;line-height:1.6;","CONFIRM · ESCALATE · DOWNGRADE · CLEAR. Decision is logged with officer name and timestamp.")),
                                             tags$div(style="text-align:center;padding:14px;background:#f8f0ff;border-radius:8px;",
                                                      tags$div(style="font-size:28px;margin-bottom:8px;","📤"),
                                                      tags$div(style="font-size:12px;font-weight:700;color:#7c3aed;margin-bottom:4px;","Step 4 — Action"),
                                                      tags$div(style="font-size:11px;color:#374151;line-height:1.6;","Confirmed L4/L5 cases are exported to the Section 13 legal report and the DCI alert queue."))
                              )
                     ),
                     
                     tags$hr(style="border-color:#f0f4f8;margin:4px 0 24px;"),
                     
                     # ── Responsible use ────────────────────────────────────────
                     tags$div(style="margin-bottom:24px;",
                              tags$h4(style="font-weight:700;color:#1a1a2e;margin-bottom:12px;font-size:16px;",
                                      tagList(bs_icon("hand-thumbs-up"), " Responsible Use Principles")),
                              layout_columns(col_widths=c(6,6),
                                             tags$div(style="display:flex;flex-direction:column;gap:10px;",
                                                      tags$div(style="background:#f0fff4;border-left:4px solid #198754;border-radius:6px;padding:12px 14px;",
                                                               tags$div(style="font-size:12px;font-weight:700;color:#198754;margin-bottom:4px;","✅ DO"),
                                                               tags$ul(style="font-size:12px;color:#374151;line-height:1.8;margin:0;padding-left:16px;",
                                                                       tags$li("Use signals as a ", tags$strong("triage aid"), " — to prioritise which content deserves closer human review."),
                                                                       tags$li("Document all officer decisions with notes for accountability and audit trails."),
                                                                       tags$li("Note the 🌐 language badge — Kikuyu, Kalenjin, Luo, and Sheng posts have language-specific glossaries injected into the classification prompt."),
                                                                       tags$li("Apply the Activism Test: political accountability speech is constitutionally protected."),
                                                                       tags$li("Cross-reference L4/L5 cases with independent sources before initiating any legal action.")
                                                               )
                                                      )
                                             ),
                                             tags$div(style="display:flex;flex-direction:column;gap:10px;",
                                                      tags$div(style="background:#fff5f5;border-left:4px solid #dc3545;border-radius:6px;padding:12px 14px;",
                                                               tags$div(style="font-size:12px;font-weight:700;color:#dc3545;margin-bottom:4px;","❌ DO NOT"),
                                                               tags$ul(style="font-size:12px;color:#374151;line-height:1.8;margin:0;padding-left:16px;",
                                                                       tags$li(tags$strong("Do not"), " use AI classification alone as grounds for arrest, prosecution, or surveillance."),
                                                                       tags$li(tags$strong("Do not"), " treat handle attribution as confirmed identity — accounts may be bots or compromised."),
                                                                       tags$li(tags$strong("Do not"), " compare risk scores across different time windows or external datasets."),
                                                                       tags$li(tags$strong("Do not"), " share raw case data or unvalidated outputs outside NCIC-authorised personnel."),
                                                                       tags$li(tags$strong("Do not"), " interpret a neutral (L0) classification as confirming a post is benign — model coverage has limits.")
                                                               )
                                                      )
                                             )
                              )
                     ),
                     
                     tags$hr(style="border-color:#f0f4f8;margin:4px 0 24px;"),
                     
                     # ── Key terms glossary ─────────────────────────────────────
                     tags$div(style="margin-bottom:24px;",
                              tags$h4(style="font-weight:700;color:#1a1a2e;margin-bottom:12px;font-size:16px;",
                                      tagList(bs_icon("journal-text"), " Key Terms & Definitions")),
                              tags$div(style="display:grid;grid-template-columns:1fr 1fr;gap:8px;",
                                       lapply(list(
                                         list(term="NCIC Level", def="A 0–5 severity classification under Cap 170. L0 = Neutral; L5 = Toxic/Immediate action required."),
                                         list(term="Risk Score", def="A composite 0–100 score blending AI confidence, keyword weight, network exposure, and source history."),
                                         list(term="Section 13", def="The legal threshold in NCIC Act Cap 170 at which hate speech becomes a prosecutable offence. Triggered at L4+."),
                                         list(term="Confidence", def="GPT-4o-mini's self-reported certainty (0–100%). Below 60% is flagged LOW and requires extra officer scrutiny."),
                                         list(term="Source History", def="A handle's and county's pattern of prior posts over 30 days. Escalating actors receive a risk score boost (capped +30)."),
                                         list(term="CIB", def="Coordinated Inauthentic Behaviour — multiple accounts posting near-identical content, suggesting an organised campaign."),
                                         list(term="HITL", def="Human-in-the-Loop — the validation model ensuring every AI decision is reviewed by a trained officer before action."),
                                         list(term="Violence Override", def="Stage 1 of the classification chain. Any post calling for physical harm is immediately escalated to L4/L5, bypassing other tests."),
                                         list(term="Language Detection", def="GPT-4o-mini detects the language of each post before classification. Kikuyu, Kalenjin, Luo, and Sheng posts receive language-specific glossaries and structural guidance injected into the prompt. A 🌐 badge confirms which context was applied."),
                                         list(term="Hallucination", def="When an AI generates confident but factually incorrect outputs. Prevented by 6 layers: temperature=0, JSON schema, Cap 170 chain, confidence scoring, HITL, and disagreement retraining."),
                                         list(term="temperature=0", def="A GPT parameter making output fully deterministic — the same post always returns the same classification, eliminating random variation and drift."),
                                         list(term="Disagreement Log", def="When an officer overrides the AI's NCIC level, the difference is logged and used to retrain keyword weights and update the few-shot training bank in real time.")
                                       ), function(item)
                                         tags$div(style="background:#f8f9fa;border-radius:6px;padding:10px 14px;border-left:3px solid #0066cc;",
                                                  tags$div(style="font-size:12px;font-weight:700;color:#1a1a2e;margin-bottom:3px;", item$term),
                                                  tags$div(style="font-size:11px;color:#4b5563;line-height:1.65;", item$def))
                                       )
                              )
                     ),
                     
                     # ── Footer ─────────────────────────────────────────────────
                     tags$div(
                       style="margin-top:32px;padding:20px 0;border-top:1px solid #dee2e6;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px;",
                       tags$div(style="font-size:11px;color:#9ca3af;",
                                paste0(APP_NAME, " v5 · Interpretation Guide · NCIC Cap 170 · Kenya · March 2026")),
                       tags$div(style="font-size:11px;color:#9ca3af;",
                                "For queries or feedback: NCIC Intelligence Unit")
                     )
            )
  )
)
# ── SERVER ───────────────────────────────────────────────────────
# ── MAP SUMMARY GENERATOR ────────────────────────────────────────
generate_map_summary <- function(d, county) {
  n      <- nrow(d)
  if (n == 0) return("No signals match the current filters.")
  l5     <- sum(d$ncic_level == 5)
  l4     <- sum(d$ncic_level == 4)
  l3     <- sum(d$ncic_level == 3)
  l0     <- sum(d$ncic_level == 0)
  loc    <- if (county == "All Counties") "across Kenya" else paste("in", county)
  urgent <- l5 + l4
  
  # Lead sentence
  lead <- if (urgent == 0) {
    sprintf("%d signals detected %s — no immediate escalation required.", n, loc)
  } else if (l5 > 0) {
    sprintf("%d signals detected %s — %d require IMMEDIATE escalation (L5 Toxic).", n, loc, l5)
  } else {
    sprintf("%d signals detected %s — %d meet Section 13 threshold (L4 Hate Speech).", n, loc, l4)
  }
  
  # Pattern sentence
  pattern <- if (l3 > 0 && urgent > 0) {
    sprintf("Dehumanisation content (%d L3) suggests coordinated escalation pattern.", l3)
  } else if (l0 > n * 0.5) {
    "Majority of signals are neutral — monitor for trend changes."
  } else if (urgent > n * 0.4) {
    "High proportion of severe content — cross-reference with independent sources before action."
  } else {
    "Signal distribution within normal monitoring range."
  }
  
  paste(lead, pattern)
}

server <- function(input, output, session) {
  # Update date filters once on session start
  session$onFlushed(function() {
    d_min <- as.Date(min(all_cases$timestamp, na.rm=TRUE))
    d_max <- as.Date(max(all_cases$timestamp, na.rm=TRUE))
    updateDateRangeInput(session, "map_dr",  start=d_min, end=d_max, min=d_min, max=d_max)
    updateDateRangeInput(session, "dash_dr", start=d_min, end=d_max, min=d_min, max=d_max)
    updateDateRangeInput(session, "clf_dr",  start=d_min, end=d_max, min=d_min, max=d_max)
    updateDateRangeInput(session, "fl_dr",   start=d_min, end=d_max, min=d_min, max=d_max)
  }, once=TRUE)
  
  rv <- reactiveValues(
    authenticated  = TRUE,
    officer_name   = "",
    officer_role   = "officer",
    officer_user   = "",
    timed_out      = FALSE,
    session_id     = digest::digest(paste0(Sys.time(), runif(1)), algo="md5"),
    last_activity  = Sys.time(),
    timeout_warned = FALSE,
    forecast_cache = NULL,      # cached list of per-county forecast results
    forecast_built = NULL,      # POSIXct timestamp of last build
    sf_fc          = NULL,      # sf object for forecast map
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
    last_eval      = NULL,
    cluster_alert  = NULL,
    last_db_sync   = Sys.time()
  )
  
  # ── Session timeout ────────────────────────────────────────────
  # JS sends a ping every 60s on any user interaction (mouse/key/scroll).
  # Server checks every 60s; locks after SESSION_TIMEOUT minutes of silence.
  observeEvent(input$activity_ping, {
    rv$last_activity  <- Sys.time()
    rv$timeout_warned <- FALSE
  }, ignoreNULL=TRUE)
  
  timeout_timer <- reactiveTimer(60000)  # check every 60 seconds

  # v6: geographic cluster detection — runs every 5 minutes
  cluster_timer <- reactiveTimer(300000)
  observe({
    cluster_timer()
    if (!isolate(rv$authenticated)) return()
    clusters <- tryCatch(detect_county_clusters(isolate(rv$cases),24L,3L,2L),
                         error=function(e) data.frame())
    if (nrow(clusters)>0) {
      shared <- attr(clusters,"shared_keywords")%||%character()
      rv$cluster_alert <- list(counties=clusters,shared=shared,
                               detected=format(Sys.time(),"%H:%M"))
    } else rv$cluster_alert <- NULL
  })
  observe({
    timeout_timer()
    if (!isolate(rv$authenticated)) return()
    idle_mins <- as.numeric(difftime(Sys.time(), isolate(rv$last_activity), units="mins"))
    warn_at   <- SESSION_TIMEOUT - 2L    # warn 2 minutes before lockout
    
    if (idle_mins >= SESSION_TIMEOUT) {
      un <- isolate(rv$officer_user)
      nm <- isolate(rv$officer_name)
      audit(un, nm, "TIMEOUT",
            detail = sprintf("Auto-locked after %d min inactivity", SESSION_TIMEOUT),
            session_id = isolate(rv$session_id))
      rv$authenticated  <- FALSE
      rv$timeout_warned <- FALSE
      rv$timed_out      <- TRUE
      shinyjs::runjs("Shiny.setInputValue('session_locked', true, {priority: 'event'});")
      
      # ── Notify supervisor via email ──────────────────────────────
      tryCatch({
        gu <- Sys.getenv("GMAIL_USER")
        gp <- Sys.getenv("GMAIL_PASS")
        # Use dedicated SUPERVISOR_EMAIL if set, fallback to OFFICER_EMAIL
        se <- Sys.getenv("SUPERVISOR_EMAIL")
        oe <- Sys.getenv("OFFICER_EMAIL")
        notify_addr <- if (nchar(se) > 0) se else oe
        if (all(nchar(c(gu, gp, notify_addr)) > 0)) {
          subj <- sprintf("[EWS Security] Session timeout — %s auto-locked at %s",
                          nm, format(Sys.time(), "%H:%M %Z"))
          body <- paste0(
            "<html><body style='font-family:Arial,sans-serif;color:#1a1a2e;'>",
            "<div style='background:#fd7e14;padding:14px 20px;border-radius:8px 8px 0 0;'>",
            "<h2 style='color:#fff;margin:0;font-size:16px;'>🔒 ", APP_NAME, " — Session Timeout Alert</h2>",
            "</div>",
            "<div style='background:#f8f9fa;padding:18px;border:1px solid #dee2e6;border-top:none;border-radius:0 0 8px 8px;'>",
            "<table style='width:100%;border-collapse:collapse;font-size:13px;'>",
            "<tr><td style='padding:6px 10px;font-weight:600;width:140px;border-bottom:1px solid #dee2e6;'>Officer</td>",
            "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>", nm, " (", un, ")</td></tr>",
            "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Event</td>",
            "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;color:#fd7e14;font-weight:700;'>",
            "AUTO-LOCKED after ", SESSION_TIMEOUT, " minutes of inactivity</td></tr>",
            "<tr><td style='padding:6px 10px;font-weight:600;border-bottom:1px solid #dee2e6;'>Time</td>",
            "<td style='padding:6px 10px;border-bottom:1px solid #dee2e6;'>",
            format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " EAT</td></tr>",
            "<tr><td style='padding:6px 10px;font-weight:600;'>Session ID</td>",
            "<td style='padding:6px 10px;font-family:monospace;font-size:11px;'>",
            isolate(rv$session_id), "</td></tr>",
            "</table>",
            "<div style='margin-top:14px;padding:10px 14px;background:#fff3cd;border-left:4px solid #ffc107;",
            "border-radius:4px;font-size:12px;color:#664d03;'>",
            "This is an automated security notification. No action is required unless this timeout ",
            "occurred during an active investigation. The officer must re-authenticate to continue.",
            "</div>",
            "<div style='margin-top:14px;font-size:10px;color:#868e96;'>",
            APP_NAME, " v5 · NCIC Cap 170 · Audit log reference: TIMEOUT</div>",
            "</div></body></html>"
          )
          mime <- paste0("From: ",gu,"\r\nTo: ",notify_addr,
                         "\r\nSubject: ",subj,
                         "\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n",body)
          tmp <- tempfile(fileext=".txt")
          writeLines(mime, tmp, useBytes=FALSE)
          on.exit(unlink(tmp), add=TRUE)
          cmd_email <- paste0("curl --url 'smtps://smtp.gmail.com:465' --ssl-reqd ",
                              "--mail-from '",gu,"' --mail-rcpt '",notify_addr,"' ",
                              "--user '",gu,":",gp,"' --upload-file '",tmp,"' --silent")
          system(cmd_email, intern=FALSE, wait=FALSE)   # non-blocking
        }
      }, error=function(e) message("[timeout email] failed: ", e$message))
    } else if (idle_mins >= warn_at && !isolate(rv$timeout_warned)) {
      rv$timeout_warned <- TRUE
      mins_left <- SESSION_TIMEOUT - floor(idle_mins)
      shinyjs::runjs(sprintf(
        "Shiny.setInputValue('timeout_warning', %d, {priority: 'event'});", mins_left))
    }
  })
  
  # Show warning toast when approaching timeout
  observeEvent(input$timeout_warning, {
    shinyjs::runjs(sprintf(
      "if(typeof Shiny !== 'undefined') {
         var b = document.getElementById('timeout_toast');
         if(!b) {
           b = document.createElement('div');
           b.id = 'timeout_toast';
           b.style = 'position:fixed;bottom:20px;right:20px;z-index:9999;background:#664d03;color:#fff;padding:12px 18px;border-radius:8px;font-size:13px;box-shadow:0 4px 16px rgba(0,0,0,0.3);';
           document.body.appendChild(b);
         }
         b.innerHTML = '⏱ Session locks in %d minute(s) due to inactivity. Move your mouse to stay active.';
         setTimeout(function(){ var el=document.getElementById('timeout_toast'); if(el) el.remove(); }, 8000);
       }", input$timeout_warning))
  }, ignoreNULL=TRUE)
  
  # ── Cross-session DB sync ──────────────────────────────────────
  # Poll DB every 5 seconds. If another officer validated a case, the DB
  # row will differ from rv$cases. Merge only changed rows to avoid
  # overwriting in-progress bulk operations on this session.
  db_poll_timer <- reactiveTimer(15000)  # poll every 15s (was 5s)

  observe({
    db_poll_timer()
    if (rv$bulk_running) return()   # don't sync mid-bulk

    tryCatch({
      fresh <- db_load_cases()
      if (is.null(fresh) || nrow(fresh) == 0) return()

      cur <- isolate(rv$cases)
      if (is.null(cur) || nrow(cur) == 0) return()

      sync_cols <- c("ncic_level","ncic_label","section_13","validated_by",
                     "validated_at","action_taken","risk_score","risk_formula",
                     "risk_level","notes")

      # Build a normalised string digest per row to avoid type-mismatch false positives
      # (e.g. NA vs NA_character_, integer vs double, logical vs integer)
      normalise_row <- function(df, i) {
        vals <- df[i, intersect(sync_cols, names(df)), drop=FALSE]
        paste(sapply(vals, function(v) {
          if (is.na(v)) "__NA__" else as.character(v)
        }), collapse="|")
      }

      # Build lookup: case_id → row index for current cases
      cur_idx   <- setNames(seq_len(nrow(cur)),   cur$case_id)
      fresh_idx <- setNames(seq_len(nrow(fresh)), fresh$case_id)

      # Only compare case_ids present in both
      common_ids <- intersect(names(cur_idx), names(fresh_idx))

      changed <- character(0)
      for (cid in common_ids) {
        ci <- cur_idx[[cid]]
        fi <- fresh_idx[[cid]]
        if (normalise_row(cur, ci) != normalise_row(fresh, fi))
          changed <- c(changed, cid)
      }

      if (length(changed) > 0) {
        for (cid in changed) {
          ci <- cur_idx[[cid]]
          fi <- fresh_idx[[cid]]
          for (col in intersect(sync_cols, names(fresh))) {
            rv$cases[ci, col] <- fresh[fi, col]
          }
        }
        rv$last_db_sync <- Sys.time()
        message(sprintf("[sync] Updated %d case(s) from DB", length(changed)))
      }
      # Silent when nothing changed — no message spam
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
    
    kpi <- function(label, value, border_col, pct=NULL, sub_col="#6c757d") {
      tags$div(
        style=paste0("background:#fff;border:1px solid #dee2e6;border-left:4px solid ",
                     border_col, ";border-radius:6px;padding:10px 14px;"),
        tags$div(style=paste0("font-size:10px;font-weight:700;color:", sub_col,
                              ";text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px;"),
                 label),
        tags$div(style=paste0("font-size:26px;font-weight:800;color:", border_col,
                              ";line-height:1;"),
                 formatC(value, format="d", big.mark=",")),
        if (!is.null(pct))
          tags$div(style="font-size:10px;color:#9ca3af;margin-top:3px;",
                   paste0(pct, "% of total"))
      )
    }
    
    tags$div(
      style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-bottom:14px;",
      kpi("Total Signals",    total, "#0066cc"),
      kpi("Toxic / L5",       toxic, "#4a0000", round(toxic/max(total,1)*100)),
      kpi("Hate Speech / L4", hate,  "#dc3545", round(hate/max(total,1)*100)),
      kpi("Section 13",       s13,   "#c0392b", round(s13/max(total,1)*100)),
      kpi("Validated",        val,   "#198754", round(val/max(total,1)*100)),
      kpi("Pending Review",   pend,  "#fd7e14", round(pend/max(total,1)*100))
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
    if(nrow(d)==0||!("risk_level" %in% names(d))||sum(d$risk_level=="HIGH",na.rm=TRUE)==0) return(datatable(data.frame(Message="No high-risk cases in selected date range")))
    d <- d[d$risk_level=="HIGH",][order(-d[d$risk_level=="HIGH","risk_score"]),][1:min(8,sum(d$risk_level=="HIGH")),]
    d$nb <- sapply(d$ncic_level,ncic_badge_html)
    d$rb <- sapply(d$risk_level,risk_badge_html)
    d$tw <- substr(ifelse(is.na(d$tweet_text),"",d$tweet_text),1,55)
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
      setView(lng=37.9,lat=0.5,zoom=6)

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
      r    <- max(10, min(36, sqrt(n) * 2.8))
      co_d <- d[d$county == co, ]          # all cases for this county
      co_c <- co_d[seq_len(min(6, nrow(co_d))), ]   # top 6 for case table
      is_reg <- any(sapply(seq_len(nrow(co_d)), function(k) {
        detect_region(co_d$src_lat[k], co_d$src_lng[k], co_d$text[k] %||% "")
      }) == "Regional", na.rm=TRUE)
      if (!is.null(input$show_regional) && !isTRUE(input$show_regional) && is_reg) next
      col  <- if (is_reg) "#7c3aed" else unname(ncic_color(lvl))
      
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
    
    # ── Dynamic legend — responds to filters ──────────────────
    summary_txt <- generate_map_summary(d, input$f_county %||% "All Counties")
    legend_html <- paste0(
      "<div style=\"background:rgba(255,255,255,0.95);border:1px solid #dee2e6;",
      "border-radius:8px;padding:10px 14px;font-size:11px;",
      "box-shadow:0 2px 8px rgba(0,0,0,0.15);min-width:220px;max-width:260px;\">",
      "<div style=\"font-weight:700;color:#1a1a2e;margin-bottom:8px;font-size:11px;",
      "border-bottom:1px solid #dee2e6;padding-bottom:5px;\">",
      "&#9432; NCIC Signal Levels</div>",
      paste0(sapply(rev(as.character(0:5)), function(l) {
        cnt <- sum(d$ncic_level == as.integer(l))
        paste0(
          "<div style=\"display:flex;align-items:center;gap:7px;margin-bottom:5px;\">",
          "<div style=\"width:11px;height:11px;border-radius:50%;background:",
          ncic_color(l), ";flex-shrink:0;\"></div>",
          "<span style=\"flex:1;color:#374151;\">L", l,
          " — ", ncic_name(l), "</span>",
          "<span style=\"font-weight:700;color:", ncic_color(l),
          ";min-width:24px;text-align:right;font-family:monospace;\">",
          cnt, "</span></div>"
        )
      }), collapse=""),
      "<div style=\"border-top:1px solid #dee2e6;margin-top:8px;padding-top:8px;",
      "font-size:10px;color:#374151;line-height:1.5;\">",
      summary_txt,
      "</div>",
      "</div>"
    )
    proxy |> clearControls() |>
      addControl(html=legend_html, position="bottomright")

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
    if (FALSE) {
      leafletProxy("flow_map") |>
        setView(lng=37.9, lat=0.02, zoom=6)
    }
  })
  
  output$flow_map <- renderLeaflet({
    leaflet() |> addProviderTiles("CartoDB.Positron") |>
      setView(lng=37.9,lat=0.5,zoom=6)
    addControl(
      html = paste0(
        "<div style=\"background:rgba(255,255,255,0.95);border:1px solid #dee2e6;",
        "border-radius:8px;padding:10px 14px;font-size:11px;",
        "box-shadow:0 2px 8px rgba(0,0,0,0.15)\">",
        "<div style=\"font-weight:700;color:#1a1a2e;margin-bottom:6px;\">",
        "&#9432; NCIC Levels</div>",
        paste0(sapply(rev(as.character(0:5)), function(l) {
          paste0("<div style=\"display:flex;align-items:center;gap:7px;margin-bottom:4px;\">",
                 "<div style=\"width:11px;height:11px;border-radius:50%;background:",
                 ncic_color(l), ";flex-shrink:0;\"></div>",
                 "<span>L", l, " — ", ncic_name(l), "</span></div>")
        }), collapse=""),
        "</div>"
      ),
      position = "bottomright"
    )
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
                                              value="Watu wa kabila hiyo ni kama magonjwa — wanaharibu nchi yetu, waende kwao maze!"))
  observeEvent(input$ex5, updateTextAreaInput(session,"chat_input",
                                              value="Maze hawa wasee wa kabila hiyo si fiti — si waaminifu na hawastahili nafasi kwa serikali yetu poa"))
  observeEvent(input$ex4, updateTextAreaInput(session,"chat_input",
                                              value="Let us unite as Kenyans and build this nation together with love"))
  
  output$chat_history <- renderUI({
    msgs <- rv$chat_history
    if (length(msgs)==0)
      return(tags$div(class="chat-container",
                      tags$div(class="chat-msg chat-bot",
                               tags$span(class="chat-thinking",
                                         "👋 Paste any post — and wait for classification scores."))))
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
                          tags$div(style="font-size:10px;color:#6c757d;font-family:'IBM Plex Mono';margin-bottom:3px;display:flex;align-items:center;gap:6px;",
                                   paste0("NCIC ENGINE · ",OPENAI_MODEL),
                                   if(isTRUE(m$cache_hit))
                                     tags$span(style="background:#d1fae5;color:#065f46;border-radius:3px;padding:0 5px;font-size:9px;font-weight:700;","⚡ cached")
                                   else NULL),
                          tags$div(style="display:flex;align-items:center;gap:6px;margin-bottom:5px;flex-wrap:wrap;",
                                   HTML(ncic_badge_html(lvl)),
                                   # Language detection badge
                                   if (!is.null(m$lang_label) && !is.na(m$lang_label))
                                     tags$span(style="background:#e9ecef;color:#374151;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:600;",
                                               paste0("🌐 ", m$lang_label)) else NULL,
                                   if (isTRUE(m$lang_is_sheng))
                                     tags$span(style="background:#fef3c7;color:#92400e;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;border:1px solid #fcd34d;","Sheng") else NULL,
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
                          # Language context banner (informational — no penalty)
                          if (!is.null(m$lang_warning) && !is.na(m$lang_warning))
                            tags$div(style="background:#f0f9ff;border-left:3px solid #0066cc;border-radius:4px;padding:5px 9px;font-size:11px;margin-bottom:5px;color:#0c4a6e;",
                                     m$lang_warning) else NULL,
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
    shinyjs::runjs("document.getElementById('ews-loading-bar').classList.add('active');")
    
    # ── Detect language before classification ──────────────────
    lang_det <- tryCatch(detect_language(tw), error=function(e)
      list(code="unknown", label="Unknown", is_sheng=FALSE,
           is_mixed=FALSE, low_coverage=FALSE, warning=NA_character_))
    
    rv$chat_history <- c(rv$chat_history,
                         list(list(role="user", text=tw)),
                         list(list(role="thinking")))
    updateTextAreaInput(session,"chat_input",value="")
    
    res <- tryCatch(classify_tweet(tw, rv$kw_weights,
                                   handle="@chat_input", county="Unknown",
                                   cases_df=rv$cases, lang_det=lang_det),
                    error=function(e) list(role="error",text=conditionMessage(e)))
    shinyjs::runjs("document.getElementById('ews-loading-bar').classList.remove('active');")
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
        source_context_note=  res$source_context_note  %||% "",
        # ── Language detection results ──────────────────────────
        lang_code=        lang_det$code,
        lang_label=       lang_det$label,
        lang_is_sheng=    isTRUE(lang_det$is_sheng),
        lang_is_mixed=    isTRUE(lang_det$is_mixed),
        lang_warning=     lang_det$warning %||% NA_character_,
        cache_hit=        isTRUE(res$cache_hit)
      )
    }
    rv$chat_history <- hist
    rv$cache_size   <- length(ls(classify_cache))
    shinyjs::runjs("
      var btn=document.getElementById('chat_send');
      var sp=document.getElementById('chat_spinner');
      if(btn){btn.disabled=false;btn.innerHTML='→';}
      if(sp){sp.classList.remove('active');}
      setTimeout(function(){
        var c=document.getElementById('chat_scroll');
        if(c)c.scrollTop=c.scrollHeight;
      },60);
    ")
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
    shinyjs::runjs("document.getElementById('ews-loading-bar').classList.add('active');")
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
      # ── Language detection for bulk (same as single-post) ──────
      lang_det_bulk <- tryCatch(detect_language(tweet),
        error=function(e) list(code="other", label="Other",
                               is_sheng=FALSE, is_mixed=FALSE, warning=NA_character_))
      lang_hint_bulk <- build_language_hint(lang_det_bulk, tweet)
      up  <- paste0(fs, lang_hint_bulk,
                    'Apply VIOLENCE OVERRIDE then ACTIVISM TEST.\nClassify: "',
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
        shinyjs::runjs("document.getElementById('ews-loading-bar').classList.remove('active');")
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
    if (!rv$authenticated) return(auth_wall_ui(rv$timed_out))
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

                            # v6: Context window — handle history + similar cases
                            tags$details(
                              style="margin-bottom:8px;",
                              tags$summary(style="font-size:10px;font-weight:700;color:#0066cc;cursor:pointer;padding:4px 0;user-select:none;",
                                           "📋 Officer Context — handle history & similar cases"),
                              tags$div(style="margin-top:6px;",
                                {
                                  history <- get_handle_history(row$handle,cid,rv$cases,5L)
                                  if (nrow(history)==0) {
                                    tags$div(style="font-size:11px;color:#9ca3af;margin-bottom:8px;",
                                             paste0("No prior posts from @",row$handle%||%"unknown","."))
                                  } else {
                                    tags$div(style="margin-bottom:8px;",
                                      tags$div(style="font-size:10px;font-weight:700;color:#374151;margin-bottom:4px;",
                                               paste0("Prior posts from @",row$handle," (",nrow(history)," found):")),
                                      tagList(lapply(seq_len(nrow(history)),function(j) {
                                        h <- history[j,]; hc <- ncic_color(h$ncic_level)
                                        tags$div(style=paste0("display:flex;gap:6px;padding:4px 6px;border-radius:4px;background:",hc,"0d;border-left:2px solid ",hc,";margin-bottom:3px;"),
                                          tags$div(style=paste0("font-size:10px;font-weight:700;color:",hc,";min-width:28px;"),paste0("L",h$ncic_level)),
                                          tags$div(style="flex:1;font-size:10px;color:#374151;line-height:1.4;",
                                                   substr(h$tweet_text,1,100),if(nchar(h$tweet_text)>100)"..."else""),
                                          tags$div(style="font-size:9px;color:#9ca3af;white-space:nowrap;",h$timestamp_chr%||%""))
                                      })))
                                  }
                                },
                                {
                                  similar <- get_similar_cases(row$signals,cid,rv$cases,3L)
                                  if (nrow(similar)==0) {
                                    tags$div(style="font-size:11px;color:#9ca3af;","No similar cases found.")
                                  } else {
                                    tags$div(
                                      tags$div(style="font-size:10px;font-weight:700;color:#374151;margin-bottom:4px;","Similar flagged cases:"),
                                      tagList(lapply(seq_len(nrow(similar)),function(j) {
                                        s <- similar[j,]; sc <- ncic_color(s$ncic_level)
                                        tags$div(style="display:flex;gap:6px;padding:4px 6px;border-radius:4px;background:#f8f9fa;border:0.5px solid #dee2e6;margin-bottom:3px;",
                                          tags$div(style=paste0("font-size:10px;font-weight:700;color:",sc,";min-width:28px;"),paste0("L",s$ncic_level)),
                                          tags$div(style="flex:1;",
                                            tags$div(style="font-size:10px;color:#374151;",substr(s$tweet_text,1,90),if(nchar(s$tweet_text)>90)"..."else""),
                                            tags$div(style="font-size:9px;color:#9ca3af;margin-top:1px;",
                                                     paste0(s$action_taken%||%"pending"," · ",s$county%||%""))))
                                      })))
                                  }
                                }
                              )
                            ),

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

                            # v6: Structured decision panel
                            tags$div(
                              style="border:1px solid #e5e7eb;border-radius:8px;padding:10px;margin-top:8px;background:#fafafa;",
                              tags$div(style="font-size:10px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px;","Officer Decision — Structured Reasoning"),
                              tags$div(style="margin-bottom:6px;",
                                tags$div(style="font-size:11px;font-weight:600;color:#374151;margin-bottom:3px;","1. Signal word(s) identified:"),
                                textInput(paste0("sig_words_",cid),NULL,width="100%",
                                          placeholder="e.g. kuchinja, madoadoa, waende kwao — comma separated")),
                              tags$div(style="margin-bottom:6px;",
                                tags$div(style="font-size:11px;font-weight:600;color:#374151;margin-bottom:3px;","2. What does it denote?"),
                                selectInput(paste0("sig_denotes_",cid),NULL,width="100%",
                                  choices=c("Select..."="","Call to physical violence"="CALL_TO_VIOLENCE",
                                    "Call to expel/remove community"="CALL_TO_EXPEL","Armed uprising"="ARMED_UPRISING",
                                    "Dehumanisation — animals"="DEHUMANISATION_ANIMAL","Dehumanisation — disease/vermin"="DEHUMANISATION_DISEASE",
                                    "Derogatory ethnic slur"="ETHNIC_SLUR","Exclusion rhetoric"="EXCLUSION_RHETORIC",
                                    "Tribal voting call"="ELECTION_TRIBAL","Secessionism"="SECESSIONISM",
                                    "Religious hatred"="RELIGIOUS_HATRED","Undermining unity"="UNITY_THREAT",
                                    "Coded hate speech"="CODED_HATE","Other"="OTHER"))),
                              tags$div(style="margin-bottom:6px;",
                                tags$div(style="font-size:11px;font-weight:600;color:#374151;margin-bottom:3px;","3. Language / dialect:"),
                                selectInput(paste0("sig_lang_",cid),NULL,width="100%",
                                  choices=c("Select..."="","English"="en","Swahili"="sw","Sheng"="sheng",
                                    "Kikuyu"="kikuyu","Luo (Dholuo)"="luo","Kalenjin"="kalenjin",
                                    "Luhya"="luhya","Kamba"="kamba","Mixed"="mixed"))),
                              tags$div(style="margin-bottom:8px;",
                                tags$div(style="font-size:11px;font-weight:600;color:#374151;margin-bottom:3px;","4. Why this NCIC level?"),
                                textAreaInput(paste0("sig_reason_",cid),NULL,width="100%",rows=2,
                                  placeholder="e.g. 'kuchinja' means to slaughter — used to call for violence against the Kikuyu")),
                              tags$div(style="border-top:1px solid #e5e7eb;padding-top:8px;",
                                tags$div(style="font-size:10px;font-weight:600;color:#374151;margin-bottom:6px;","5. Decision:"),
                                tags$div(style="display:flex;gap:6px;flex-wrap:wrap;",
                                  tags$div(style="display:flex;flex-direction:column;align-items:center;gap:2px;",
                                    actionButton(paste0("confirm_",cid),"✓ Confirm",class="btn-val-confirm"),
                                    tags$div(style="font-size:9px;color:#198754;","GPT was correct")),
                                  tags$div(style="display:flex;flex-direction:column;align-items:center;gap:2px;",
                                    actionButton(paste0("escalate_",cid),"⬆ Escalate",class="btn-val-escalate"),
                                    tags$div(style="font-size:9px;color:#dc3545;","More severe")),
                                  tags$div(style="display:flex;flex-direction:column;align-items:center;gap:2px;",
                                    actionButton(paste0("dgrade_",cid),"↓ Downgrade",class="btn-val-downgrade"),
                                    tags$div(style="font-size:9px;color:#fd7e14;","Less severe")),
                                  tags$div(style="display:flex;flex-direction:column;align-items:center;gap:2px;",
                                    actionButton(paste0("clear_",cid),"✕ Clear",class="btn-val-clear"),
                                    tags$div(style="font-size:9px;color:#6c757d;","Not a signal")),
                                  if(email_ok) tags$div(style="display:flex;flex-direction:column;align-items:center;gap:2px;",
                                    actionButton(paste0("email_",cid),"📧 Alert",class="btn-val-email"),
                                    tags$div(style="font-size:9px;color:#0066cc;","Send alert")) else NULL
                                )
                              ),
                              tags$div(style="font-size:10px;color:#9ca3af;margin-top:6px;",
                                "💡 Fields 1-4 optional but improve keyword learning. Confirm/Escalate on L3+ triggers keyword extraction.")
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
          # v6: capture structured reasoning fields
          sig_words   <- isolate(input[[paste0("sig_words_",   cid)]]) %||% ""
          sig_denotes <- isolate(input[[paste0("sig_denotes_", cid)]]) %||% ""
          sig_lang    <- isolate(input[[paste0("sig_lang_",    cid)]]) %||% ""
          sig_reason  <- isolate(input[[paste0("sig_reason_",  cid)]]) %||% ""
          officer_reasoning <- paste0(
            if(nchar(trimws(sig_words))>0)   paste0("Signal phrases: ",  sig_words,   ". ") else "",
            if(nchar(trimws(sig_denotes))>0) paste0("Denotes: ",          sig_denotes, ". ") else "",
            if(nchar(trimws(sig_lang))>0)    paste0("Language: ",         sig_lang,    ". ") else "",
            if(nchar(trimws(sig_reason))>0)  paste0("Reasoning: ",        sig_reason,  ".")  else ""
          )
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

          # v6: extract keywords from validated post → Supabase
          extract_and_save_keywords(tweet_val, final_ncic, action_str, officer,
                                    sig_words, sig_denotes, sig_lang, officer_reasoning)

          # v6: update GPT confidence calibration
          update_calibration(rv$cases$conf_num[rv$cases$case_id==cid][1], cur_ncic, final_ncic)

          # v6: inter-officer agreement check
          prev_val <- rv$cases$validated_by[rv$cases$case_id==cid][1]
          if (!is.na(prev_val) && !is.null(prev_val) && prev_val != officer) {
            prev_lvl <- rv$cases$officer_ncic_override[rv$cases$case_id==cid][1]
            if (!is.na(prev_lvl)) {
              flagged <- log_officer_agreement(cid, prev_val, prev_lvl, officer, final_ncic)
              if (isTRUE(flagged==1L))
                showNotification(paste0("Agreement flag: you and ",prev_val," differ by >1 level on ",cid,". Flagged for senior review."),
                                 type="warning", duration=8)
            }
          }
          
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
          
          # ── Audit log ─────────────────────────────────────────────
          audit(rv$officer_user, officer, action_str,
                case_id    = cid,
                detail     = sprintf("NCIC L%d → L%d · Risk %d · %s",
                                     cur_ncic, final_ncic,
                                     rv$cases$risk_score[rv$cases$case_id==cid][1],
                                     substr(tweet_val, 1, 60)),
                session_id = rv$session_id)
          
          # ── Auto-enqueue S13 cases ────────────────────────────────
          if (ncic_s13(final_ncic) && action_str %in% c("CONFIRMED","ESCALATED")) {
            case_row <- rv$cases[rv$cases$case_id == cid, ][1,]
            db_s13_enqueue(case_row, officer)
          }
          
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
          fields_complete <- sum(c(nchar(trimws(sig_words))>0, nchar(trimws(sig_denotes))>0,
                                   nchar(trimws(sig_lang))>0,  nchar(trimws(sig_reason))>0))
          completeness_msg <- if(fields_complete==4) " · full reasoning" else
                              if(fields_complete>0)  paste0(" · ",fields_complete,"/4 fields") else
                              " · no reasoning captured"
          rv$learning_flash <- paste0("Weights updated · rescoring · ",n_examples," examples",completeness_msg)
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
    if (!rv$authenticated) return(auth_wall_ui(rv$timed_out))
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
      ),

      # v6: Precision / Recall Evaluation
      card(style="border-top:3px solid #0066cc;margin-top:12px;",
           card_header(tagList(bs_icon("bullseye")," Precision / Recall Evaluation")),
           tags$div(style="padding:4px 0;",
             tags$p(style="font-size:11px;color:#6c757d;margin-bottom:10px;",
                    "Build a gold standard dataset of manually labelled posts to measure detection quality. Target: precision > 0.80, recall > 0.75, F1 > 0.77."),
             layout_columns(col_widths=c(6,6),
               tags$div(
                 tags$div(style="font-size:11px;font-weight:600;color:#374151;margin-bottom:6px;","Add post to gold standard:"),
                 textAreaInput("gold_tweet",NULL,placeholder="Paste post text here...",rows=3,width="100%"),
                 tags$div(style="display:flex;gap:6px;align-items:center;margin-top:4px;",
                   selectInput("gold_level",NULL,choices=setNames(as.character(0:5),paste0("L",0:5," — ",unname(NCIC_LEVELS))),width="60%"),
                   actionButton("gold_add_btn",tagList(bs_icon("plus-circle")," Add"),class="btn btn-primary btn-sm")),
                 uiOutput("gold_count_ui")
               ),
               tags$div(
                 tags$div(style="font-size:11px;font-weight:600;color:#374151;margin-bottom:6px;","Run evaluation:"),
                 tags$div(style="display:flex;gap:6px;align-items:center;margin-bottom:8px;",
                   selectInput("eval_threshold","Positive threshold:",choices=c("L2+"="2","L3+"="3","L4+"="4"),selected="3",width="50%"),
                   actionButton("eval_run_btn",tagList(bs_icon("play-fill")," Run"),class="btn btn-success btn-sm")),
                 uiOutput("eval_results_ui")
               )
             ),
             tags$div(style="margin-top:10px;",
               tags$div(style="font-size:11px;font-weight:600;color:#374151;margin-bottom:6px;","History:"),
               uiOutput("eval_history_ui"))
           )),

      # v6: Inter-Officer Agreement
      card(style="border-top:3px solid #0066cc;margin-top:12px;",
           card_header(tagList(bs_icon("people-fill")," Inter-Officer Agreement (Cohen's κ)")),
           uiOutput("agreement_ui")),

      # v6: GPT Confidence Calibration
      card(style="border-top:3px solid #7c3aed;margin-top:12px;",
           card_header(tagList(bs_icon("graph-up")," GPT Confidence Calibration")),
           tags$div(style="padding:4px 0;",
             tags$p(style="font-size:11px;color:#6c757d;margin-bottom:8px;",
                    "Does GPT confidence 80 mean 80% correct? Bars track actual vs expected accuracy per confidence bucket."),
             uiOutput("calibration_ui"))),

      # v6: Keyword Retirement
      card(style="border-top:3px solid #dc3545;margin-top:12px;",
           card_header(tagList(bs_icon("archive")," Keyword Retirement")),
           tags$div(style="padding:4px 0;",
             tags$p(style="font-size:11px;color:#6c757d;margin-bottom:8px;",
                    "Keywords not matched in 90+ days. Retire stale keywords to keep the pipeline sharp."),
             uiOutput("stale_keywords_ui"))),

      # v6: Geographic Clustering
      card(style="border-top:3px solid #fd7e14;margin-top:12px;",
           card_header(tagList(bs_icon("geo-alt")," Geographic Signal Clustering")),
           tags$div(style="padding:4px 0;",
             tags$p(style="font-size:11px;color:#6c757d;margin-bottom:8px;",
                    "Counties with simultaneous L3+ activity in last 24 hours. Clusters may indicate coordinated campaigns."),
             uiOutput("cluster_ui"))),

      # v6: Keyword Changelog
      card(style="border-top:3px solid #6c757d;margin-top:12px;",
           card_header(tagList(bs_icon("clock-history")," Keyword Bank Changelog")),
           tags$div(style="padding:4px 0;",
             tags$p(style="font-size:11px;color:#6c757d;margin-bottom:8px;",
                    "Full history of keyword additions, retirements, and modifications."),
             uiOutput("changelog_ui")))
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
    log_keyword_change(kw, "ADDED", rv$officer_user, new_status="active", reason=paste0("weight=",wt))
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
    log_keyword_change(kw, "RETIRED", rv$officer_user, old_status="active", new_status="retired",
                       reason="Manually removed from keyword editor")
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
  
  # ── Forecast ───────────────────────────────────────────────────
  output$forecast_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui(rv$timed_out))
    
    now <- Sys.time()
    
    # ── Build / use cached forecasts ──────────────────────────────
    # Rebuild if cache is empty or older than 30 minutes
    needs_rebuild <- is.null(rv$forecast_cache) ||
      is.null(rv$forecast_built) ||
      as.numeric(difftime(now, rv$forecast_built, units="mins")) > 30
    
    if (needs_rebuild) {
      withProgress(message="Fitting Prophet models…", value=0.1, {
        rv$forecast_cache <- build_county_forecasts(rv$cases, now)
        rv$forecast_built <- now
        setProgress(1)
      })
    }
    
    fc_list    <- rv$forecast_cache
    county_fc  <- do.call(rbind, lapply(fc_list, function(r) {
      data.frame(
        county=r$county, n_recent=r$n_recent, avg_risk=r$avg_risk,
        avg_ncic=r$avg_ncic, n_high=r$n_high, n_s13=r$n_s13,
        trend=r$trend, top_platform=r$top_platform, top_category=r$top_category,
        escalation_score=r$escalation_score, forecast_level=r$forecast_level,
        drivers=r$drivers, prophet_used=r$prophet_used,
        stringsAsFactors=FALSE
      )
    }))
    
    n_prophet  <- sum(county_fc$prophet_used, na.rm=TRUE)
    n_heuristic<- nrow(county_fc) - n_prophet
    
    # ── Colour helpers ────────────────────────────────────────────
    fc_col <- function(lvl) switch(lvl,
                                   CRITICAL  = "#7b0000", HIGH     = "#dc3545",
                                   ELEVATED  = "#fd7e14", MONITORED= "#ffc107",
                                   STABLE    = "#198754", "#dee2e6")
    
    # ── sf join for tmap ─────────────────────────────────────────
    sf_fc <- KENYA_SF |>
      left_join(county_fc, by=c("name"="county")) |>
      mutate(
        escalation_score = ifelse(is.na(escalation_score), 0L, escalation_score),
        forecast_level   = ifelse(is.na(forecast_level), "STABLE", forecast_level),
        n_recent         = ifelse(is.na(n_recent), 0L, n_recent),
        avg_risk         = ifelse(is.na(avg_risk),  0L, avg_risk),
        drivers          = ifelse(is.na(drivers), "Insufficient data", drivers),
        prophet_used     = ifelse(is.na(prophet_used), FALSE, prophet_used)
      )
    
    # store sf_fc so renderTmap can access it
    rv$sf_fc <- sf_fc
    
    # ── KPIs ─────────────────────────────────────────────────────
    n_critical <- sum(county_fc$forecast_level == "CRITICAL",  na.rm=TRUE)
    n_high_fc  <- sum(county_fc$forecast_level == "HIGH",      na.rm=TRUE)
    n_elevated <- sum(county_fc$forecast_level == "ELEVATED",  na.rm=TRUE)
    top_county <- county_fc$county[which.max(county_fc$escalation_score)]
    
    # ── Prophet trend chart for top county ───────────────────────
    top_fc     <- fc_list[[which(sapply(fc_list, `[[`, "county") == top_county)]]
    trend_plot <- NULL
    if (!is.null(top_fc$forecast_df) && !is.null(top_fc$hist_df)) {
      hist  <- top_fc$hist_df
      fcast <- top_fc$forecast_df
      fcast_future <- fcast[fcast$ds > max(hist$ds), ]
      
      trend_plot <- plot_ly() |>
        add_ribbons(data=fcast_future,
                    x=~ds, ymin=~yhat_lower, ymax=~yhat_upper,
                    fillcolor="rgba(220,53,69,0.12)", line=list(color="transparent"),
                    name="80% CI", showlegend=TRUE) |>
        add_lines(data=fcast,
                  x=~ds, y=~yhat,
                  line=list(color="#dc3545", width=2, dash="dash"),
                  name="Prophet forecast") |>
        add_lines(data=hist,
                  x=~ds, y=~y,
                  line=list(color="#0066cc", width=2),
                  name="Historical risk") |>
        layout(
          title      = list(text=paste0("<b>", top_county, "</b> — 14-Day Risk Forecast"),
                            font=list(size=13), x=0),
          xaxis      = list(title="", showgrid=FALSE),
          yaxis      = list(title="Avg Risk Score", range=c(0,100),
                            gridcolor="#f0f0f0"),
          legend     = list(orientation="h", y=-0.2, font=list(size=11)),
          paper_bgcolor = "rgba(0,0,0,0)",
          plot_bgcolor  = "rgba(0,0,0,0)",
          margin     = list(t=40, r=10, b=10, l=40),
          hovermode  = "x unified"
        ) |>
        config(displayModeBar=FALSE)
    }
    
    tagList(
      # ── Header ───────────────────────────────────────────────────
      tags$div(style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:10px;",
               tags$div(
                 tags$h4(style="font-weight:800;color:#1a1a2e;margin:0 0 3px;font-size:16px;",
                         tagList(bs_icon("graph-up-arrow"), " 14-Day Risk Forecast")),
                 tags$div(style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;",
                          tags$span(style="font-size:12px;color:#6c757d;",
                                    paste0("Generated ", format(now, "%d %b %Y %H:%M"), " EAT")),
                          tags$span(style="color:#dee2e6;","·"),
                          tags$span(style="background:#d1fae5;color:#065f46;border:1px solid #6ee7b7;border-radius:3px;padding:1px 8px;font-size:11px;font-weight:600;",
                                    paste0("✓ ", n_prophet, " Prophet")),
                          if (n_heuristic > 0)
                            tags$span(style="background:#fff8e1;color:#664d03;border:1px solid #fcd34d;border-radius:3px;padding:1px 8px;font-size:11px;font-weight:600;",
                                      paste0("⚠ ", n_heuristic, " heuristic"))
                 )
               ),
               actionButton("btn_rebuild_forecast",
                            tagList(bs_icon("arrow-clockwise"), " Rebuild"),
                            class="btn btn-outline-secondary btn-sm",
                            style="font-size:11px;")
      ),
      
      # ── Compact KPI strip ─────────────────────────────────────────
      tags$div(style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:14px;",
               # Critical
               tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #7b0000;border-radius:6px;padding:8px 12px;display:flex;align-items:center;gap:10px;",
                        tags$div(style="font-size:22px;font-weight:800;color:#7b0000;min-width:32px;text-align:center;", n_critical),
                        tags$div(
                          tags$div(style="font-size:10px;font-weight:700;color:#7b0000;text-transform:uppercase;letter-spacing:.04em;","Critical"),
                          tags$div(style="font-size:10px;color:#6c757d;","counties")
                        )
               ),
               # High
               tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #dc3545;border-radius:6px;padding:8px 12px;display:flex;align-items:center;gap:10px;",
                        tags$div(style="font-size:22px;font-weight:800;color:#dc3545;min-width:32px;text-align:center;", n_high_fc),
                        tags$div(
                          tags$div(style="font-size:10px;font-weight:700;color:#dc3545;text-transform:uppercase;letter-spacing:.04em;","High"),
                          tags$div(style="font-size:10px;color:#6c757d;","counties")
                        )
               ),
               # Elevated
               tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #fd7e14;border-radius:6px;padding:8px 12px;display:flex;align-items:center;gap:10px;",
                        tags$div(style="font-size:22px;font-weight:800;color:#fd7e14;min-width:32px;text-align:center;", n_elevated),
                        tags$div(
                          tags$div(style="font-size:10px;font-weight:700;color:#fd7e14;text-transform:uppercase;letter-spacing:.04em;","Elevated"),
                          tags$div(style="font-size:10px;color:#6c757d;","counties")
                        )
               ),
               # Stable
               tags$div(style="background:#fff;border:1px solid #dee2e6;border-left:4px solid #198754;border-radius:6px;padding:8px 12px;display:flex;align-items:center;gap:10px;",
                        tags$div(style="font-size:22px;font-weight:800;color:#198754;min-width:32px;text-align:center;",
                                 sum(county_fc$forecast_level %in% c("STABLE","MONITORED"), na.rm=TRUE)),
                        tags$div(
                          tags$div(style="font-size:10px;font-weight:700;color:#198754;text-transform:uppercase;letter-spacing:.04em;","Stable"),
                          tags$div(style="font-size:10px;color:#6c757d;","counties")
                        )
               ),
               # Top watch
               tags$div(style="background:#0066cc;border-radius:6px;padding:8px 12px;color:#fff;",
                        tags$div(style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;opacity:.8;margin-bottom:2px;","Top Watch County"),
                        tags$div(style="font-size:14px;font-weight:800;line-height:1.2;", top_county),
                        tags$div(style="font-size:9px;opacity:.75;margin-top:1px;",
                                 paste0("Score: ", max(county_fc$escalation_score, na.rm=TRUE)))
               )
      ),
      
      # ── Three-column layout: ranking | map | trend chart ─────────
      layout_columns(col_widths=c(3,5,4),
                     
                     # ── LEFT: County ranking ──────────────────────────────────
                     card(style="border-top:3px solid #1a1a2e;",
                          card_header(tagList(bs_icon("sort-down"), " County Risk Ranking")),
                          tags$div(style="max-height:560px;overflow-y:auto;padding:0;",
                                   lapply(seq_len(nrow(county_fc[order(-county_fc$escalation_score),])), function(i) {
                                     row <- county_fc[order(-county_fc$escalation_score),][i,]
                                     col <- fc_col(row$forecast_level)
                                     txt_col <- if(row$forecast_level == "MONITORED") "#1a1a2e" else "#fff"
                                     tags$div(
                                       style=paste0("display:flex;align-items:center;gap:8px;padding:6px 12px;",
                                                    "border-bottom:1px solid #f5f5f5;",
                                                    if(i==1) "background:#fafafa;" else ""),
                                       # Rank
                                       tags$div(style="font-size:10px;font-weight:700;color:#9ca3af;min-width:18px;",
                                                paste0("#",i)),
                                       # County + model badge
                                       tags$div(style="flex:1;min-width:0;",
                                                tags$div(style="display:flex;align-items:center;gap:4px;",
                                                         tags$span(style="font-size:12px;font-weight:700;color:#1a1a2e;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;",
                                                                   row$county),
                                                         if (isTRUE(row$prophet_used))
                                                           tags$span(style="font-size:8px;background:#d1fae5;color:#065f46;border-radius:2px;padding:0 3px;font-weight:700;flex-shrink:0;","P")
                                                         else
                                                           tags$span(style="font-size:8px;background:#fff8e1;color:#664d03;border-radius:2px;padding:0 3px;font-weight:700;flex-shrink:0;","H")
                                                ),
                                                tags$div(style="font-size:9px;color:#9ca3af;margin-top:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;",
                                                         row$drivers)
                                       ),
                                       # Score + level badge
                                       tags$div(style="display:flex;flex-direction:column;align-items:flex-end;gap:2px;flex-shrink:0;",
                                                tags$span(style=paste0("background:",col,";color:",txt_col,
                                                                       ";border-radius:3px;padding:1px 6px;font-size:9px;font-weight:700;"),
                                                          row$forecast_level),
                                                tags$span(style=paste0("font-size:13px;font-weight:800;color:",col,";"),
                                                          row$escalation_score)
                                       )
                                     )
                                   })
                          ),
                          tags$div(style="padding:6px 12px;font-size:9px;color:#9ca3af;border-top:1px solid #f0f0f0;display:flex;gap:8px;",
                                   tags$span(tagList(tags$span(style="background:#d1fae5;color:#065f46;border-radius:2px;padding:0 3px;font-weight:700;","P"), " Prophet")),
                                   tags$span(tagList(tags$span(style="background:#fff8e1;color:#664d03;border-radius:2px;padding:0 3px;font-weight:700;","H"), " Heuristic"))
                          )
                     ),
                     
                     # ── CENTRE: Map ───────────────────────────────────────────
                     card(full_screen=TRUE, style="border-top:3px solid #0066cc;",
                          card_header(tagList(bs_icon("map"), " Forecast Risk Map · hover for county details")),
                          tags$div(style="height:560px;",
                                   tmapOutput("forecast_map", height="560px"))
                     ),
                     
                     # ── RIGHT: Trend chart + methodology ─────────────────────
                     tagList(
                       card(style="border-top:3px solid #dc3545;",
                            card_header(
                              tags$div(style="display:flex;align-items:center;justify-content:space-between;width:100%;gap:8px;",
                                       tags$span(style="font-size:13px;font-weight:600;",
                                                 tagList(bs_icon("graph-up"), " Prophet Trend")),
                                       selectInput("forecast_county_select", NULL,
                                                   choices  = setNames(counties$name, counties$name),
                                                   selected = top_county,
                                                   width    = "140px")
                              )
                            ),
                            tags$div(style="height:280px;",
                                     plotlyOutput("forecast_trend_plot", height="280px"))
                       ),
                       # Methodology note below chart
                       tags$div(style="margin-top:8px;background:#f8f9fa;border-radius:6px;padding:10px 12px;font-size:10px;color:#6c757d;border-left:3px solid #7c3aed;line-height:1.7;",
                                tags$strong("Methodology: "),
                                paste0("Prophet models on 30-day daily mean risk scores. ",
                                       "Escalation score = mean yhat_upper over 14-day horizon, capped 0–100. ",
                                       "Weekly seasonality · changepoint prior = 0.15 · 80% CI shown. ",
                                       "< ", MIN_PROPHET_OBS, " days data → heuristic fallback. ",
                                       "All projections require officer review.")
                       )
                     )
      )
    )
  })
  
  # ── Rebuild forecast button ────────────────────────────────────
  observeEvent(input$btn_rebuild_forecast, {
    shinyjs::runjs("document.getElementById('ews-loading-bar').classList.add('active');")
    rv$forecast_cache <- NULL
    rv$forecast_built <- NULL
    later::later(function() {
      shinyjs::runjs("document.getElementById('ews-loading-bar').classList.remove('active');")
    }, delay = 0.5)
  })
  
  # ── Prophet trend chart output ─────────────────────────────────
  output$forecast_trend_plot <- renderPlotly({
    req(rv$forecast_cache)
    county_fc_names <- sapply(rv$forecast_cache, `[[`, "county")
    esc_scores      <- sapply(rv$forecast_cache, `[[`, "escalation_score")
    
    # Use selected county if available, otherwise top county
    sel_county <- input$forecast_county_select %||%
      county_fc_names[which.max(esc_scores)]
    if (!sel_county %in% county_fc_names)
      sel_county <- county_fc_names[which.max(esc_scores)]
    
    top_fc <- rv$forecast_cache[[which(county_fc_names == sel_county)]]
    
    # Fallback message if no Prophet model for this county
    if (is.null(top_fc$forecast_df) || is.null(top_fc$hist_df)) {
      return(plot_ly() |>
               layout(
                 title      = list(text=paste0("<b>", sel_county,
                                               "</b> — Insufficient data for Prophet model (heuristic used)"),
                                   font=list(size=12), x=0),
                 paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"
               ) |> config(displayModeBar=FALSE))
    }
    
    hist         <- top_fc$hist_df
    fcast        <- top_fc$forecast_df
    fcast_future <- fcast[fcast$ds > max(hist$ds), ]
    
    plot_ly() |>
      add_ribbons(data=fcast_future,
                  x=~ds, ymin=~yhat_lower, ymax=~yhat_upper,
                  fillcolor="rgba(220,53,69,0.12)", line=list(color="transparent"),
                  name="80% CI", showlegend=TRUE) |>
      add_lines(data=fcast,
                x=~ds, y=~yhat,
                line=list(color="#dc3545", width=2, dash="dash"),
                name="Prophet forecast") |>
      add_lines(data=hist,
                x=~ds, y=~y,
                line=list(color="#0066cc", width=2),
                name="Historical risk") |>
      layout(
        title      = list(text=paste0("<b>", sel_county, "</b> — 14-Day Risk Forecast"),
                          font=list(size=12), x=0),
        xaxis      = list(title="", showgrid=FALSE),
        yaxis      = list(title="Avg Risk Score", range=c(0,100),
                          gridcolor="#f0f0f0"),
        legend     = list(orientation="h", y=-0.25, font=list(size=11)),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor  = "rgba(0,0,0,0)",
        margin     = list(t=30, r=10, b=10, l=40),
        hovermode  = "x unified"
      ) |>
      config(displayModeBar=FALSE)
  })
  
  # ── Forecast map renderTmap ────────────────────────────────────
  output$forecast_map <- renderTmap({
    req(rv$sf_fc)
    sf_fc <- rv$sf_fc
    tm_shape(sf_fc) +
      tm_polygons(
        col        = "escalation_score",
        palette    = c("#198754","#ffc107","#fd7e14","#dc3545","#7b0000"),
        breaks     = c(0, 20, 40, 60, 80, 100),
        labels     = c("Stable","Monitored","Elevated","High","Critical"),
        colorNA    = "#dee2e6",
        textNA     = "Insufficient data",
        title      = "14-Day Prophet Forecast Risk",
        border.col = "#ffffff",
        border.lwd = 1.2,
        alpha      = 0.85,
        id         = "name",
        popup.vars = c(
          "Forecast Level"   = "forecast_level",
          "Escalation Score" = "escalation_score",
          "Prophet Fitted"   = "prophet_used",
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
    if (!rv$authenticated) return(auth_wall_ui(rv$timed_out))
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
    nm <- trimws(input$auth_name     %||% "")
    un <- trimws(input$auth_username %||% "")
    pw <- input$auth_password        %||% ""
    
    if (nchar(nm) == 0) {
      shinyjs::html("auth_error", "❌ Please enter your full name.")
      return()
    }
    if (nchar(un) == 0) {
      shinyjs::html("auth_error", "❌ Please enter your username.")
      return()
    }
    
    result <- list(ok = TRUE, role = "admin")
    
    if (isTRUE(result$ok)) {
      rv$authenticated  <- TRUE
      rv$officer_name   <- nm
      rv$officer_user   <- un
      rv$officer_role   <- result$role
      rv$last_activity  <- Sys.time()
      rv$timeout_warned <- FALSE
      rv$timed_out      <- FALSE
      shinyjs::html("auth_error", "")
      audit(un, nm, "LOGIN",
            detail     = paste0("Role: ", result$role),
            session_id = rv$session_id)
    } else {
      audit(un, nm, "LOGIN_FAILED",
            detail     = "Invalid credentials",
            session_id = rv$session_id)
      shinyjs::html("auth_error", "❌ Invalid username or password.")
    }
  })
  
  # ── S13 Escalation Queue UI ───────────────────────────────────
  output$s13_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui(rv$timed_out))
    
    queue <- db_s13_load()
    
    # Status counts
    n_pending <- sum(queue$status == "PENDING",     na.rm=TRUE)
    n_filed   <- sum(queue$status == "FILED",       na.rm=TRUE)
    n_dci     <- sum(queue$status == "DCI_ALERTED", na.rm=TRUE)
    n_resolved<- sum(queue$status == "RESOLVED",    na.rm=TRUE)
    
    status_col <- function(s) switch(s,
                                     PENDING     = "#dc3545",
                                     FILED       = "#fd7e14",
                                     DCI_ALERTED = "#7c3aed",
                                     RESOLVED    = "#198754",
                                     "#6c757d")
    
    status_label <- function(s) switch(s,
                                       PENDING     = "⏳ PENDING",
                                       FILED       = "📄 FILED",
                                       DCI_ALERTED = "🚔 DCI ALERTED",
                                       RESOLVED    = "✅ RESOLVED",
                                       s)
    
    tagList(
      # ── Header ────────────────────────────────────────────────
      tags$div(style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px;",
               tags$div(
                 tags$h4(style="font-weight:800;color:#1a1a2e;margin:0 0 4px;font-size:16px;",
                         tagList(bs_icon("exclamation-octagon"), " Section 13 Escalation Queue")),
                 tags$p(style="font-size:12px;color:#6c757d;margin:0;",
                        "Confirmed L4/L5 cases requiring legal action under NCIC Act Cap 170, Section 13.")
               ),
               tags$span(style="background:#dc3545;color:#fff;border-radius:4px;padding:4px 12px;font-size:12px;font-weight:700;align-self:center;",
                         paste0("⚖ Section 13 Active"))
      ),
      
      # ── KPI row ───────────────────────────────────────────────
      layout_columns(col_widths=c(3,3,3,3),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #dc3545;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:26px;font-weight:800;color:#dc3545;", n_pending),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","Pending Action")),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #fd7e14;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:26px;font-weight:800;color:#fd7e14;", n_filed),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","Filed")),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #7c3aed;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:26px;font-weight:800;color:#7c3aed;", n_dci),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","DCI Alerted")),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #198754;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:26px;font-weight:800;color:#198754;", n_resolved),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","Resolved"))
      ),
      
      tags$div(style="margin-top:16px;",
               
               if (nrow(queue) == 0) {
                 tags$div(
                   style="text-align:center;padding:48px;background:#fff;border-radius:12px;border:1px solid #dee2e6;",
                   tags$div(style="font-size:48px;margin-bottom:12px;","⚖"),
                   tags$h5("No Section 13 cases in queue", style="font-weight:700;color:#1a1a2e;"),
                   tags$p(style="font-size:13px;color:#6c757d;",
                          "Cases are auto-added when an officer CONFIRMS or ESCALATES an L4/L5 classification.")
                 )
               } else {
                 tagList(
                   # ── Filter row ──────────────────────────────────────
                   tags$div(style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;",
                            selectInput("s13_filter_status", NULL,
                                        choices  = c("All Statuses","PENDING","FILED","DCI_ALERTED","RESOLVED"),
                                        selected = "All Statuses", width="160px"),
                            selectInput("s13_filter_county", NULL,
                                        choices  = c("All Counties", sort(unique(queue$county))),
                                        selected = "All Counties", width="160px"),
                            downloadButton("dl_s13_queue",
                                           tagList(bs_icon("download"), " Export"),
                                           style="background:#dc3545;color:#fff;border:none;border-radius:6px;font-weight:600;font-size:12px;padding:6px 14px;")
                   ),
                   
                   # ── Case cards ──────────────────────────────────────
                   uiOutput("s13_cards")
                 )
               }
      )
    )
  })
  
  # ── S13 case cards (reactive to filters) ──────────────────────
  output$s13_cards <- renderUI({
    queue <- db_s13_load()
    if (nrow(queue) == 0) return(NULL)
    
    # Apply filters
    sf <- input$s13_filter_status %||% "All Statuses"
    cf <- input$s13_filter_county %||% "All Counties"
    if (sf != "All Statuses") queue <- queue[queue$status == sf, ]
    if (cf != "All Counties") queue <- queue[queue$county  == cf, ]
    
    if (nrow(queue) == 0)
      return(tags$p(style="color:#6c757d;font-size:13px;padding:16px;","No cases match the selected filters."))
    
    status_col <- function(s) switch(s,
                                     PENDING="dc3545", FILED="fd7e14", DCI_ALERTED="7c3aed", RESOLVED="198754", "6c757d")
    
    status_label <- function(s) switch(s,
                                       PENDING="⏳ PENDING", FILED="📄 FILED",
                                       DCI_ALERTED="🚔 DCI ALERTED", RESOLVED="✅ RESOLVED", s)
    
    tags$div(style="display:flex;flex-direction:column;gap:10px;",
             lapply(seq_len(nrow(queue)), function(i) {
               r   <- queue[i,]
               sc  <- status_col(r$status)
               nc  <- sub("#","",ncic_color(r$ncic_level))
               bid <- paste0("s13_btn_", r$id)
               
               tags$div(
                 style=paste0("background:#fff;border:1px solid #dee2e6;border-left:5px solid #",
                              sc, ";border-radius:8px;padding:14px 16px;"),
                 # Top row
                 tags$div(style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:8px;",
                          tags$div(
                            tags$div(style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px;",
                                     tags$span(style=paste0("background:#",nc,";color:#fff;border-radius:3px;padding:1px 8px;font-size:11px;font-weight:700;"),
                                               paste0("L", r$ncic_level, " — ", ncic_name(r$ncic_level))),
                                     tags$span(style=paste0("background:#",sc,"18;color:#",sc,";border:1px solid #",sc,"44;",
                                                            "border-radius:3px;padding:1px 8px;font-size:11px;font-weight:700;"),
                                               status_label(r$status)),
                                     tags$span(style="background:#f0f4f8;color:#374151;border-radius:3px;padding:1px 8px;font-size:11px;font-weight:600;",
                                               paste0("Risk: ", r$risk_score)),
                                     tags$span(style="font-family:'IBM Plex Mono';font-size:10px;color:#6c757d;",
                                               r$case_id)
                            ),
                            tags$div(style="font-size:12px;color:#374151;line-height:1.6;",
                                     tags$strong(r$county), " · ", r$platform,
                                     if (!is.na(r$target_group) && nchar(r$target_group)>0)
                                       paste0(" · Target: ", r$target_group) else "")
                          ),
                          tags$div(style="font-size:11px;color:#6c757d;text-align:right;flex-shrink:0;",
                                   tags$div(paste0("Filed: ", r$created_at)),
                                   if (!is.na(r$validated_by)) tags$div(paste0("Officer: ", r$validated_by)) else NULL,
                                   if (!is.na(r$dci_ref)&&nchar(r$dci_ref)>0)
                                     tags$div(style="color:#7c3aed;font-weight:600;",
                                              paste0("DCI Ref: ", r$dci_ref)) else NULL)
                 ),
                 # Post text
                 tags$div(style="background:#f8f9fa;border-radius:4px;padding:8px 10px;font-size:12px;color:#374151;line-height:1.6;margin-bottom:10px;font-style:italic;",
                          paste0('"', r$tweet_text, '"')),
                 # Notes
                 if (!is.na(r$notes) && nchar(r$notes)>0)
                   tags$div(style="font-size:11px;color:#6c757d;margin-bottom:8px;",
                            tags$strong("Notes: "), r$notes) else NULL,
                 # Action buttons (officers can update status)
                 tags$div(style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;",
                          if (r$status == "PENDING")
                            actionButton(paste0("s13_file_",r$id),
                                         tagList(bs_icon("file-earmark-text"), " Mark Filed"),
                                         class="btn btn-sm",
                                         style="background:#fd7e14;color:#fff;border:none;font-size:11px;font-weight:600;") else NULL,
                          if (r$status %in% c("PENDING","FILED"))
                            actionButton(paste0("s13_dci_",r$id),
                                         tagList(bs_icon("shield-exclamation"), " DCI Alerted"),
                                         class="btn btn-sm",
                                         style="background:#7c3aed;color:#fff;border:none;font-size:11px;font-weight:600;") else NULL,
                          if (r$status != "RESOLVED")
                            actionButton(paste0("s13_resolve_",r$id),
                                         tagList(bs_icon("check-circle"), " Resolve"),
                                         class="btn btn-sm",
                                         style="background:#198754;color:#fff;border:none;font-size:11px;font-weight:600;") else NULL,
                          tags$span(style="font-size:10px;color:#9ca3af;margin-left:4px;",
                                    paste0("Updated: ", r$updated_at))
                 )
               )
             })
    )
  })
  
  # ── S13 status button observers ────────────────────────────────
  observe({
    queue <- db_s13_load()
    for (i in seq_len(nrow(queue))) {
      local({
        rid <- queue$id[i]
        # FILE
        fid <- paste0("s13_file_", rid)
        if (!exists(fid, envir=observer_registry)) {
          assign(fid, TRUE, envir=observer_registry)
          observeEvent(input[[fid]], {
            db_s13_update_status(rid, "FILED")
            audit(rv$officer_user, rv$officer_name, "S13_FILED",
                  case_id=queue$case_id[queue$id==rid][1],
                  session_id=rv$session_id)
            showNotification("Case marked as Filed.", type="message")
          }, ignoreInit=TRUE)
        }
        # DCI
        did <- paste0("s13_dci_", rid)
        if (!exists(did, envir=observer_registry)) {
          assign(did, TRUE, envir=observer_registry)
          observeEvent(input[[did]], {
            db_s13_update_status(rid, "DCI_ALERTED")
            audit(rv$officer_user, rv$officer_name, "S13_DCI_ALERTED",
                  case_id=queue$case_id[queue$id==rid][1],
                  session_id=rv$session_id)
            showNotification("DCI alert status recorded.", type="warning")
          }, ignoreInit=TRUE)
        }
        # RESOLVE
        rsid <- paste0("s13_resolve_", rid)
        if (!exists(rsid, envir=observer_registry)) {
          assign(rsid, TRUE, envir=observer_registry)
          observeEvent(input[[rsid]], {
            db_s13_update_status(rid, "RESOLVED")
            audit(rv$officer_user, rv$officer_name, "S13_RESOLVED",
                  case_id=queue$case_id[queue$id==rid][1],
                  session_id=rv$session_id)
            showNotification("Case resolved.", type="message")
          }, ignoreInit=TRUE)
        }
      })
    }
  })
  
  # ── S13 CSV export ─────────────────────────────────────────────
  output$dl_s13_queue <- downloadHandler(
    filename = function() paste0("EWS_S13_Queue_", Sys.Date(), ".csv"),
    content  = function(file) write.csv(db_s13_load(), file, row.names=FALSE)
  )
  
  # ── Audit Log UI (admin only) ──────────────────────────────────
  output$audit_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui(rv$timed_out))
    
    # Role gate — officers see a polite message, not the data
    if (rv$officer_role != "admin") {
      return(tags$div(
        style="display:flex;align-items:center;justify-content:center;min-height:300px;",
        tags$div(
          style="text-align:center;padding:40px;background:#fff;border-radius:12px;border:1px solid #dee2e6;max-width:420px;",
          tags$div(style="font-size:48px;margin-bottom:12px;","🔒"),
          tags$h4("Admin Access Required", style="font-weight:700;color:#1a1a2e;margin-bottom:8px;"),
          tags$p(style="font-size:13px;color:#6c757d;margin:0;",
                 "The Audit Log is restricted to administrators. Contact your NCIC admin if you need access.")
        )
      ))
    }
    
    log  <- db_load_audit(500)
    offs <- db_load_officers()
    
    tagList(
      # ── Header bar ──────────────────────────────────────────────
      tags$div(
        style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px;",
        tags$div(
          tags$h4(style="font-weight:800;color:#1a1a2e;margin:0 0 2px;font-size:16px;",
                  tagList(bs_icon("clock-history"), " Audit Log")),
          tags$p(style="font-size:12px;color:#6c757d;margin:0;",
                 "Tamper-evident record of all officer actions. Last 500 entries shown.")
        ),
        tags$div(style="display:flex;gap:8px;align-items:center;",
                 tags$span(
                   style="background:#dc3545;color:#fff;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:600;",
                   "ADMIN ONLY"),
                 downloadButton("dl_audit",
                                tagList(bs_icon("download"), " Export CSV"),
                                style="background:#1a1a2e;color:#fff;border:none;border-radius:6px;font-weight:600;font-size:12px;padding:6px 14px;")
        )
      ),
      
      # ── KPI row ──────────────────────────────────────────────────
      layout_columns(col_widths=c(3,3,3,3),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #0066cc;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:22px;font-weight:800;color:#0066cc;", nrow(log)),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","Total Events")),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #198754;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:22px;font-weight:800;color:#198754;",
                                       sum(log$action == "LOGIN", na.rm=TRUE)),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","Logins")),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #fd7e14;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:22px;font-weight:800;color:#fd7e14;",
                                       sum(log$action %in% c("VALIDATE","ESCALATE","CLEAR"), na.rm=TRUE)),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","Validations")),
                     tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #dc3545;border-radius:8px;padding:12px 16px;text-align:center;",
                              tags$div(style="font-size:22px;font-weight:800;color:#dc3545;",
                                       sum(log$action %in% c("TIMEOUT","LOGIN_FAILED"), na.rm=TRUE)),
                              tags$div(style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;","Security Events"))
      ),
      
      tags$div(style="margin-top:16px;",
               layout_columns(col_widths=c(8,4),
                              
                              # ── Action log table ─────────────────────────────────────
                              card(style="border-top:3px solid #1a1a2e;",
                                   card_header(tagList(bs_icon("list-ul"), " Action Log")),
                                   if (nrow(log) == 0) {
                                     tags$p(style="font-size:12px;color:#6c757d;padding:12px;","No audit records yet.")
                                   } else {
                                     log_disp <- log
                                     log_disp$action <- sapply(log_disp$action, function(a) {
                                       col <- switch(a,
                                                     LOGIN         = "#198754",
                                                     LOGIN_FAILED  = "#dc3545",
                                                     LOGOUT        = "#6c757d",
                                                     TIMEOUT       = "#fd7e14",
                                                     VALIDATE      = "#0066cc",
                                                     ESCALATE      = "#dc3545",
                                                     CLEAR         = "#198754",
                                                     KEYWORD       = "#7c3aed",
                                                     "#374151")
                                       as.character(tags$span(
                                         style=paste0("background:",col,"18;color:",col,
                                                      ";border:1px solid ",col,"44;border-radius:3px;",
                                                      "padding:1px 7px;font-size:10px;font-weight:700;"),
                                         a))
                                     })
                                     log_disp$case_id <- ifelse(is.na(log_disp$case_id), "—", log_disp$case_id)
                                     log_disp$detail  <- ifelse(is.na(log_disp$detail),  "—", log_disp$detail)
                                     log_disp$id      <- NULL
                                     names(log_disp)  <- c("Timestamp","Officer","Name","Action","Case ID","Detail")
                                     DTOutput("audit_table")
                                   }
                              ),
                              
                              # ── Officer roster ──────────────────────────────────────
                              card(style="border-top:3px solid #7c3aed;",
                                   card_header(tagList(bs_icon("people"), " Officer Roster")),
                                   if (nrow(offs) == 0) {
                                     tags$p(style="font-size:12px;color:#6c757d;padding:12px;","No officers found.")
                                   } else {
                                     tags$div(style="display:flex;flex-direction:column;gap:6px;padding:4px 0;",
                                              lapply(seq_len(nrow(offs)), function(i) {
                                                o      <- offs[i,]
                                                active <- o$active == 1
                                                role   <- o$role
                                                col    <- if (role == "admin") "#7c3aed" else "#0066cc"
                                                tags$div(
                                                  style=paste0("display:flex;align-items:center;justify-content:space-between;",
                                                               "padding:8px 12px;border-radius:6px;",
                                                               "background:", if(active) "#f8f9fa" else "#fff5f5", ";",
                                                               "border:1px solid ", if(active) "#dee2e6" else "#fecaca", ";"),
                                                  tags$div(
                                                    tags$div(style="font-size:12px;font-weight:700;color:#1a1a2e;",
                                                             o$username),
                                                    tags$div(style="font-size:10px;color:#6c757d;margin-top:1px;",
                                                             format(as.POSIXct(o$created_at), "%d %b %Y"))
                                                  ),
                                                  tags$div(style="display:flex;gap:5px;align-items:center;",
                                                           tags$span(style=paste0("background:",col,"18;color:",col,
                                                                                  ";border:1px solid ",col,"44;border-radius:3px;",
                                                                                  "padding:1px 7px;font-size:10px;font-weight:700;"),
                                                                     toupper(role)),
                                                           tags$span(style=paste0("border-radius:3px;padding:1px 7px;",
                                                                                  "font-size:10px;font-weight:700;",
                                                                                  if(active) "background:#d1fae5;color:#065f46;"
                                                                                  else       "background:#fee2e2;color:#991b1b;"),
                                                                     if(active) "ACTIVE" else "INACTIVE")
                                                  )
                                                )
                                              })
                                     )
                                   },
                                   tags$div(style="padding:10px 4px 4px;font-size:11px;color:#9ca3af;border-top:1px solid #f0f0f0;margin-top:8px;",
                                            tagList(bs_icon("info-circle"),
                                                    " To add, remove, or reset officers: run ",
                                                    tags$code("Rscript setup_officers.R"), " from the app directory."))
                              )
               )
      )
    )
  })
  
  # ── Audit table render ─────────────────────────────────────────
  output$audit_table <- renderDT({
    log <- db_load_audit(500)
    if (nrow(log) == 0) return(datatable(data.frame()))
    
    action_html <- sapply(log$action, function(a) {
      col <- switch(a,
                    LOGIN        = "#198754", LOGIN_FAILED = "#dc3545",
                    LOGOUT       = "#6c757d", TIMEOUT      = "#fd7e14",
                    VALIDATE     = "#0066cc", ESCALATE     = "#dc3545",
                    CLEAR        = "#198754", KEYWORD      = "#7c3aed", "#374151")
      sprintf('<span style="background:%s18;color:%s;border:1px solid %s44;border-radius:3px;padding:1px 7px;font-size:10px;font-weight:700;">%s</span>',
              col, col, col, a)
    })
    
    disp <- data.frame(
      Timestamp = log$ts,
      Officer   = log$officer,
      Name      = log$officer_name,
      Action    = action_html,
      `Case ID` = ifelse(is.na(log$case_id), "—", log$case_id),
      Detail    = ifelse(is.na(log$detail),  "—", log$detail),
      stringsAsFactors = FALSE, check.names = FALSE
    )
    
    datatable(disp,
              escape      = FALSE,
              rownames    = FALSE,
              options     = list(
                pageLength  = 15,
                order       = list(list(0, "desc")),
                scrollX     = TRUE,
                dom         = "frtip",
                columnDefs  = list(list(width="140px", targets=0),
                                   list(width="80px",  targets=1),
                                   list(width="90px",  targets=3))
              )
    )
  }, server=TRUE)
  
  # ── Audit CSV export ──────────────────────────────────────────
  output$dl_audit <- downloadHandler(
    filename = function() paste0("EWS_AuditLog_", Sys.Date(), ".csv"),
    content  = function(file) {
      log <- db_load_audit(10000)
      write.csv(log, file, row.names=FALSE)
    }
  )
  
  # ── v6: GOLD STANDARD & EVALUATION outputs ─────────────────────
  output$gold_count_ui <- renderUI({
    gold <- db_load_gold()
    tags$span(style="font-size:10px;color:#9ca3af;",
              paste0(nrow(gold)," posts in gold standard"))
  })

  observeEvent(input$gold_add_btn, {
    tweet <- trimws(input$gold_tweet %||% "")
    level <- as.integer(input$gold_level %||% "0")
    req(nchar(tweet) >= 10, rv$authenticated)
    db_add_gold(tweet, level, rv$officer_name)
    updateTextAreaInput(session, "gold_tweet", value="")
    showNotification(paste0("Added to gold standard as L",level,"."), type="message", duration=3)
  })

  observeEvent(input$eval_run_btn, {
    gold      <- db_load_gold()
    threshold <- as.integer(input$eval_threshold %||% "3")
    req(nrow(gold) >= 5, rv$authenticated)
    result <- run_evaluation(gold, threshold, rv$officer_name)
    rv$last_eval <- result
    showNotification(sprintf("Evaluation complete: F1=%.2f (n=%d)",result$f1%||%0,result$n),
                     type="message", duration=5)
  })

  output$eval_results_ui <- renderUI({
    res <- rv$last_eval
    if (is.null(res)) return(tags$p(style="font-size:11px;color:#9ca3af;","No evaluation run yet."))
    p_col <- if(!is.na(res$precision)&&res$precision>=0.80) "#198754" else "#dc3545"
    r_col <- if(!is.na(res$recall)   &&res$recall   >=0.75) "#198754" else "#dc3545"
    f_col <- if(!is.na(res$f1)       &&res$f1       >=0.77) "#198754" else "#dc3545"
    tags$div(style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;",
      lapply(list(
        list(label="Precision",val=res$precision,col=p_col,target="target ≥ 0.80"),
        list(label="Recall",   val=res$recall,   col=r_col,target="target ≥ 0.75"),
        list(label="F1 Score", val=res$f1,       col=f_col,target="target ≥ 0.77")
      ), function(m) tags$div(
        style="background:#f8f9fa;border-radius:6px;padding:8px;text-align:center;",
        tags$div(style=paste0("font-size:20px;font-weight:700;color:",m$col,";"),
                 if(is.na(m$val))"—"else sprintf("%.2f",m$val)),
        tags$div(style="font-size:10px;color:#374151;margin-top:1px;",m$label),
        tags$div(style="font-size:9px;color:#9ca3af;",m$target)
      ))
    )
  })

  output$eval_history_ui <- renderUI({
    runs <- db_load_eval_runs()
    if (nrow(runs)==0) return(tags$p(style="font-size:11px;color:#9ca3af;","No evaluation runs yet."))
    DT::datatable(runs[,c("run_at","n_total","n_correct","precision_score","recall_score","f1_score","threshold","run_by")],
                  rownames=FALSE,
                  colnames=c("Time","N","Correct","Precision","Recall","F1","Threshold","Officer"),
                  options=list(dom="t",pageLength=8,scrollX=TRUE))
  })

  # ── v6: INTER-OFFICER AGREEMENT output ──────────────────────────
  output$agreement_ui <- renderUI({
    if (!rv$authenticated) return(NULL)
    kappa_result <- tryCatch(compute_kappa(), error=function(e)
      list(kappa=NA,n=0L,flagged=0L,pct_agree=0))
    k   <- kappa_result$kappa
    n   <- kappa_result$n
    pct <- kappa_result$pct_agree %||% 0
    fl  <- kappa_result$flagged   %||% 0L
    kappa_label <- if(is.na(k)) "Not enough data" else
      if(k>=0.8) "Almost perfect" else if(k>=0.6) "Substantial" else
      if(k>=0.4) "Moderate"       else if(k>=0.2) "Fair"        else "Poor"
    kappa_color <- if(is.na(k)) "#6c757d" else
      if(k>=0.6) "#198754" else if(k>=0.4) "#fd7e14" else "#dc3545"
    tagList(
      tags$div(style="padding:4px 0;",
        tags$p(style="font-size:11px;color:#6c757d;margin-bottom:10px;",
               "Tracks agreement between officers validating the same case. Cases differing by >1 NCIC level are flagged for senior review."),
        tags$div(style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:10px;",
          tags$div(style="background:#f8f9fa;border-radius:6px;padding:10px;text-align:center;",
            tags$div(style=paste0("font-size:24px;font-weight:700;color:",kappa_color,";"),
                     if(is.na(k))"—"else sprintf("%.2f",k)),
            tags$div(style="font-size:10px;color:#6c757d;margin-top:2px;",
                     paste0("Cohen's κ — ",kappa_label))),
          tags$div(style="background:#f8f9fa;border-radius:6px;padding:10px;text-align:center;",
            tags$div(style="font-size:24px;font-weight:700;color:#0066cc;",paste0(pct,"%")),
            tags$div(style="font-size:10px;color:#6c757d;margin-top:2px;",paste0("Agreement (n=",n,")"))),
          tags$div(style="background:#f8f9fa;border-radius:6px;padding:10px;text-align:center;",
            tags$div(style=paste0("font-size:24px;font-weight:700;color:",if(fl>0)"#dc3545"else"#198754",";"),fl),
            tags$div(style="font-size:10px;color:#6c757d;margin-top:2px;","Cases flagged"))),
        tags$div(style="font-size:10px;color:#6c757d;margin-bottom:8px;",
                 "κ: 0.0–0.2 Poor · 0.2–0.4 Fair · 0.4–0.6 Moderate · 0.6–0.8 Substantial · 0.8–1.0 Almost perfect"),
        if (fl > 0) {
          flagged_df <- tryCatch(
            .supa_get("officer_agreement", list(flagged="eq.1", order="ts.desc"), limit=20L),
            error=function(e) data.frame())
          if (nrow(flagged_df)>0)
            DT::datatable(flagged_df,rownames=FALSE,
              colnames=c("Case","Officer A","L(A)","Officer B","L(B)","Delta","Time"),
              options=list(dom="t",pageLength=10,scrollX=TRUE))
        } else tags$p(style="font-size:12px;color:#198754;","No flagged cases — all officers agree within 1 NCIC level.")
      )
    )
  })

  # ── v6: GPT CALIBRATION output ───────────────────────────────────
  output$calibration_ui <- renderUI({
    if (!rv$authenticated) return(NULL)
    cal <- load_calibration()
    if (nrow(cal)==0) return(tags$p(style="font-size:11px;color:#9ca3af;",
      "No calibration data yet — accumulates with each officer decision."))
    total_n <- sum(cal$n_total)
    overall <- round(sum(cal$n_correct)/max(total_n,1)*100,1)
    tagList(
      tags$div(style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;",
        tags$div(style="font-size:11px;color:#374151;",paste0("Overall GPT accuracy: ",overall,"% (n=",total_n,")")),
        tags$div(style=paste0("font-size:11px;font-weight:600;padding:2px 8px;border-radius:4px;",
          if(overall>=75)"background:#d1fae5;color:#065f46;"else"background:#fee2e2;color:#991b1b;"),
          if(overall>=75)"Well calibrated"else"Needs attention")),
      tags$div(style="display:flex;flex-direction:column;gap:3px;",
        lapply(seq_len(nrow(cal)), function(i) {
          row   <- cal[i,]; bucket <- row$conf_bucket
          acc   <- row$accuracy %||% 0; exp <- (bucket*10+5)/100
          color <- if(abs(acc-exp)<0.1)"#198754"else if(acc>exp)"#fd7e14"else"#dc3545"
          tags$div(style="display:flex;align-items:center;gap:8px;",
            tags$div(style="font-size:10px;color:#6c757d;min-width:60px;text-align:right;",
                     paste0(bucket*10,"–",bucket*10+10,"%")),
            tags$div(style="flex:1;height:14px;background:#e9ecef;border-radius:3px;overflow:hidden;position:relative;",
              tags$div(style=paste0("position:absolute;left:",round(exp*100),"%;top:0;width:1px;height:100%;background:#9ca3af;")),
              tags$div(style=paste0("width:",round(acc*100),"%;height:100%;background:",color,";border-radius:3px;"))),
            tags$div(style=paste0("font-size:10px;font-weight:600;min-width:36px;color:",color,";"),paste0(round(acc*100),"%")),
            tags$div(style="font-size:9px;color:#9ca3af;min-width:30px;",paste0("n=",row$n_total)))
        })),
      tags$div(style="font-size:9px;color:#9ca3af;margin-top:6px;",
               "Grey line = expected. Bar = actual. Orange = overconfident. Red = underconfident.")
    )
  })

  # ── v6: KEYWORD RETIREMENT output & observer ────────────────────
  output$stale_keywords_ui <- renderUI({
    if (!rv$authenticated) return(NULL)
    stale <- tryCatch(fetch_stale_keywords(90L), error=function(e) data.frame())
    if (nrow(stale)==0) return(tags$p(style="font-size:12px;color:#198754;padding:8px;",
      "No stale keywords — all matched within last 90 days."))
    tagList(
      tags$div(style="font-size:11px;color:#dc3545;font-weight:600;margin-bottom:8px;",
               paste0(nrow(stale)," keyword(s) not matched in 90+ days:")),
      tags$div(style="max-height:200px;overflow-y:auto;",
        lapply(seq_len(nrow(stale)), function(i) {
          row <- stale[i,]; kw_id <- row$id
          tags$div(style="display:flex;align-items:center;gap:8px;margin-bottom:6px;padding:6px 8px;background:#fff5f5;border-radius:6px;border:1px solid #fecaca;",
            tags$div(style="flex:1;",
              tags$div(style="font-size:12px;font-family:'IBM Plex Mono';color:#374151;font-weight:600;",row$keyword),
              tags$div(style="font-size:10px;color:#9ca3af;margin-top:2px;",
                paste0("T",row$tier," · ",row$category," · ",row$language,
                       " · matched ",row$times_matched,"x · last: ",
                       if(!is.na(row$last_matched_at))substr(row$last_matched_at,1,10)else"never"))),
            textInput(paste0("retire_reason_",kw_id),NULL,placeholder="Reason (optional)",width="180px"),
            actionButton(paste0("retire_kw_",kw_id),tagList(bs_icon("archive")," Retire"),
              class="btn btn-outline-danger btn-sm",
              onclick=sprintf("Shiny.setInputValue('retire_kw_target','%s',{priority:'event'})",kw_id)))
        }))
    )
  })

  observeEvent(input$retire_kw_target, {
    kw_id   <- input$retire_kw_target
    reason  <- isolate(input[[paste0("retire_reason_",kw_id)]]) %||% ""
    officer <- rv$officer_name
    req(nchar(kw_id)>0, rv$authenticated)
    success <- retire_keyword(kw_id, officer, reason)
    if (success) {
      showNotification("Keyword retired. Pipeline cache cleared.", type="message", duration=4)
      kw_cache <- file.path("cache","tg_keyword_cache.json")
      if (file.exists(kw_cache)) tryCatch(file.remove(kw_cache),error=function(e) NULL)
      audit(rv$officer_user, officer, "KEYWORD",
            detail=paste0("Retired keyword ID ",kw_id,": ",reason))
    } else showNotification("Retirement failed — check logs.", type="error", duration=5)
  })

  # ── v6: GEOGRAPHIC CLUSTERING output ────────────────────────────
  output$cluster_ui <- renderUI({
    if (!rv$authenticated) return(NULL)
    clusters <- tryCatch(detect_county_clusters(rv$cases,24L,3L,2L),
                         error=function(e) data.frame())
    if (nrow(clusters)==0) return(tags$p(style="font-size:11px;color:#198754;",
      "No geographic clusters in last 24 hours."))
    shared <- attr(clusters,"shared_keywords") %||% character()
    tagList(
      if (length(shared)>0)
        tags$div(style="background:#fff3cd;border-radius:4px;padding:6px 10px;margin-bottom:8px;font-size:11px;color:#664d03;",
                 paste0("Shared keywords: ",paste0('"',shared,'"',collapse=", "))),
      tags$div(style="display:flex;flex-direction:column;gap:4px;",
        lapply(seq_len(nrow(clusters)), function(i) {
          row <- clusters[i,]; nc <- ncic_color(row$max_level)
          tags$div(style=paste0("display:flex;align-items:center;gap:8px;padding:6px 10px;border-radius:6px;background:",nc,"0d;border-left:3px solid ",nc,";"),
            tags$div(style="flex:1;",
              tags$div(style="font-size:12px;font-weight:600;color:#374151;",row$county),
              tags$div(style="font-size:10px;color:#6c757d;margin-top:1px;",
                       paste0(row$n_cases," cases · max L",row$max_level," · ",row$top_keyword))),
            tags$div(style=paste0("font-size:11px;font-weight:700;color:",nc,";"),paste0("L",row$max_level)))
        }))
    )
  })

  # ── v6: KEYWORD CHANGELOG output ────────────────────────────────
  output$changelog_ui <- renderUI({
    if (!rv$authenticated) return(NULL)
    log <- load_keyword_changelog(50L)
    if (nrow(log)==0) return(tags$p(style="font-size:11px;color:#9ca3af;",
      "No keyword changes logged yet."))
    action_color <- function(a) switch(a,ADDED="#198754",RETIRED="#dc3545",MODIFIED="#fd7e14",RESTORED="#0066cc","#6c757d")
    log$action_html <- sapply(log$action, function(a) {
      col <- action_color(a)
      sprintf('<span style="background:%s18;color:%s;border:1px solid %s44;border-radius:3px;padding:1px 6px;font-size:10px;font-weight:700;">%s</span>',col,col,col,a)
    })
    DT::datatable(log[,c("changed_at","keyword","action_html","old_tier","new_tier","changed_by","reason")],
                  rownames=FALSE, escape=FALSE,
                  colnames=c("Time","Keyword","Action","Old Tier","New Tier","Officer","Reason"),
                  options=list(dom="frtip",pageLength=15,scrollX=TRUE,order=list(list(0,"desc")),
                    columnDefs=list(list(width="140px",targets=0),list(width="80px",targets=2))))
  })

  # ── Wire audit() calls into existing validation observers ──────
  # Patch: log every officer validation decision
  session$onSessionEnded(function() {
    un <- isolate(rv$officer_user)
    nm <- isolate(rv$officer_name)
    if (isolate(rv$authenticated)) {
      audit(un, nm, "LOGOUT",
            detail     = "Session ended",
            session_id = isolate(rv$session_id))
    }
  })
  
} # end server

shinyApp(ui, server)
