# ================================================================
#  R/scoring.R — Risk Scoring, Keyword Weights, v6 Clustering
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

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

# ── v6: GOLD STANDARD & EVALUATION ───────────────────────────────
db_add_gold <- function(tweet, true_level, officer, notes = "") {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
    "INSERT OR IGNORE INTO gold_standard (tweet_text,true_level,labelled_by,labelled_at,notes) VALUES (?,?,?,?,?)",
    list(substr(tweet,1,500), as.integer(true_level), officer,
         format(Sys.time(),"%Y-%m-%d %H:%M:%S"), notes))
}

db_load_gold <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(dbReadTable(con,"gold_standard"), error=function(e) data.frame())
}

db_load_eval_runs <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(dbGetQuery(con,"SELECT * FROM eval_runs ORDER BY run_at DESC LIMIT 20"),
           error=function(e) data.frame())
}

run_evaluation <- function(gold_df, threshold=3L, officer="system") {
  if (nrow(gold_df) == 0) return(NULL)
  results <- lapply(seq_len(nrow(gold_df)), function(i) {
    tweet      <- gold_df$tweet_text[i]
    true_level <- as.integer(gold_df$true_level[i])
    key        <- digest(tolower(trimws(tweet)))
    pred_level <- if (exists(key, envir=classify_cache))
      as.integer(classify_cache[[key]]$ncic_level %||% 0L) else 0L
    list(true_pos=true_level>=threshold, pred_pos=pred_level>=threshold,
         correct=true_level==pred_level)
  })
  tp <- sum(sapply(results, function(r) r$true_pos  &  r$pred_pos))
  fp <- sum(sapply(results, function(r) !r$true_pos &  r$pred_pos))
  fn <- sum(sapply(results, function(r) r$true_pos  & !r$pred_pos))
  n  <- nrow(gold_df)
  nc <- sum(sapply(results, `[[`, "correct"))
  precision <- if (tp+fp>0) round(tp/(tp+fp),3) else NA_real_
  recall    <- if (tp+fn>0) round(tp/(tp+fn),3) else NA_real_
  f1 <- if (!is.na(precision)&&!is.na(recall)&&precision+recall>0)
    round(2*precision*recall/(precision+recall),3) else NA_real_
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
    "INSERT INTO eval_runs (run_at,n_total,n_correct,precision_score,recall_score,f1_score,threshold,run_by) VALUES (?,?,?,?,?,?,?,?)",
    list(format(Sys.time(),"%Y-%m-%d %H:%M:%S"),n,nc,precision,recall,f1,as.integer(threshold),officer))
  list(n=n,correct=nc,precision=precision,recall=recall,f1=f1,tp=tp,fp=fp,fn=fn,threshold=threshold)
}

# ── v6: INTER-OFFICER AGREEMENT ───────────────────────────────────
log_officer_agreement <- function(case_id, officer_a, level_a, officer_b, level_b) {
  delta   <- abs(as.integer(level_a) - as.integer(level_b))
  flagged <- if (delta > 1L) 1L else 0L
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
    "INSERT INTO officer_agreement (case_id,officer_a,level_a,officer_b,level_b,level_delta,flagged,ts) VALUES (?,?,?,?,?,?,?,?)",
    list(case_id, officer_a, as.integer(level_a), officer_b, as.integer(level_b),
         delta, flagged, format(Sys.time(),"%Y-%m-%d %H:%M:%S")))
  flagged
}

compute_kappa <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  ag  <- tryCatch(dbReadTable(con,"officer_agreement"), error=function(e) data.frame())
  if (nrow(ag)==0) return(list(kappa=NA,n=0L,flagged=0L,pct_agree=0))
  n   <- nrow(ag)
  po  <- sum(ag$level_a==ag$level_b)/n
  pe_num <- sum(sapply(0:5,function(l) sum(ag$level_a==l)*sum(ag$level_b==l)))
  pe  <- pe_num/n^2
  kappa   <- if (pe<1) round((po-pe)/(1-pe),3) else 1.0
  flagged <- sum(ag$flagged==1L,na.rm=TRUE)
  list(kappa=kappa,n=n,flagged=flagged,pct_agree=round(po*100,1))
}

# ── v6: GPT CONFIDENCE CALIBRATION ────────────────────────────────
update_calibration <- function(gpt_conf, gpt_level, officer_level) {
  bucket  <- min(9L, as.integer(gpt_conf %/% 10))
  correct <- as.integer(abs(as.integer(gpt_level)-as.integer(officer_level))<=1L)
  con <- db_connect(); on.exit(dbDisconnect(con))
  existing <- dbGetQuery(con,"SELECT id,n_total,n_correct FROM confidence_calibration WHERE conf_bucket=?",list(bucket))
  if (nrow(existing)==0) {
    dbExecute(con,"INSERT INTO confidence_calibration (conf_bucket,n_total,n_correct,accuracy,updated_at) VALUES (?,?,?,?,?)",
              list(bucket,1L,correct,round(correct,3),format(Sys.time(),"%Y-%m-%d %H:%M:%S")))
  } else {
    new_total <- existing$n_total[1]+1L; new_correct <- existing$n_correct[1]+correct
    dbExecute(con,"UPDATE confidence_calibration SET n_total=?,n_correct=?,accuracy=?,updated_at=? WHERE conf_bucket=?",
              list(new_total,new_correct,round(new_correct/new_total,3),format(Sys.time(),"%Y-%m-%d %H:%M:%S"),bucket))
  }
}

load_calibration <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(dbGetQuery(con,"SELECT conf_bucket,n_total,n_correct,accuracy FROM confidence_calibration ORDER BY conf_bucket"),
           error=function(e) data.frame())
}

# ── v6: KEYWORD RETIREMENT ────────────────────────────────────────
fetch_stale_keywords <- function(days=90L) {
  supa_url <- Sys.getenv("SUPABASE_URL"); supa_key <- Sys.getenv("SUPABASE_KEY")
  if (nchar(supa_url)==0) return(data.frame())
  tryCatch({
    cutoff <- format(Sys.time()-as.difftime(days,units="days"),"%Y-%m-%dT%H:%M:%SZ")
    req <- request(paste0(supa_url,"/rest/v1/keyword_bank")) |>
      req_headers("apikey"=supa_key,"Authorization"=paste("Bearer",supa_key)) |>
      req_url_query(select="id,keyword,tier,category,language,times_matched,last_matched_at",
                    status="eq.approved",last_matched_at=paste0("lt.",cutoff))
    fromJSON(resp_body_string(req_perform(req)),flatten=TRUE)
  }, error=function(e) { message("[keyword retirement] ",e$message); data.frame() })
}

retire_keyword <- function(keyword_id, officer, reason="") {
  supa_url <- Sys.getenv("SUPABASE_URL"); supa_key <- Sys.getenv("SUPABASE_KEY")
  if (nchar(supa_url)==0) return(FALSE)
  tryCatch({
    req <- request(paste0(supa_url,"/rest/v1/keyword_bank")) |>
      req_headers("apikey"=supa_key,"Authorization"=paste("Bearer",supa_key),
                  "Content-Type"="application/json","Prefer"="return=minimal") |>
      req_url_query(id=paste0("eq.",keyword_id)) |>
      req_body_raw(toJSON(list(status="retired",retired_by=officer,
                               retired_at=format(Sys.time(),"%Y-%m-%dT%H:%M:%SZ"),
                               context=paste0("[RETIRED] ",reason)),auto_unbox=TRUE),
                   type="application/json") |> req_method("PATCH")
    req_perform(req)
    log_keyword_change(keyword_id,"RETIRED",officer,old_status="approved",new_status="retired",reason=reason)
    TRUE
  }, error=function(e) { message("[retire] ",e$message); FALSE })
}

# ── v6: KEYWORD CHANGELOG ─────────────────────────────────────────
log_keyword_change <- function(keyword, action, officer,
                               old_tier=NA, new_tier=NA,
                               old_status=NA, new_status=NA, reason="") {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(dbExecute(con,
    "INSERT INTO keyword_changelog (keyword,action,old_tier,new_tier,old_status,new_status,changed_by,changed_at,reason) VALUES (?,?,?,?,?,?,?,?,?)",
    list(keyword,action,
         if(is.na(old_tier)) NULL else as.integer(old_tier),
         if(is.na(new_tier)) NULL else as.integer(new_tier),
         old_status%||%NULL,new_status%||%NULL,officer,
         format(Sys.time(),"%Y-%m-%d %H:%M:%S"),reason)),
  error=function(e) NULL)
}

load_keyword_changelog <- function(n=50L) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(dbGetQuery(con,
    "SELECT keyword,action,old_tier,new_tier,old_status,new_status,changed_by,changed_at,reason FROM keyword_changelog ORDER BY changed_at DESC LIMIT ?",
    list(n)), error=function(e) data.frame())
}

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

