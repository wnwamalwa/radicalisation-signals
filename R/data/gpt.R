# ================================================================
#  R/gpt.R — GPT Classification, Language Detection,
#            Email Alerts, Prophet Forecast Engine
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

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
                            passwordInput("auth_password", NULL, placeholder="Password", width="100%"),
                            tags$div(style="font-size:10px;color:#9ca3af;margin-bottom:10px;line-height:1.6;",
                                     "Password must be 10+ characters with uppercase, lowercase, digit, and special character."),
                            actionButton("auth_submit","Sign In",
                                         style="width:100%;background:#0066cc;color:#fff;border:none;font-weight:700;font-size:14px;padding:11px;border-radius:6px;"),
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
  
  if (n_recent >= MIN_PROPHET_OBS) {
    daily <- h |>
      mutate(ds = as.Date(timestamp)) |>
      group_by(ds) |>
      summarise(y = mean(risk_score, na.rm=TRUE), .groups="drop") |>
      arrange(ds)
    
    # Need at least MIN_PROPHET_OBS distinct days
    if (nrow(daily) >= MIN_PROPHET_OBS) {
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

# ── Run forecast for all counties (cached per session) ───────────
build_county_forecasts <- function(cases_df, now = Sys.time()) {
  results <- lapply(counties$name, function(co) {
    tryCatch(
      run_prophet_forecast(cases_df, co, now),
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
