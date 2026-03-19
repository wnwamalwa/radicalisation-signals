# ============================================================
#  Kenya Hate Speech Intelligence Platform
#  R Shiny + bslib | Light Theme | OpenAI | Email Forwarding
# ============================================================
#  Install dependencies (run once):
#    install.packages(c("shiny","bslib","bsicons","leaflet",
#                       "leaflet.extras2","DT","dplyr","writexl",
#                       "httr2","jsonlite","shinyjs","emayili"))
#
#  secrets/.Renviron must contain:
#    OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxx
#    GMAIL_USER=youremail@gmail.com
#    GMAIL_PASS=xxxx xxxx xxxx xxxx
#    OFFICER_EMAIL=officer@police.go.ke
# ============================================================

readRenviron("secrets/.Renviron")

library(shiny)
library(bslib)
library(bsicons)
library(leaflet)
library(leaflet.extras2)
library(DT)
library(dplyr)
library(writexl)
library(httr2)
library(jsonlite)
library(shinyjs)
# emayili removed — email sent via Gmail SMTP using base R system call

# ── NULL COALESCING ───────────────────────────────────────────
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── CONSTANTS ─────────────────────────────────────────────────
OFFICER_PASSWORD <- "intel2025"
OPENAI_MODEL     <- "gpt-4o-mini"

# ── SEED DATA ─────────────────────────────────────────────────
set.seed(42)

counties <- data.frame(
  name  = c("Nairobi","Mombasa","Kisumu","Nakuru","Eldoret",
            "Garissa","Mandera","Kakamega","Meru","Nyeri",
            "Kilifi","Kitui","Turkana","Wajir","Kisii"),
  lat   = c(-1.286,-4.043,-0.102,-0.303, 0.520,
            -0.454, 3.937, 0.281, 0.047,-0.417,
            -3.629,-1.366, 3.119, 1.747,-0.682),
  lng   = c(36.817,39.668,34.762,36.080,35.270,
            39.646,41.855,34.752,37.649,36.950,
            39.852,38.012,35.597,40.058,34.768),
  count = c(187,78,95,62,54,43,38,51,29,22,35,18,15,24,41),
  stringsAsFactors = FALSE
)

tweet_texts <- c(
  "Watu wa kabila hiyo hawana haki ya kuishi hapa Kenya yetu",
  "These politicians are corrupt and should face consequences for their actions",
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
  "Religious extremists are ruining our peaceful coexistence in this region",
  "Sisi sote ni ndugu, tunaomba amani na upendo Kenya nzima"
)

platforms  <- c("Twitter/X","Facebook","TikTok","Telegram","WhatsApp")
languages  <- c("Swahili","English","Sheng","Kikuyu","Luo","Kalenjin")
categories <- c("Ethnicity","Religion","Gender","Political","Regional","Incitement")
handles    <- c("@user_ke","@nairobi_talk","@kisumu_voice","@mombasa_news","@kenyatalk")
ml_labels  <- c("TOXIC","HATE SPEECH","SAFE","SAFE","PENDING","HATE SPEECH")

all_cases <- do.call(rbind, lapply(seq_len(nrow(counties)), function(i) {
  n <- counties$count[i]
  data.frame(
    case_id      = paste0("KE-", formatC(
      seq(sum(counties$count[seq_len(i-1)]) + 1,
          sum(counties$count[seq_len(i)])),
      width = 5, flag = "0")),
    county       = counties$name[i],
    platform     = sample(platforms,   n, replace = TRUE),
    handle       = sample(handles,     n, replace = TRUE),
    tweet_text   = sample(tweet_texts, n, replace = TRUE),
    language     = sample(languages,   n, replace = TRUE),
    ml_label     = sample(ml_labels,   n, replace = TRUE),
    confidence   = paste0(sample(62:99, n, replace = TRUE), "%"),
    conf_num     = sample(62:99,        n, replace = TRUE),
    category     = sample(categories,  n, replace = TRUE),
    validated_by = sample(c("Officer Mwangi","Officer Njeri",
                            "Officer Otieno", NA, NA, NA),
                          n, replace = TRUE),
    timestamp    = format(
      Sys.time() - sample(0:(30*86400), n, replace = TRUE),
      "%Y-%m-%d %H:%M"),
    action_taken = NA_character_,
    stringsAsFactors = FALSE
  )
}))

# ── OPENAI CLASSIFY ───────────────────────────────────────────
classify_tweet <- function(tweet) {
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (nchar(api_key) == 0)
    stop("OPENAI_API_KEY not found. Check secrets/.Renviron")
  
  prompt <- paste0(
    "You are a hate speech detection AI specialised in Kenyan social media.\n",
    "Analyse the post and respond ONLY with valid JSON — no markdown, no backticks.\n\n",
    'Post: "', tweet, '"\n\n',
    'Return: {"label":"TOXIC" or "HATE SPEECH" or "SAFE",',
    '"confidence":<integer 0-100>,',
    '"category":"Ethnicity" or "Religion" or "Gender" or ',
    '"Political" or "Regional" or "Incitement" or "None",',
    '"reasoning":"<one sentence>",',
    '"action":"<short recommended action for officers>"}'
  )
  
  resp <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(
      "Content-Type"  = "application/json",
      "Authorization" = paste("Bearer", api_key)
    ) |>
    req_body_json(list(
      model       = OPENAI_MODEL,
      max_tokens  = 300,
      temperature = 0,
      messages    = list(
        list(role = "system",
             content = "You are a hate speech classifier. Respond with valid JSON only."),
        list(role = "user", content = prompt)
      )
    )) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform() |>
    resp_body_json()
  
  if (!is.null(resp$error)) stop(resp$error$message)
  
  raw <- gsub("```json|```|\\n", "", trimws(resp$choices[[1]]$message$content))
  fromJSON(raw)
}

send_alert_email <- function(case_id, ml_label, county, platform, handle,
                             category, language, tweet_text, timestamp,
                             decision, note) {
  
  gmail_user    <- Sys.getenv("GMAIL_USER")
  gmail_pass    <- Sys.getenv("GMAIL_PASS")
  officer_email <- Sys.getenv("OFFICER_EMAIL")
  
  if (nchar(gmail_user) == 0 || nchar(gmail_pass) == 0 || nchar(officer_email) == 0)
    stop("Email credentials missing. Check GMAIL_USER, GMAIL_PASS, OFFICER_EMAIL in secrets/.Renviron")
  
  hdr_col <- if (decision == "CONFIRMED TOXIC") "#dc3545" else
    if (decision == "DOWNGRADED")       "#fd7e14" else "#198754"
  
  subject_line <- paste0("[KENYA HS INTEL] ", decision, " - Case ", case_id)
  
  html_body <- paste0(
    "<html><body style='font-family:Arial,sans-serif;color:#1a1a2e;'>",
    "<div style='background:", hdr_col, ";padding:16px 20px;border-radius:8px 8px 0 0;'>",
    "<h2 style='color:#fff;margin:0;font-size:18px;'>Kenya HS Intelligence Platform</h2>",
    "<p style='color:rgba(255,255,255,0.85);margin:4px 0 0;font-size:13px;'>",
    "Automated Alert - Human Validation Completed</p></div>",
    "<div style='background:#f8f9fa;padding:20px;border:1px solid #dee2e6;",
    "border-top:none;border-radius:0 0 8px 8px;'>",
    "<table style='width:100%;border-collapse:collapse;font-size:13px;'>",
    "<tr><td style='padding:8px 12px;font-weight:600;width:140px;border-bottom:1px solid #dee2e6;'>Case ID</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", case_id, "</td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>Decision</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>",
    "<strong style='color:", hdr_col, ";'>", decision, "</strong></td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>ML Label</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", ml_label, "</td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>County</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", county, "</td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>Platform</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", platform, "</td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>Handle</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", handle, "</td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>Category</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", category, "</td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>Language</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", language, "</td></tr>",
    "<tr><td style='padding:8px 12px;font-weight:600;border-bottom:1px solid #dee2e6;'>Timestamp</td>",
    "<td style='padding:8px 12px;border-bottom:1px solid #dee2e6;'>", timestamp, "</td></tr>",
    "</table>",
    "<div style='margin-top:16px;'>",
    "<p style='font-weight:600;margin-bottom:6px;'>Tweet Content:</p>",
    "<div style='background:#fff;border-left:4px solid ", hdr_col, ";",
    "padding:12px 14px;border-radius:4px;font-size:13px;line-height:1.6;",
    "border:1px solid #dee2e6;'>", tweet_text, "</div></div>",
    if (nchar(trimws(note)) > 0)
      paste0("<div style='margin-top:14px;'>",
             "<p style='font-weight:600;margin-bottom:6px;'>Officer Note:</p>",
             "<div style='background:#fff8e1;border-left:4px solid #ffc107;",
             "padding:10px 14px;border-radius:4px;font-size:13px;'>",
             note, "</div></div>")
    else "",
    "<div style='margin-top:20px;padding-top:14px;border-top:1px solid #dee2e6;",
    "font-size:11px;color:#868e96;'>Generated at ",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " EAT.</div>",
    "</div></body></html>"
  )
  
  # Build raw MIME message
  mime_msg <- paste0(
    "From: ", gmail_user, "\r\n",
    "To: ", officer_email, "\r\n",
    "Subject: ", subject_line, "\r\n",
    "MIME-Version: 1.0\r\n",
    "Content-Type: text/html; charset=UTF-8\r\n",
    "\r\n",
    html_body
  )
  
  # Write message to temp file for curl
  tmp <- tempfile(fileext = ".txt")
  writeLines(mime_msg, tmp, useBytes = FALSE)
  on.exit(unlink(tmp))
  
  # Send via curl SMTP with STARTTLS
  cmd <- paste0(
    "curl --url 'smtps://smtp.gmail.com:465' ",
    "--ssl-reqd ",
    "--mail-from '", gmail_user, "' ",
    "--mail-rcpt '", officer_email, "' ",
    "--user '", gmail_user, ":", gmail_pass, "' ",
    "--upload-file '", tmp, "' ",
    "--silent --show-error"
  )
  
  result <- system(cmd, intern = TRUE, ignore.stderr = FALSE)
  status <- attr(result, "status") %||% 0
  
  if (!is.null(status) && status != 0)
    stop(paste("curl SMTP failed with exit code:", status))
  
  invisible(TRUE)
}

# ── THEME (LIGHT) ─────────────────────────────────────────────
hs_theme <- bs_theme(
  version      = 5,
  bg           = "#f0f4f8",
  fg           = "#1a1a2e",
  primary      = "#0066cc",
  secondary    = "#e9ecef",
  success      = "#198754",
  danger       = "#dc3545",
  warning      = "#fd7e14",
  info         = "#0dcaf0",
  base_font    = font_google("IBM Plex Sans"),
  code_font    = font_google("IBM Plex Mono"),
  heading_font = font_google("IBM Plex Sans"),
  bootswatch   = NULL
) |>
  bs_add_rules("
    body { background:#f0f4f8 !important; }
    .navbar { background:#1a1a2e !important; border-bottom:2px solid #0066cc; }
    .nav-item .nav-link { color:rgba(255,255,255,0.75) !important;
                          font-size:13px; font-weight:500; }
    .nav-item .nav-link:hover { color:#fff !important; }
    .nav-item .nav-link.active { color:#fff !important; background:#0066cc !important;
                                  border-radius:6px; }
    .sidebar { background:#ffffff !important;
               border-right:1px solid #dee2e6 !important;
               box-shadow:2px 0 8px rgba(0,0,0,0.06); }
    .accordion-button { background:#f8f9fa !important; color:#1a1a2e !important;
                        font-size:13px; font-weight:600; border:none !important;
                        box-shadow:none !important; }
    .accordion-button:not(.collapsed) { background:#0066cc !important;
                                         color:#fff !important; }
    .accordion-body  { background:#ffffff !important; padding:14px; }
    .accordion-item  { border:1px solid #dee2e6 !important; border-radius:8px !important;
                       margin-bottom:6px; overflow:hidden;
                       box-shadow:0 1px 3px rgba(0,0,0,0.06); }
    .card        { background:#ffffff !important; border:1px solid #dee2e6 !important;
                   border-radius:10px; box-shadow:0 2px 8px rgba(0,0,0,0.06); }
    .card-header { background:#f8f9fa !important; border-bottom:1px solid #dee2e6;
                   font-size:13px; font-weight:600; padding:10px 14px; color:#1a1a2e; }
    .value-box   { border-radius:10px; box-shadow:0 2px 8px rgba(0,0,0,0.08); }
    .badge-toxic   { background:rgba(220,53,69,0.12);  color:#dc3545;
                     border:1px solid rgba(220,53,69,0.3);
                     padding:3px 8px; border-radius:4px; font-size:11px; font-weight:600; }
    .badge-hate    { background:rgba(253,126,20,0.12); color:#fd7e14;
                     border:1px solid rgba(253,126,20,0.3);
                     padding:3px 8px; border-radius:4px; font-size:11px; font-weight:600; }
    .badge-safe    { background:rgba(25,135,84,0.12);  color:#198754;
                     border:1px solid rgba(25,135,84,0.3);
                     padding:3px 8px; border-radius:4px; font-size:11px; font-weight:600; }
    .badge-pending { background:rgba(0,102,204,0.1);   color:#0066cc;
                     border:1px solid rgba(0,102,204,0.25);
                     padding:3px 8px; border-radius:4px; font-size:11px; font-weight:600; }
    .chat-container { display:flex; flex-direction:column; gap:8px; max-height:340px;
                      overflow-y:auto; padding:4px 0; margin-bottom:10px; }
    .chat-msg       { padding:9px 11px; border-radius:8px; font-size:12px; line-height:1.55; }
    .chat-user      { background:#e8f0fe; border-left:3px solid #0066cc;
                      margin-left:8px; color:#1a1a2e; }
    .chat-bot       { background:#f8f9fa; border-left:3px solid #fd7e14;
                      margin-right:8px; color:#1a1a2e; }
    .chat-bot .result-label { font-weight:700; font-size:13px; margin-bottom:4px; }
    .chat-thinking  { color:#6c757d; font-style:italic; font-size:11px; }
    .chat-input-row { display:flex; gap:6px; align-items:flex-end; }
    .chat-textarea  { flex:1; background:#f8f9fa !important;
                      border:1px solid #ced4da !important; border-radius:6px;
                      color:#1a1a2e !important; font-size:12px !important;
                      resize:none; padding:8px 10px; }
    .chat-textarea:focus { border-color:#0066cc !important; outline:none;
                           box-shadow:0 0 0 3px rgba(0,102,204,0.15) !important; }
    .btn-classify { background:#0066cc; color:#fff; border:none; border-radius:6px;
                    padding:8px 14px; font-size:16px; font-weight:700; cursor:pointer; }
    .btn-classify:hover { background:#0052a3; }
    .human-notice { background:#fff3cd; border:1px solid #ffc107; border-radius:6px;
                    padding:10px 12px; font-size:11px; color:#664d03; margin-top:8px; }
    .error-msg { background:#fff5f5; border:1px solid rgba(220,53,69,0.3);
                 border-radius:6px; padding:8px 10px; color:#dc3545;
                 font-size:11px; margin-top:6px; font-family:'IBM Plex Mono'; }
    .auth-box    { background:#ffffff; border:1px solid #dee2e6; border-radius:12px;
                   padding:36px; width:380px; text-align:center;
                   box-shadow:0 8px 32px rgba(0,0,0,0.1); }
    .auth-box h3 { font-size:20px; font-weight:700; margin-bottom:6px; color:#1a1a2e; }
    .auth-box p  { color:#6c757d; font-size:13px; margin-bottom:20px; }
    .val-tweet-box { padding:12px 14px; border-radius:6px; font-size:13px;
                     line-height:1.6; margin-bottom:12px; background:#f8f9fa;
                     border:1px solid #e9ecef; }
    .btn-val-confirm   { background:#dc3545; color:#fff; border:none; border-radius:5px;
                         padding:6px 12px; font-size:12px; font-weight:600; cursor:pointer; }
    .btn-val-confirm:hover   { opacity:.85; }
    .btn-val-downgrade { background:#fd7e14; color:#fff; border:none; border-radius:5px;
                         padding:6px 12px; font-size:12px; font-weight:600; cursor:pointer; }
    .btn-val-downgrade:hover { opacity:.85; }
    .btn-val-clear     { background:#198754; color:#fff; border:none; border-radius:5px;
                         padding:6px 12px; font-size:12px; font-weight:600; cursor:pointer; }
    .btn-val-clear:hover     { opacity:.85; }
    .btn-val-email     { background:#0066cc; color:#fff; border:none; border-radius:5px;
                         padding:6px 12px; font-size:12px; font-weight:600; cursor:pointer; }
    .btn-val-email:hover     { opacity:.85; }
    .email-sent { display:inline-flex; align-items:center; gap:5px;
                  background:rgba(25,135,84,0.1); color:#198754;
                  border:1px solid rgba(25,135,84,0.3); border-radius:4px;
                  padding:3px 8px; font-size:11px; font-weight:600; }
    .stat-mini { background:#f8f9fa; border:1px solid #dee2e6; border-radius:8px;
                 padding:10px; text-align:center; }
    .dataTables_wrapper, table.dataTable { background:#ffffff !important;
                                           color:#1a1a2e !important; }
    .dataTables_wrapper .dataTables_filter input,
    .dataTables_wrapper .dataTables_length select {
      background:#f8f9fa !important; color:#1a1a2e !important;
      border:1px solid #ced4da !important; border-radius:4px; }
    table.dataTable thead th { background:#f8f9fa !important; color:#495057 !important;
      font-family:'IBM Plex Mono',monospace; font-size:10px; text-transform:uppercase;
      letter-spacing:.08em; border-bottom:2px solid #dee2e6 !important; }
    table.dataTable tbody tr       { background:#ffffff !important; }
    table.dataTable tbody tr:hover { background:#f0f4f8 !important; }
    table.dataTable tbody td { border-bottom:1px solid #f0f0f0 !important; font-size:12px; }
    .leaflet-popup-content-wrapper { background:#ffffff !important; color:#1a1a2e !important;
      border:1px solid #dee2e6 !important; border-radius:8px !important;
      box-shadow:0 4px 16px rgba(0,0,0,0.12) !important; }
    .leaflet-popup-tip { background:#ffffff !important; }
    .live-badge { display:inline-flex; align-items:center; gap:6px; font-size:11px;
                  font-family:'IBM Plex Mono',monospace; color:#90ee90; }
    .live-dot   { width:7px; height:7px; border-radius:50%; background:#90ee90;
                  animation:pulse 2s infinite; display:inline-block; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
    .form-control, .form-select { background:#f8f9fa !important;
      border:1px solid #ced4da !important; color:#1a1a2e !important; }
    .form-control:focus, .form-select:focus {
      border-color:#0066cc !important;
      box-shadow:0 0 0 3px rgba(0,102,204,0.15) !important; }
    ::-webkit-scrollbar       { width:5px; height:5px; }
    ::-webkit-scrollbar-track { background:#f0f4f8; }
    ::-webkit-scrollbar-thumb { background:#ced4da; border-radius:3px; }
  ")

# ── HELPERS ───────────────────────────────────────────────────
badge_html <- function(label) {
  cls <- switch(label,
                "TOXIC"       = "badge-toxic",
                "HATE SPEECH" = "badge-hate",
                "SAFE"        = "badge-safe",
                "PENDING"     = "badge-pending",
                "badge-pending"
  )
  as.character(tags$span(class = cls, label))
}

auth_wall_ui <- function() {
  tagList(
    useShinyjs(),
    tags$div(
      style = "display:flex;align-items:center;justify-content:center;min-height:440px;",
      tags$div(
        class = "auth-box",
        tags$div(style = "font-size:52px;margin-bottom:12px;", "🛡️"),
        tags$h3("Officer Access"),
        tags$p("This section is restricted to authorised personnel only."),
        passwordInput("auth_password", NULL,
                      placeholder = "Enter access code", width = "100%"),
        tags$div(style = "margin-bottom:10px;"),
        actionButton("auth_submit", "Authenticate",
                     style = "width:100%;background:#0066cc;color:#fff;border:none;
                   font-weight:700;font-size:14px;padding:11px;border-radius:6px;"),
        tags$div(id = "auth_error",
                 style = "color:#dc3545;font-size:12px;margin-top:8px;")
      )
    )
  )
}

# ════════════════════════════════════════════════════════════
#  UI
# ════════════════════════════════════════════════════════════
ui <- page_navbar(
  title = tags$span(
    tags$span(
      style = "background:linear-gradient(135deg,#0066cc,#dc3545);
               border-radius:5px;padding:3px 8px;font-size:14px;margin-right:8px;",
      "🛡"
    ),
    tags$strong(style = "color:#fff;", "Kenya HS Intel Platform"),
    tags$span(class = "live-badge ms-3",
              tags$span(class = "live-dot"), "LIVE")
  ),
  theme    = hs_theme,
  fillable = TRUE,
  id       = "main_nav",
  
  # ── TAB 1: MAP ───────────────────────────────────────────────
  nav_panel(
    title = tagList(bs_icon("map"), " Map Overview"),
    value = "tab_map",
    
    layout_sidebar(
      fillable = TRUE,
      
      sidebar = sidebar(
        id    = "sidebar_map",
        width = 320,
        open  = TRUE,
        bg    = "#ffffff",
        
        accordion(
          id   = "acc_map",
          open = c("acc_chatbot","acc_filters"),
          
          # ── ML Chatbot ──────────────────────────────────────
          accordion_panel(
            title = tagList(bs_icon("robot"), " ML Tweet Classifier"),
            value = "acc_chatbot",
            
            tags$p(style = "font-size:11px;color:#6c757d;margin-bottom:8px;",
                   "Paste any tweet — GPT will classify it instantly."),
            
            uiOutput("chat_history"),
            
            tags$div(
              class = "chat-input-row",
              tags$textarea(
                id          = "chat_input",
                class       = "form-control chat-textarea",
                placeholder = "Type or paste a tweet...",
                rows        = 3
              ),
              tags$button("→", id = "chat_send", class = "btn-classify",
                          onclick = "Shiny.setInputValue('chat_send', Math.random())")
            ),
            
            tags$div(class = "human-notice",
                     tags$strong("⚠ Human validation required"),
                     " — AI labels must be confirmed by an officer before any action."
            ),
            
            tags$hr(style = "border-color:#dee2e6;margin:10px 0;"),
            
            tags$p(style = "font-size:10px;color:#6c757d;font-family:'IBM Plex Mono';
                            text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px;",
                   "Quick examples"),
            tags$div(style = "display:flex;flex-direction:column;gap:4px;",
                     actionButton("ex1", "🔴 Ethnicity example",
                                  class = "btn btn-sm btn-outline-secondary w-100",
                                  style = "font-size:11px;text-align:left;"),
                     actionButton("ex2", "🟡 Incitement example",
                                  class = "btn btn-sm btn-outline-secondary w-100",
                                  style = "font-size:11px;text-align:left;"),
                     actionButton("ex3", "🟢 Safe / neutral tweet",
                                  class = "btn btn-sm btn-outline-secondary w-100",
                                  style = "font-size:11px;text-align:left;")
            )
          ),
          
          # ── Map Filters ─────────────────────────────────────
          accordion_panel(
            title = tagList(bs_icon("funnel"), " Map Filters"),
            value = "acc_filters",
            
            selectInput("filter_county", "County",
                        choices  = c("All Counties", counties$name),
                        selected = "All Counties", width = "100%"),
            selectInput("filter_label", "ML Label",
                        choices  = c("All","TOXIC","HATE SPEECH","SAFE","PENDING"),
                        selected = "All", width = "100%"),
            selectInput("filter_platform", "Platform",
                        choices  = c("All", platforms),
                        selected = "All", width = "100%"),
            sliderInput("filter_conf", "Min. Confidence (%)",
                        min = 0, max = 100, value = 0, step = 5, width = "100%")
          ),
          
          # ── Quick Stats ─────────────────────────────────────
          accordion_panel(
            title = tagList(bs_icon("bar-chart"), " Quick Stats"),
            value = "acc_stats",
            uiOutput("sidebar_stats")
          ),
          
          # ── Legend ──────────────────────────────────────────
          accordion_panel(
            title = tagList(bs_icon("info-circle"), " Map Legend"),
            value = "acc_legend",
            tags$div(style = "font-size:12px;",
                     tags$div(style = "display:flex;align-items:center;gap:8px;margin-bottom:6px;",
                              tags$div(style = "width:14px;height:14px;border-radius:50%;
                                  background:#dc3545;flex-shrink:0;"),
                              "High risk  > 100 incidents"),
                     tags$div(style = "display:flex;align-items:center;gap:8px;margin-bottom:6px;",
                              tags$div(style = "width:14px;height:14px;border-radius:50%;
                                  background:#fd7e14;flex-shrink:0;"),
                              "Medium  50 - 100"),
                     tags$div(style = "display:flex;align-items:center;gap:8px;",
                              tags$div(style = "width:14px;height:14px;border-radius:50%;
                                  background:#198754;flex-shrink:0;"),
                              "Low  < 50")
            )
          )
        ) # end accordion
      ), # end sidebar
      
      card(
        full_screen = TRUE,
        card_header(tagList(bs_icon("geo-alt"),
                            " Kenya Incident Heatmap - click a circle to view cases")),
        leafletOutput("kenya_map", height = "620px")
      )
    ) # end layout_sidebar
  ), # end MAP
  
  # ── TAB 2: RAW SIGNALS ──────────────────────────────────────
  nav_panel(
    title = tagList(bs_icon("broadcast"), " Raw Signals"),
    value = "tab_signals", padding = 16,
    card(
      card_header("Unprocessed Signal Feed - awaiting ML classification"),
      DTOutput("raw_signals_table")
    )
  ),
  
  # ── TAB 3: ML CLASSIFICATION ────────────────────────────────
  nav_panel(
    title = tagList(bs_icon("cpu"), " ML Classification"),
    value = "tab_classify", padding = 16,
    
    layout_columns(
      col_widths = c(3,3,3,3),
      value_box("Total Signals", nrow(all_cases),
                showcase = bs_icon("collection"),           theme = "primary"),
      value_box("Toxic / Hate",
                sum(all_cases$ml_label %in% c("TOXIC","HATE SPEECH")),
                showcase = bs_icon("exclamation-triangle"), theme = "danger"),
      value_box("Validated",
                sum(!is.na(all_cases$validated_by)),
                showcase = bs_icon("shield-check"),         theme = "success"),
      value_box("Pending Review",
                sum(all_cases$ml_label == "PENDING"),
                showcase = bs_icon("hourglass"),            theme = "warning")
    ),
    
    card(
      card_header(tagList(
        bs_icon("table"), " Classified Cases",
        tags$div(style = "float:right;",
                 selectInput("clf_filter", NULL, width = "160px",
                             choices  = c("All","TOXIC","HATE SPEECH","SAFE","PENDING"),
                             selected = "All"))
      )),
      DTOutput("classified_table")
    )
  ),
  
  # ── TAB 4: VALIDATION ───────────────────────────────────────
  nav_panel(
    title = tagList(bs_icon("lock"), " Validation"),
    value = "tab_validate", padding = 16,
    uiOutput("validation_ui")
  ),
  
  # ── TAB 5: REPORTS ──────────────────────────────────────────
  nav_panel(
    title = tagList(bs_icon("lock"), " Reports"),
    value = "tab_reports", padding = 16,
    uiOutput("reports_ui")
  )
)

# ════════════════════════════════════════════════════════════
#  SERVER
# ════════════════════════════════════════════════════════════
server <- function(input, output, session) {
  
  rv <- reactiveValues(
    authenticated = FALSE,
    chat_history  = list(),
    cases         = all_cases,
    email_status  = list()
  )
  
  # ── Email status renders (defined once at startup) ────────
  # Must be OUTSIDE observe() to avoid emayili receiving render functions
  lapply(seq_len(min(6, nrow(all_cases))), function(i) {
    cid <- all_cases$case_id[i]
    output[[paste0("email_status_", cid)]] <- renderUI({
      status <- rv$email_status[[cid]]
      if (is.null(status)) return(NULL)
      if (status == "sending")
        tags$div(style = "font-size:11px;color:#6c757d;margin-top:6px;",
                 "Sending email...")
      else if (status == "sent")
        tags$div(class = "email-sent", style = "margin-top:6px;",
                 "Alert email sent successfully")
      else
        tags$div(class = "error-msg", style = "margin-top:6px;",
                 tags$strong("Email failed: "),
                 gsub("failed: ", "", status))
    })
  })
  
  # ── Sidebar stats ────────────────────────────────────────
  output$sidebar_stats <- renderUI({
    d <- rv$cases
    mk <- function(val, lbl, col) {
      tags$div(class = "stat-mini",
               tags$div(style = paste0("font-size:22px;font-weight:700;color:", col, ";"), val),
               tags$div(style = "font-size:10px;color:#6c757d;font-family:'IBM Plex Mono';", lbl)
      )
    }
    tags$div(style = "display:grid;grid-template-columns:1fr 1fr;gap:8px;",
             mk(sum(d$ml_label == "TOXIC"),       "TOXIC",   "#dc3545"),
             mk(sum(d$ml_label == "HATE SPEECH"), "HATE",    "#fd7e14"),
             mk(sum(d$ml_label == "SAFE"),        "SAFE",    "#198754"),
             mk(sum(d$ml_label == "PENDING"),     "PENDING", "#0066cc")
    )
  })
  
  # ── Map ──────────────────────────────────────────────────
  output$kenya_map <- renderLeaflet({
    leaflet(options = leafletOptions(zoomControl = TRUE)) |>
      addProviderTiles("CartoDB.Positron") |>
      setView(lng = 37.9, lat = 0.02, zoom = 6)
  })
  
  observe({
    d <- rv$cases
    if (!is.null(input$filter_county)   && input$filter_county   != "All Counties")
      d <- d[d$county   == input$filter_county, ]
    if (!is.null(input$filter_label)    && input$filter_label    != "All")
      d <- d[d$ml_label == input$filter_label, ]
    if (!is.null(input$filter_platform) && input$filter_platform != "All")
      d <- d[d$platform == input$filter_platform, ]
    
    agg <- d |>
      group_by(county) |>
      summarise(n = n(), .groups = "drop") |>
      left_join(counties, by = c("county" = "name"))
    
    proxy <- leafletProxy("kenya_map")
    proxy |> clearMarkers() |> clearShapes()
    
    for (i in seq_len(nrow(agg))) {
      co  <- agg$county[i]; n <- agg$n[i]
      lat <- agg$lat[i];   lng <- agg$lng[i]
      col <- if (n > 100) "#dc3545" else if (n > 50) "#fd7e14" else "#198754"
      r   <- max(8, min(30, sqrt(n) * 2.5))
      
      co_cases <- d[d$county == co, ][seq_len(min(8, sum(d$county == co))), ]
      rows <- paste0(
        "<tr>",
        "<td style='font-family:monospace;font-size:11px;color:#6c757d;padding:4px 6px;'>",
        co_cases$case_id, "</td>",
        "<td style='font-size:11px;padding:4px 6px;'>",
        substr(co_cases$tweet_text, 1, 50), "...</td>",
        "<td style='font-size:11px;padding:4px 6px;'>", co_cases$ml_label, "</td>",
        "<td style='font-size:11px;padding:4px 6px;'>", co_cases$confidence, "</td>",
        "</tr>", collapse = "")
      
      popup_html <- paste0(
        "<div style='min-width:380px;font-family:Arial,sans-serif;color:#1a1a2e;'>",
        "<div style='font-size:13px;font-weight:600;color:#0066cc;",
        "margin-bottom:10px;border-bottom:1px solid #dee2e6;padding-bottom:6px;'>",
        "📍 ", co, " — ", n, " incidents</div>",
        "<table style='width:100%;border-collapse:collapse;'>",
        "<tr style='background:#f8f9fa;'>",
        "<th style='padding:5px 7px;font-size:10px;color:#6c757d;'>ID</th>",
        "<th style='padding:5px 7px;font-size:10px;color:#6c757d;'>CONTENT</th>",
        "<th style='padding:5px 7px;font-size:10px;color:#6c757d;'>LABEL</th>",
        "<th style='padding:5px 7px;font-size:10px;color:#6c757d;'>CONF</th>",
        "</tr>", rows, "</table>",
        "<div style='margin-top:6px;font-size:11px;color:#6c757d;'>",
        "Showing ", min(8, n), " of ", n, " cases</div></div>"
      )
      
      proxy |> addCircleMarkers(
        lat = lat, lng = lng, radius = r,
        color = col, fillColor = col, fillOpacity = 0.6, weight = 2,
        popup = popup_html, label = paste0(co, " (", n, ")")
      )
    }
  })
  
  # ── Chatbot examples ─────────────────────────────────────
  observeEvent(input$ex1, {
    updateTextAreaInput(session, "chat_input",
                        value = "Watu wa kabila hiyo hawana haki ya kuishi hapa Kenya yetu")
  })
  observeEvent(input$ex2, {
    updateTextAreaInput(session, "chat_input",
                        value = "Kesho tutawaonyesha nguvu yetu, waambie wajiandae!")
  })
  observeEvent(input$ex3, {
    updateTextAreaInput(session, "chat_input",
                        value = "Let us unite as Kenyans and build this nation together")
  })
  
  # ── Chatbot render ────────────────────────────────────────
  output$chat_history <- renderUI({
    msgs <- rv$chat_history
    if (length(msgs) == 0) {
      return(tags$div(class = "chat-container",
                      tags$div(class = "chat-msg chat-bot",
                               tags$span(class = "chat-thinking",
                                         "👋 Paste a tweet above and GPT will classify it."))))
    }
    tags$div(
      class = "chat-container", id = "chat_scroll",
      lapply(msgs, function(m) {
        if (m$role == "user") {
          tags$div(class = "chat-msg chat-user",
                   tags$div(style = "font-size:10px;color:#6c757d;
                              font-family:'IBM Plex Mono';margin-bottom:3px;", "YOU"),
                   m$text)
        } else if (m$role == "thinking") {
          tags$div(class = "chat-msg chat-bot",
                   tags$span(class = "chat-thinking", "⏳ Classifying with GPT..."))
        } else if (m$role == "error") {
          tags$div(class = "error-msg", tags$strong("API Error: "), m$text)
        } else {
          lcol <- switch(m$label,
                         "TOXIC"       = "color:#dc3545",
                         "HATE SPEECH" = "color:#fd7e14",
                         "SAFE"        = "color:#198754",
                         "color:#0066cc")
          licon <- switch(m$label,
                          "TOXIC"       = "🔴 TOXIC",
                          "HATE SPEECH" = "🟡 HATE SPEECH",
                          "SAFE"        = "🟢 SAFE", "🔵 UNKNOWN")
          tags$div(class = "chat-msg chat-bot",
                   tags$div(style = "font-size:10px;color:#6c757d;
                              font-family:'IBM Plex Mono';margin-bottom:3px;",
                            paste0("ML ENGINE · ", OPENAI_MODEL)),
                   tags$div(class = "result-label", style = lcol, licon),
                   tags$div(style = "font-size:11px;margin-top:5px;",
                            tags$strong("Confidence: "), paste0(m$conf, "%"),
                            "  |  ", tags$strong("Category: "), m$category),
                   tags$div(style = "font-size:11px;color:#6c757d;
                              margin-top:4px;font-style:italic;", m$reasoning),
                   tags$div(style = "font-size:11px;background:#e8f0fe;border-radius:4px;
                              padding:5px 8px;margin-top:6px;color:#0066cc;",
                            "📋 ", m$action))
        }
      }),
      tags$script(
        "var el=document.getElementById('chat_scroll');
         if(el) el.scrollTop=el.scrollHeight;")
    )
  })
  
  # ── Chatbot send ──────────────────────────────────────────
  observeEvent(input$chat_send, {
    req(input$chat_input, nchar(trimws(input$chat_input)) > 0)
    tweet <- trimws(input$chat_input)
    rv$chat_history <- c(rv$chat_history,
                         list(list(role = "user", text = tweet)),
                         list(list(role = "thinking")))
    updateTextAreaInput(session, "chat_input", value = "")
    
    result <- tryCatch(classify_tweet(tweet),
                       error = function(e) list(role = "error", text = conditionMessage(e)))
    
    hist <- rv$chat_history
    if (!is.null(result$role) && result$role == "error") {
      hist[[length(hist)]] <- result
    } else {
      hist[[length(hist)]] <- list(
        role      = "bot",
        label     = result$label      %||% "PENDING",
        conf      = result$confidence %||% 0,
        category  = result$category   %||% "Unknown",
        reasoning = result$reasoning  %||% "",
        action    = result$action     %||% "")
    }
    rv$chat_history <- hist
  })
  
  # ── Raw signals table ─────────────────────────────────────
  output$raw_signals_table <- renderDT({
    d <- rv$cases[, c("case_id","platform","handle","tweet_text","county","timestamp")]
    d$status <- "UNCLASSIFIED"
    datatable(d, rownames = FALSE,
              colnames = c("Case ID","Platform","Handle","Tweet Content","County","Timestamp","Status"),
              escape = FALSE,
              options = list(pageLength = 15, scrollX = TRUE, dom = "frtip",
                             columnDefs = list(list(width = "300px", targets = 3))))
  })
  
  # ── Classified table ──────────────────────────────────────
  output$classified_table <- renderDT({
    d <- rv$cases
    if (!is.null(input$clf_filter) && input$clf_filter != "All")
      d <- d[d$ml_label == input$clf_filter, ]
    d$ml_label_html <- sapply(d$ml_label, badge_html)
    d$validated_str <- ifelse(is.na(d$validated_by),
                              "<span style='color:#6c757d;font-size:11px;'>Pending</span>",
                              paste0("<span style='color:#198754;font-size:11px;'>✓ ", d$validated_by, "</span>"))
    disp <- d[, c("case_id","tweet_text","platform","county","ml_label_html",
                  "confidence","category","language","validated_str","timestamp")]
    datatable(disp, rownames = FALSE, escape = FALSE,
              colnames = c("Case ID","Tweet","Platform","County","Label",
                           "Confidence","Category","Language","Validated","Timestamp"),
              options = list(pageLength = 15, scrollX = TRUE, dom = "frtip",
                             columnDefs = list(list(width = "260px", targets = 1))))
  })
  
  # ── Validation UI ─────────────────────────────────────────
  output$validation_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui())
    
    pending <- rv$cases[is.na(rv$cases$validated_by) &
                          rv$cases$ml_label != "SAFE", ]
    
    if (nrow(pending) == 0)
      return(tags$div(
        style = "text-align:center;padding:60px;color:#6c757d;font-size:16px;",
        "✅ All cases have been validated."))
    
    email_configured <- nchar(Sys.getenv("GMAIL_USER")) > 0 &&
      nchar(Sys.getenv("GMAIL_PASS")) > 0 &&
      nchar(Sys.getenv("OFFICER_EMAIL")) > 0
    
    tagList(
      tags$div(
        style = "display:flex;justify-content:space-between;
                 align-items:center;margin-bottom:20px;",
        tags$div(
          tags$h5(style = "margin:0;",
                  tagList(bs_icon("shield-check"), " Officer Validation Panel")),
          tags$p(style = "font-size:12px;color:#6c757d;margin:4px 0 0;",
                 paste0(nrow(pending), " cases awaiting review"))
        ),
        tags$div(style = "display:flex;gap:8px;align-items:center;",
                 if (email_configured)
                   tags$span(
                     style = "font-size:11px;color:#198754;background:rgba(25,135,84,0.1);
                       border:1px solid rgba(25,135,84,0.3);border-radius:4px;padding:4px 10px;",
                     paste0("📧 Alerts → ", Sys.getenv("OFFICER_EMAIL")))
                 else
                   tags$span(
                     style = "font-size:11px;color:#fd7e14;background:rgba(253,126,20,0.1);
                       border:1px solid rgba(253,126,20,0.3);border-radius:4px;padding:4px 10px;",
                     "⚠ Email not configured"),
                 downloadButton("dl_validated", "⬇ Export (Excel)",
                                style = "background:#0066cc;color:#fff;border:none;
                     font-weight:600;border-radius:6px;")
        )
      ),
      
      layout_columns(
        col_widths = c(6,6),
        lapply(seq_len(min(6, nrow(pending))), function(i) {
          row <- pending[i, ]
          cid <- row$case_id
          lbl <- row$ml_label
          bdr <- switch(lbl,
                        "TOXIC"       = "#dc3545",
                        "HATE SPEECH" = "#fd7e14",
                        "#0066cc")
          
          card(
            style = paste0("border-top:3px solid ", bdr, ";"),
            card_header(
              tags$div(
                style = "display:flex;justify-content:space-between;align-items:center;",
                tags$span(
                  style = "font-family:'IBM Plex Mono';font-size:11px;color:#6c757d;",
                  cid),
                HTML(badge_html(lbl))
              )
            ),
            tags$div(class = "val-tweet-box",
                     style = paste0("border-left:3px solid ", bdr, ";"),
                     row$tweet_text),
            tags$div(style = "display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px;",
                     tags$span(class = "badge-pending", row$county),
                     tags$span(class = "badge-pending", row$platform),
                     tags$span(class = "badge-pending", row$language),
                     tags$span(class = "badge-pending", row$category),
                     tags$span(
                       style = "font-size:11px;color:#6c757d;
                         font-family:'IBM Plex Mono';align-self:center;",
                       row$confidence, " confidence")
            ),
            textAreaInput(paste0("note_", cid), "Officer note",
                          placeholder = "Add context or justification...",
                          rows = 2, width = "100%"),
            tags$div(style = "display:flex;gap:6px;margin-top:8px;flex-wrap:wrap;",
                     actionButton(paste0("confirm_", cid), "✓ Confirm Toxic",
                                  class = "btn-val-confirm"),
                     actionButton(paste0("dgrade_", cid),  "↓ Downgrade",
                                  class = "btn-val-downgrade"),
                     actionButton(paste0("clear_", cid),   "✓ Clear",
                                  class = "btn-val-clear"),
                     if (email_configured)
                       actionButton(paste0("email_", cid), "📧 Forward Alert",
                                    class = "btn-val-email")
            ),
            uiOutput(paste0("email_status_", cid))
          )
        })
      )
    )
  })
  
  # ── Validation + email handlers ───────────────────────────
  observe({
    pending <- rv$cases[is.na(rv$cases$validated_by) &
                          rv$cases$ml_label != "SAFE", ]
    for (i in seq_len(min(6, nrow(pending)))) {
      local({
        cid <- pending$case_id[i]
        
        observeEvent(input[[paste0("confirm_", cid)]], {
          rv$cases$validated_by[rv$cases$case_id == cid] <- "Officer (You)"
          rv$cases$action_taken[rv$cases$case_id == cid] <- "CONFIRMED TOXIC"
        }, ignoreInit = TRUE)
        
        observeEvent(input[[paste0("dgrade_", cid)]], {
          rv$cases$ml_label[rv$cases$case_id    == cid] <- "HATE SPEECH"
          rv$cases$validated_by[rv$cases$case_id == cid] <- "Officer (You)"
          rv$cases$action_taken[rv$cases$case_id == cid] <- "DOWNGRADED"
        }, ignoreInit = TRUE)
        
        observeEvent(input[[paste0("clear_", cid)]], {
          rv$cases$ml_label[rv$cases$case_id    == cid] <- "SAFE"
          rv$cases$validated_by[rv$cases$case_id == cid] <- "Officer (You)"
          rv$cases$action_taken[rv$cases$case_id == cid] <- "CLEARED"
        }, ignoreInit = TRUE)
        
        observeEvent(input[[paste0("email_", cid)]], {
          # Extract all fields as plain character scalars
          row <- rv$cases[rv$cases$case_id == cid, ]
          
          case_id_val    <- as.character(row$case_id[1])
          ml_label_val   <- as.character(row$ml_label[1])
          county_val     <- as.character(row$county[1])
          platform_val   <- as.character(row$platform[1])
          handle_val     <- as.character(row$handle[1])
          category_val   <- as.character(row$category[1])
          language_val   <- as.character(row$language[1])
          tweet_text_val <- as.character(row$tweet_text[1])
          timestamp_val  <- as.character(row$timestamp[1])
          note_val       <- as.character(isolate(input[[paste0("note_", cid)]]) %||% "")
          
          action_val <- row$action_taken[1]
          decision_val <- if (!is.na(action_val) && nchar(action_val) > 0)
            as.character(action_val) else as.character(row$ml_label[1])
          
          rv$email_status[[cid]] <- "sending"
          
          result <- tryCatch({
            send_alert_email(
              case_id    = case_id_val,
              ml_label   = ml_label_val,
              county     = county_val,
              platform   = platform_val,
              handle     = handle_val,
              category   = category_val,
              language   = language_val,
              tweet_text = tweet_text_val,
              timestamp  = timestamp_val,
              decision   = decision_val,
              note       = note_val
            )
            "sent"
          }, error = function(e) {
            message("Email error: ", conditionMessage(e))
            paste0("failed: ", conditionMessage(e))
          })
          
          rv$email_status[[cid]] <- result
        }, ignoreInit = TRUE)
      })
    }
  })
  
  # ── Download: validated ───────────────────────────────────
  output$dl_validated <- downloadHandler(
    filename = function() paste0("Kenya_Validated_", Sys.Date(), ".xlsx"),
    content  = function(file) {
      d <- rv$cases[!is.na(rv$cases$validated_by),
                    c("case_id","county","platform","tweet_text","ml_label",
                      "confidence","category","language","validated_by",
                      "action_taken","timestamp")]
      names(d) <- c("Case ID","County","Platform","Tweet","Label",
                    "Confidence","Category","Language","Validated By",
                    "Action Taken","Timestamp")
      write_xlsx(d, file)
    }
  )
  
  # ── Reports UI ────────────────────────────────────────────
  output$reports_ui <- renderUI({
    if (!rv$authenticated) return(auth_wall_ui())
    tagList(
      tags$h5(tagList(bs_icon("file-earmark-bar-graph"), " Intelligence Reports"),
              style = "margin-bottom:20px;"),
      layout_columns(
        col_widths = c(4,4,4),
        card(style = "border-top:3px solid #0066cc;",
             card_header("⚡ Quick Briefing"),
             tags$p(style = "font-size:12px;color:#6c757d;padding:0 4px;margin-bottom:14px;",
                    "24-hour snapshot: top incidents, severity counts, alerts by county."),
             downloadButton("dl_quick", "⬇ Download Excel",
                            style = "background:#0066cc;color:#fff;border:none;
                     border-radius:6px;font-weight:600;width:100%;")
        ),
        card(style = "border-top:3px solid #fd7e14;",
             card_header("📊 Detailed Report"),
             tags$p(style = "font-size:12px;color:#6c757d;padding:0 4px;margin-bottom:14px;",
                    "Full case-by-case breakdown with ML scores and validation status."),
             downloadButton("dl_detailed", "⬇ Download Excel",
                            style = "background:#fd7e14;color:#fff;border:none;
                     border-radius:6px;font-weight:600;width:100%;")
        ),
        card(style = "border-top:3px solid #198754;",
             card_header("🗺 County Report"),
             tags$p(style = "font-size:12px;color:#6c757d;padding:0 4px;margin-bottom:14px;",
                    "Per-county geo-distribution with hotspot and risk scoring."),
             downloadButton("dl_county", "⬇ Download Excel",
                            style = "background:#198754;color:#fff;border:none;
                     border-radius:6px;font-weight:600;width:100%;")
        )
      ),
      card(card_header("📋 Recent Exports"), DTOutput("recent_exports_tbl"))
    )
  })
  
  output$recent_exports_tbl <- renderDT({
    data.frame(
      Report         = c("Kenya_HS_QuickBrief_Mar19.xlsx",
                         "Kenya_HS_Detailed_Mar18.xlsx",
                         "Kenya_County_Report_Mar17.xlsx"),
      Type           = c("Quick","Detailed","County"),
      `Generated By` = c("Officer Mwangi","Officer Njeri","Officer Otieno"),
      Date           = c("2025-03-19 08:22","2025-03-18 16:40","2025-03-17 11:10"),
      Records        = c(47, 284, 47),
      check.names    = FALSE
    ) |> datatable(rownames = FALSE, options = list(dom = "t", pageLength = 10))
  })
  
  output$dl_quick <- downloadHandler(
    filename = function() paste0("Kenya_QuickBrief_", Sys.Date(), ".xlsx"),
    content  = function(file) {
      d <- rv$cases[rv$cases$ml_label %in% c("TOXIC","HATE SPEECH"),
                    c("case_id","county","platform","tweet_text",
                      "ml_label","confidence","category","timestamp")]
      names(d) <- c("Case ID","County","Platform","Tweet",
                    "Label","Confidence","Category","Timestamp")
      summary_d <- counties |>
        left_join(rv$cases |>
                    group_by(county) |>
                    summarise(Total = n(), Toxic = sum(ml_label == "TOXIC"),
                              Hate  = sum(ml_label == "HATE SPEECH"), .groups = "drop"),
                  by = c("name" = "county")) |>
        select(County = name, Lat = lat, Lng = lng, Total, Toxic, Hate)
      write_xlsx(list(Incidents = d, `County Summary` = summary_d), file)
    }
  )
  
  output$dl_detailed <- downloadHandler(
    filename = function() paste0("Kenya_Detailed_", Sys.Date(), ".xlsx"),
    content  = function(file) {
      d <- rv$cases[, c("case_id","county","platform","handle","tweet_text",
                        "ml_label","confidence","category","language",
                        "validated_by","action_taken","timestamp")]
      names(d) <- c("Case ID","County","Platform","Handle","Tweet","Label",
                    "Confidence","Category","Language","Validated By",
                    "Action Taken","Timestamp")
      write_xlsx(list(`All Cases` = d), file)
    }
  )
  
  output$dl_county <- downloadHandler(
    filename = function() paste0("Kenya_County_", Sys.Date(), ".xlsx"),
    content  = function(file) {
      d <- rv$cases |>
        group_by(county) |>
        summarise(Total = n(), Toxic = sum(ml_label == "TOXIC"),
                  Hate  = sum(ml_label == "HATE SPEECH"),
                  Safe  = sum(ml_label == "SAFE"),
                  Validated = sum(!is.na(validated_by)), .groups = "drop") |>
        left_join(counties, by = c("county" = "name")) |>
        mutate(Risk = case_when(
          Total > 100 ~ "HIGH", Total > 50 ~ "MEDIUM", TRUE ~ "LOW")) |>
        select(County = county, Lat = lat, Lng = lng,
               Total, Toxic, Hate, Safe, Validated, Risk)
      write_xlsx(list(`County Analysis` = d), file)
    }
  )
  
  # ── Auth ──────────────────────────────────────────────────
  observeEvent(input$auth_submit, {
    if (isTRUE(input$auth_password == OFFICER_PASSWORD)) {
      rv$authenticated <- TRUE
    } else {
      shinyjs::html("auth_error", "❌ Invalid code. Hint: intel2025")
    }
  })
  
} # end server

shinyApp(ui, server)