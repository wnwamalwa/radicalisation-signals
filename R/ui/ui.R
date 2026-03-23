# ================================================================
#  R/ui.R — Shiny UI Definition
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

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
                                                  href="#", onclick="$('[data-value=\"tab_s13\"]').tab('show');return false;",
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
                     
                     # ── Page header ──────────────────────────────────────────
                     tags$div(style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:12px;",
                              tags$div(
                                tags$h4(style="font-weight:800;color:#1a1a2e;margin:0 0 4px;font-size:16px;",
                                        tagList(bs_icon("cpu"), " AI Classification Engine")),
                                tags$p(style="font-size:12px;color:#6c757d;margin:0;",
                                       "GPT-4o-mini · NCIC Cap 170 · 3-stage decision chain · Async bulk processing")
                              ),
                              tags$div(style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;",
                                       tags$span(style="background:#7c3aed18;color:#7c3aed;border:1px solid #7c3aed44;border-radius:4px;padding:3px 10px;font-size:11px;font-weight:600;",
                                                 tagList(bs_icon("diagram-3"), " Violence Override → Target Test → Intent Test")),
                                       tags$a(tagList(bs_icon("arrow-right-circle"), " Methodology"),
                                              href="#", onclick="$('[data-value=\"tab_about\"]').tab('show');return false;",
                                              style="font-size:11px;color:#7c3aed;font-weight:600;text-decoration:none;white-space:nowrap;")
                              )
                     ),
                     
                     # ── Anti-hallucination bar ────────────────────────────────
                     tags$div(style="background:linear-gradient(90deg,#f0f9ff,#e8f4ff);border:1px solid #bae6fd;border-left:4px solid #0066cc;border-radius:8px;padding:10px 16px;margin-bottom:16px;display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;",
                              tags$div(style="display:flex;align-items:center;gap:10px;",
                                       tags$span(style="background:#0066cc;color:#fff;border-radius:50%;width:28px;height:28px;display:inline-flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0;",
                                                 tagList(bs_icon("shield-fill-check"))),
                                       tags$div(
                                         tags$div(style="font-size:12px;font-weight:700;color:#0c4a6e;margin-bottom:2px;",
                                                  "6-Layer Anti-Hallucination Stack Active"),
                                         tags$div(style="font-size:11px;color:#374151;",
                                                  "temperature=0  ·  JSON schema  ·  Cap 170 chain  ·  confidence scoring  ·  HITL validation  ·  disagreement retraining")
                                       )
                              ),
                              tags$a(tagList(bs_icon("book"), " Full safeguards →"),
                                     href="#", onclick="$('[data-value=\"tab_about\"]').tab('show');return false;",
                                     style="font-size:11px;color:#0066cc;font-weight:600;text-decoration:none;white-space:nowrap;flex-shrink:0;")
                     ),
                     
                     # ── KPIs + Bulk controls in one row ──────────────────────
                     layout_columns(col_widths=c(2,2,2,2,4),
                                    tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #0066cc;border-radius:8px;padding:12px 14px;",
                                             tags$div(style="font-size:10px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;font-weight:600;margin-bottom:4px;","Total Signals"),
                                             tags$div(style="font-size:28px;font-weight:800;color:#1a1a2e;line-height:1;", uiOutput("c_total", inline=TRUE)),
                                             tags$div(style="font-size:10px;color:#6c757d;margin-top:3px;","all ingested cases")
                                    ),
                                    tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #dc3545;border-radius:8px;padding:12px 14px;",
                                             tags$div(style="font-size:10px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;font-weight:600;margin-bottom:4px;","L4 + L5"),
                                             tags$div(style="font-size:28px;font-weight:800;color:#dc3545;line-height:1;", uiOutput("c_th", inline=TRUE)),
                                             tags$div(style="font-size:10px;color:#6c757d;margin-top:3px;","high severity")
                                    ),
                                    tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #198754;border-radius:8px;padding:12px 14px;",
                                             tags$div(style="font-size:10px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;font-weight:600;margin-bottom:4px;","Validated"),
                                             tags$div(style="font-size:28px;font-weight:800;color:#198754;line-height:1;", uiOutput("c_val", inline=TRUE)),
                                             tags$div(style="font-size:10px;color:#6c757d;margin-top:3px;","officer confirmed")
                                    ),
                                    tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #fd7e14;border-radius:8px;padding:12px 14px;",
                                             tags$div(style="font-size:10px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;font-weight:600;margin-bottom:4px;","Pending"),
                                             tags$div(style="font-size:28px;font-weight:800;color:#fd7e14;line-height:1;", uiOutput("c_pend", inline=TRUE)),
                                             tags$div(style="font-size:10px;color:#6c757d;margin-top:3px;","awaiting review")
                                    ),
                                    # Bulk controls in the 5th column
                                    tags$div(style="background:#fff;border:1px solid #dee2e6;border-top:3px solid #1a1a2e;border-radius:8px;padding:12px 14px;",
                                             tags$div(style="font-size:10px;color:#6c757d;text-transform:uppercase;letter-spacing:.05em;font-weight:600;margin-bottom:8px;",
                                                      tagList(bs_icon("arrow-repeat"), " Bulk Classification")),
                                             uiOutput("bulk_status_ui"),
                                             tags$div(style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;align-items:center;",
                                                      actionButton("btn_bulk",
                                                                   tagList(bs_icon("arrow-repeat"), " Run"),
                                                                   class="btn btn-primary btn-sm",
                                                                   style="font-size:11px;"),
                                                      actionButton("btn_retry_failed",
                                                                   tagList(bs_icon("exclamation-triangle"), " Retry"),
                                                                   class="btn btn-warning btn-sm",
                                                                   style="font-size:11px;"),
                                                      actionButton("btn_clear_cache",
                                                                   tagList(bs_icon("trash"), " Cache"),
                                                                   class="btn btn-outline-secondary btn-sm",
                                                                   style="font-size:11px;"),
                                                      uiOutput("cache_info")
                                             )
                                    )
                     ),
                     
                     # ── API failure strip (only shown when failures exist) ────
                     uiOutput("api_failure_ui"),
                     
                     # ── Classified cases table ────────────────────────────────
                     tags$div(style="margin-top:16px;",
                              card(style="border-top:3px solid #1a1a2e;",
                                   card_header(
                                     tags$div(style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;width:100%;",
                                              tags$div(style="font-size:13px;font-weight:700;color:#1a1a2e;",
                                                       tagList(bs_icon("table"), " Classified Cases")),
                                              tags$div(style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;",
                                                       selectInput("clf_ncic", NULL, width="200px",
                                                                   choices=c("All Levels", setNames(paste0("ncic_",0:5),
                                                                                                    paste0("L",0:5," ",NCIC_LEVELS))),
                                                                   selected="All Levels"),
                                                       selectInput("clf_risk", NULL, width="120px",
                                                                   choices=c("All Risk","HIGH","MEDIUM","LOW"),
                                                                   selected="All Risk"),
                                                       date_filter_ui("clf_dr", NULL)
                                              )
                                     )
                                   ),
                                   DTOutput("clf_table")
                              )
                     )
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
                     tags$div(style="background:#fff8e1;border:1px solid #ffc107;border-radius:6px;padding:7px 14px;margin-bottom:14px;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;",
                              tags$div(style="display:flex;align-items:center;gap:8px;font-size:12px;color:#664d03;",
                                       tags$span("⚠️"),
                                       tags$span(tags$strong("Handles are proxies, not confirmed identities."),
                                                 " Do not act on handle data alone without independent verification.")
                              ),
                              tags$a(tagList(bs_icon("book")," See responsible use principles"),
                                     href="#", onclick="$('[data-value=\"tab_interpretation\"]').tab('show');return false;",
                                     style="font-size:11px;color:#0066cc;font-weight:600;text-decoration:none;white-space:nowrap;flex-shrink:0;")
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
           
           # TAB 8: REPORTS
           nav_panel(title=tagList(bs_icon("file-earmark-bar-graph")," Reports"),value="tab_rep",padding=16,
                     uiOutput("rep_ui")),
           
           # TAB 9: S13 ESCALATION QUEUE
           nav_panel(title=tagList(bs_icon("exclamation-octagon")," S13 Escalation"),value="tab_s13",padding=16,
                     uiOutput("s13_ui")),
           
           # TAB 10: AUDIT LOG (admin only)
           nav_panel(title=tagList(bs_icon("clock-history")," Audit Log"),value="tab_audit",padding=16,
                     uiOutput("audit_ui")),
           
  ),
  
  # TAB 9: FORECAST
  nav_panel(title=tagList(bs_icon("graph-up-arrow")," Where is risk heading?"),value="tab_forecast",padding=16,
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
