# ======================
# UI.R
# ======================

page_navbar(
  title = div(
    style = "display: flex; align-items: center; padding: 0 5px;",
    icon("shield-alt", style = "margin-right: 8px; font-size: 1.1em;"),
    span("Kenya Early Warning System", style = "font-weight: 600; font-size: 0.9em; letter-spacing: 0.5px;")
  ),
  theme = super_theme,
  fillable = TRUE,

  header = tags$head(
    tags$style(HTML("
/* ===== NAVBAR STYLING ===== */
.navbar {
  padding: 0 40px !important;
  min-height: 56px !important;
  height: 56px !important;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  display: flex;
  align-items: center;
}

/* Title (left side) – perfectly centered vertically */
.navbar-brand {
  display: flex !important;
  align-items: center !important;
  height: 100% !important;
  padding: 0 !important;
  margin: 0 !important;
  font-size: 1.3em !important;
  font-weight: 600 !important;
  letter-spacing: 0.5px;
}
.navbar-brand > div {
  display: flex;
  align-items: center;
  gap: 8px;
}

/* Push all tabs to the right */
.navbar .navbar-nav {
  margin-left: auto !important;
}

/* Fine-tune tab appearance */
.navbar-nav .nav-link {
  padding: 16px 14px !important;
  font-size: 0.9em !important;
  font-weight: 500;
  letter-spacing: 0.2px;
  transition: all 0.2s ease;
}
.navbar-nav .nav-link:hover { background: rgba(255,255,255,0.1); }
.navbar-nav .nav-link.active { background: rgba(255,255,255,0.15) !important; }
.navbar-nav .nav-link i { margin-right: 6px; font-size: 0.9em; }

/* keep it on the far right with a little space */
.navbar .nav-item:last-child {
  margin-left: 20px;
}

      /* ===== FLOATING PANEL ===== */
      .floating-panel {
        position: absolute; top: 190px; left: 15px; z-index: 1000;
        width: 310px; padding: 0;
        background: rgba(255,255,255,0.98);
        border-radius: 10px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.12);
        backdrop-filter: blur(10px);
        max-height: calc(100vh - 90px);
        overflow: hidden; display: flex; flex-direction: column;
      }
      .panel-header {
        padding: 10px 14px; background: #2c3e50; color: white;
        border-radius: 10px 10px 0 0; cursor: grab; user-select: none;
      }
      .panel-header:active { cursor: grabbing; }
      .panel-header h5 { margin: 0; font-weight: 600; font-size: 1.5em; letter-spacing: 0.3px; }
      .panel-body { padding: 0; overflow-y: auto; flex: 1; }
      .panel-footer { padding: 10px 14px; border-top: 1px solid #ecf0f1; background: #fafbfc; }

      /* ===== ACCORDION STYLING ===== */
      .accordion { --bs-accordion-border-radius: 0; --bs-accordion-border-width: 0; }
      .accordion-item { border: none; border-bottom: 1px solid #ecf0f1; }
      .accordion-item:last-child { border-bottom: none; }
      .accordion-button {
        padding: 10px 14px !important;
        font-size: 0.4em !important;
        font-weight: 600 !important;
        color: #2c3e50 !important;
        background: #fafbfc !important;
        letter-spacing: 0.3px;
        text-transform: uppercase;
      }
      .accordion-button:not(.collapsed) {
        color: #3498db !important;
        background: #f0f7ff !important;
        box-shadow: none !important;
      }
      .accordion-button:focus { box-shadow: none !important; }
      .accordion-button::after {
        width: 12px; height: 12px;
        background-size: 12px;
      }
      .accordion-button i { margin-right: 8px; font-size: 0.95em; }
      .accordion-body { padding: 12px 14px !important; background: white; }

      /* ===== FILTER STYLING ===== */
      .filter-section { margin-bottom: 10px; }
      .filter-section:last-child { margin-bottom: 0; }
      .filter-section label {
        font-weight: 500; color: #5d6d7e; font-size: 0.75em;
        margin-bottom: 4px; display: block;
      }
      .filter-section .form-control,
      .filter-section .form-select {
        font-size: 0.8em !important;
        padding: 6px 10px !important;
        border-radius: 6px !important;
      }

      /* ===== CHECKBOX STYLING ===== */
      .checkbox-inline, .shiny-input-container .checkbox-inline {
        font-size: 0.72em !important;
        margin-right: 6px !important;
        padding: 3px 8px !important;
        background: #f8f9fa;
        border-radius: 4px;
        margin-bottom: 4px !important;
      }

      /* ===== LEGEND ===== */
      .legend-item { display: flex; align-items: center; margin-bottom: 3px; font-size: 0.72em; color: #5d6d7e; }
      .legend-dot { width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; flex-shrink: 0; }

      /* ===== STAT CARDS ===== */
      .stat-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white; border-radius: 12px; padding: 18px 20px;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
      }
      .stat-card:hover { transform: translateY(-2px); box-shadow: 0 8px 25px rgba(0,0,0,0.15); }
      .stat-card.danger { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }
      .stat-card.success { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }
      .stat-card.warning { background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); }
      .stat-value { font-size: 1.9em; font-weight: 700; line-height: 1.2; }
      .stat-label { font-size: 0.78em; opacity: 0.9; margin-top: 4px; letter-spacing: 0.3px; }

      /* ===== CARDS ===== */
      .card-custom {
        border: none; border-radius: 12px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        transition: box-shadow 0.2s ease;
      }
      .card-custom:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.1); }
      .card-custom .card-header {
        background: white; border-bottom: 1px solid #f1f3f4;
        font-weight: 600; font-size: 0.88em; padding: 14px 18px;
        letter-spacing: 0.2px; color: #2c3e50;
      }
      .card-custom .card-body { padding: 16px 18px; }

      /* ===== PAGE CONTENT ===== */
      .tab-content { background: #f8f9fa; }
      .container-fluid { padding: 20px 24px !important; }

      /* ===== BUTTONS ===== */
      .btn { font-size: 0.8em !important; letter-spacing: 0.2px; border-radius: 6px !important; }
      .btn-sm { padding: 6px 12px !important; }

      /* ===== SLIDER ===== */
      .irs--shiny .irs-bar { background: #3498db; }
      .irs--shiny .irs-handle { border-color: #3498db; }
      .irs--shiny .irs-single { background: #3498db; font-size: 0.7em; }
      .irs { font-size: 0.75em; }

      /* ===== TABLE ===== */
      .dataTables_wrapper { font-size: 0.85em; }
      .dataTables_filter input { border-radius: 6px !important; padding: 6px 12px !important; }

      /* ===== LAST UPDATE ===== */
      .last-update-text { font-size: 0.75em; color: #95a5a6; padding: 10px 16px; letter-spacing: 0.2px; }

      /* ===== LEAFLET ===== */
      .leaflet-container { background: #f8f9fa; }
      .leaflet-control-layers { font-size: 0.8em; border-radius: 8px !important; }

      /* ===== SCROLLBAR ===== */
      .panel-body::-webkit-scrollbar { width: 5px; }
      .panel-body::-webkit-scrollbar-track { background: #f1f1f1; border-radius: 10px; }
      .panel-body::-webkit-scrollbar-thumb { background: #ccc; border-radius: 10px; }
      .panel-body::-webkit-scrollbar-thumb:hover { background: #aaa; }

      /* ===== HR ===== */
      hr { margin: 10px 0; border-color: #ecf0f1; opacity: 0.6; }

      /* ===== MARKER BADGE ===== */
      .marker-badge {
        display: inline-flex; align-items: center; justify-content: center;
        background: #3498db; color: white; font-size: 0.7em;
        padding: 2px 8px; border-radius: 10px; font-weight: 600;
      }
    "))
  ),

  # ==================== MAP TAB ====================
  nav_panel(
    title = "Map",
    icon = icon("map-marked-alt"),
    padding = 0,

    tags$script(HTML("
      $(document).ready(function() {
        var panel = $('.floating-panel');
        var header = $('.panel-header');
        var isDragging = false, offsetX, offsetY;
        header.on('mousedown', function(e) {
          isDragging = true;
          offsetX = e.clientX - panel.offset().left;
          offsetY = e.clientY - panel.offset().top;
          panel.css('transition', 'none');
        });
        $(document).on('mousemove', function(e) {
          if (isDragging) {
            var newX = Math.max(0, Math.min(e.clientX - offsetX, $(window).width() - panel.outerWidth()));
            var newY = Math.max(0, Math.min(e.clientY - offsetY, $(window).height() - panel.outerHeight()));
            panel.css({ left: newX + 'px', top: newY + 'px' });
          }
        });
        $(document).on('mouseup', function() { isDragging = false; panel.css('transition', ''); });
      });
    ")),

    leafletOutput("map", width = "100%", height = "100%"),

    div(class = "floating-panel",

      # Panel Header (Draggable)
      div(class = "panel-header",
        div(style = "display: flex; justify-content: space-between; align-items: center;",
          h5(icon("sliders-h"), " Filters"),
          span(class = "marker-badge", textOutput("marker_count", inline = TRUE), " results")
        )
      ),

      # Panel Body with Accordion
      div(class = "panel-body",

        tags$div(class = "accordion", id = "filterAccordion",

          # Location & Time Accordion
          tags$div(class = "accordion-item",
            tags$h2(class = "accordion-header",
              tags$button(class = "accordion-button", type = "button",
                `data-bs-toggle` = "collapse", `data-bs-target` = "#locationTime",
                `aria-expanded` = "true", `aria-controls` = "locationTime",
                icon("map-marker-alt"), "Location & Time"
              )
            ),
            tags$div(id = "locationTime", class = "accordion-collapse collapse show",
              `data-bs-parent` = "#filterAccordion",
              tags$div(class = "accordion-body",
                div(class = "filter-section",
                  tags$label("County"),
                  selectInput("region_filter", NULL,
                    c("All Counties" = "All", setNames(unique(kenya_cases$county), unique(kenya_cases$county))),
                    "All", width = "100%")
                ),
                div(class = "filter-section",
                  tags$label("Time Period"),
                  selectInput("time_filter", NULL, c("All Time", time_periods), "All Time", width = "100%")
                )
              )
            )
          ),

          # Narrative Type Accordion
          tags$div(class = "accordion-item",
            tags$h2(class = "accordion-header",
              tags$button(class = "accordion-button collapsed", type = "button",
                `data-bs-toggle` = "collapse", `data-bs-target` = "#narrativeType",
                `aria-expanded` = "false", `aria-controls` = "narrativeType",
                icon("comment-alt"), "Narrative Type"
              )
            ),
            tags$div(id = "narrativeType", class = "accordion-collapse collapse",
              `data-bs-parent` = "#filterAccordion",
              tags$div(class = "accordion-body",
                div(class = "filter-section",
                  tags$label("Type"),
                  selectInput("narrative_filter", NULL,
                    c("All Types" = "All", setNames(narrative_types, narrative_types)),
                    "All", width = "100%")
                ),
                div(class = "filter-section",
                  tags$label("Sentiment"),
                  selectInput("sentiment_filter", NULL,
                    c("All Sentiments" = "All", setNames(sentiment_types, sentiment_types)),
                    "All", width = "100%")
                )
              )
            )
          ),

          # Toxicity Level Accordion
          tags$div(class = "accordion-item",
            tags$h2(class = "accordion-header",
              tags$button(class = "accordion-button collapsed", type = "button",
                `data-bs-toggle` = "collapse", `data-bs-target` = "#toxicityLevel",
                `aria-expanded` = "false", `aria-controls` = "toxicityLevel",
                icon("thermometer-half"), "Toxicity Level"
              )
            ),
            tags$div(id = "toxicityLevel", class = "accordion-collapse collapse",
              `data-bs-parent` = "#filterAccordion",
              tags$div(class = "accordion-body",
                div(class = "filter-section",
                  tags$label("Toxicity Score Range"),
                  sliderInput("toxicity_filter", NULL, 0, 100, c(0, 100), 5, width = "100%")
                ),
                div(class = "filter-section",
                  tags$label("Minimum Cases"),
                  sliderInput("case_filter", NULL, 0, 150, 0, 5, width = "100%")
                )
              )
            )
          ),

          # Data Sources Accordion
          tags$div(class = "accordion-item",
            tags$h2(class = "accordion-header",
              tags$button(class = "accordion-button collapsed", type = "button",
                `data-bs-toggle` = "collapse", `data-bs-target` = "#dataSources",
                `aria-expanded` = "false", `aria-controls` = "dataSources",
                icon("database"), "Data Sources"
              )
            ),
            tags$div(id = "dataSources", class = "accordion-collapse collapse",
              `data-bs-parent` = "#filterAccordion",
              tags$div(class = "accordion-body",
                div(class = "filter-section",
                  checkboxGroupInput("source_filter", NULL, data_sources, data_sources, inline = TRUE)
                )
              )
            )
          ),

          # Alert & Target Accordion
          tags$div(class = "accordion-item",
            tags$h2(class = "accordion-header",
              tags$button(class = "accordion-button collapsed", type = "button",
                `data-bs-toggle` = "collapse", `data-bs-target` = "#alertTarget",
                `aria-expanded` = "false", `aria-controls` = "alertTarget",
                icon("exclamation-circle"), "Alert & Target"
              )
            ),
            tags$div(id = "alertTarget", class = "accordion-collapse collapse",
              `data-bs-parent` = "#filterAccordion",
              tags$div(class = "accordion-body",
                div(class = "filter-section",
                  tags$label("Alert Status"),
                  selectInput("alert_filter", NULL,
                    c("All Statuses" = "All", setNames(alert_statuses[-1], alert_statuses[-1])),
                    "All", width = "100%")
                ),
                div(class = "filter-section",
                  tags$label("Target Group"),
                  checkboxGroupInput("target_filter", NULL, target_groups, target_groups, inline = TRUE)
                )
              )
            )
          ),

          # Legend Accordion
          tags$div(class = "accordion-item",
            tags$h2(class = "accordion-header",
              tags$button(class = "accordion-button collapsed", type = "button",
                `data-bs-toggle` = "collapse", `data-bs-target` = "#legendSection",
                `aria-expanded` = "false", `aria-controls` = "legendSection",
                icon("info-circle"), "Legend"
              )
            ),
            tags$div(id = "legendSection", class = "accordion-collapse collapse",
              `data-bs-parent` = "#filterAccordion",
              tags$div(class = "accordion-body",
                div(class = "legend-item", div(class = "legend-dot", style = "background:#27ae60;"), "Low Risk (< 50 cases)"),
                div(class = "legend-item", div(class = "legend-dot", style = "background:#f39c12;"), "Medium Risk (50-100 cases)"),
                div(class = "legend-item", div(class = "legend-dot", style = "background:#e74c3c;"), "High Risk (> 100 cases)")
              )
            )
          )
        )
      ),

      # Panel Footer with Buttons
      div(class = "panel-footer",
        div(style = "display: flex; gap: 8px;",
          actionButton("reset_filters", "Reset", icon = icon("redo"),
            class = "btn-outline-secondary btn-sm", style = "flex: 1;")
        )
      )
    )
  ),
# ==================== ABOUT TAB ====================
nav_panel(
  title = "About",
  icon = icon("info-circle"),

  div(style = "padding: 30px 40px; max-width: 1100px; margin: 0 auto;",

      # Main Title
      h2(style = "text-align: center; color: #2c3e50; font-weight: 700; margin-bottom: 30px;",
         icon("shield-alt", style = "margin-right: 12px; color: #3498db;"),
         "Kenya Early Warning System"),

      # Two-column layout: Summary + How to Use
      div(style = "display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-top: 20px;",

          # Left Column – Project Summary
          card(class = "card-custom",
               card_header(icon("book-open", style = "margin-right: 8px;"),
                           "Project Summary"),
               card_body(
                 p(style = "font-size: 0.95em; line-height: 1.7; color: #2c3e50;",
                   "The ", tags$strong("Kenya Early Warning System"), " is an ",
                   tags$strong("AI-powered, semi-agentic dashboard"),
                   " designed to detect early signals of harmful narratives, hate speech, misinformation, and radicalisation risks across Kenya — using only publicly available data."
                 ),
                 tags$ul(style = "font-size: 0.92em; line-height: 1.8; color: #34495e; margin: 16px 0;",
                   tags$li("Real-time monitoring of public social media, news comments, and forums"),
                   tags$li("Automated detection of sentiment shifts, toxicity spikes, and narrative trends"),
                   tags$li("County-level heatmaps and risk alerts"),
                   tags$li("Human-in-the-loop validation – no automated decisions"),
                   tags$li("Strict privacy compliance: no individual tracking or personal data")
                 ),
                 p(style = "font-size: 0.92em; color: #2c3e50; margin-top: 18px;",
                   "Developed for the ", tags$strong("National Counter Terrorism Centre (NCTC)"),
                   ", county CVE committees, civil society, journalists, and peacebuilding organisations."
                 ),
                 tags$div(style = "margin-top: 20px; padding: 14px; background: #f0f7ff; border-left: 4px solid #3498db; border-radius: 0 8px 8px 0;",
                   tags$i(style = "font-size: 0.9em; color: #2980b9;",
                     "This is a digital early warning observatory — not a surveillance tool.")
                 )
               )
          ),

          # Right Column – How to Use
          card(class = "card-custom",
               card_header(icon("question-circle", style = "margin-right: 8px;"),
                           "How to Use This Dashboard"),
               card_body(
                 tags$ol(style = "font-size: 0.92em; line-height: 1.9; color: #2c3e50; padding-left: 20px;",
                   tags$li(tags$strong("Map Tab"), " → Interactive county heat map. Use the floating filter panel to narrow by county, time, narrative type, toxicity, or source."),
                   tags$li(tags$strong("Trends Tab"), " → View national statistics, timelines, sentiment distribution, and top counties."),
                   tags$li(tags$strong("Alerts Tab"), " → Full table of flagged content clusters with export options (CSV/Excel)."),
                   tags$li(tags$strong("Reports Tab"), " → Generate downloadable HTML/PDF briefings for stakeholders."),
                   tags$li(tags$strong("Settings Tab"), " → Adjust thresholds, map style, refresh rate, and enable email alerts.")
                 ),
                 hr(),
                 tags$h6(style = "color: #2c3e50; margin: 16px 0 10px;", "Quick Tips"),
                 tags$ul(style = "font-size: 0.88em; color: #555; line-height: 1.7;",
                   tags$li("Drag the floating filter panel anywhere on the map"),
                   tags$li("Click any county on the map for a detailed popup"),
                   tags$li("Use “Reset” and “Apply” buttons in the filter panel after changing criteria"),
                   tags$li("High-risk counties appear in red (>100 cases), medium in orange, low in green")
                 )
               )
          )
      ),

      # Footer note
      div(style = "text-align: center; margin-top: 40px; padding: 20px; color: #7f8c8d; font-size: 0.9em;",
          "© 2025 Kenya Early Warning System • Ethical AI for Peacebuilding & Social Cohesion"
      )
  )
),
  # ==================== TRENDS TAB ====================
  nav_panel(
    title = "Trends",
    icon = icon("chart-line"),

    div(style = "padding: 20px 24px;",
      div(style = "display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px;",
        div(class = "stat-card",
          div(class = "stat-value", textOutput("total_cases_stat", inline = TRUE)),
          div(class = "stat-label", icon("chart-bar"), " Total Cases")
        ),
        div(class = "stat-card danger",
          div(class = "stat-value", textOutput("high_risk_stat", inline = TRUE)),
          div(class = "stat-label", icon("exclamation-triangle"), " High Risk Alerts")
        ),
        div(class = "stat-card success",
          div(class = "stat-value", textOutput("validated_stat", inline = TRUE)),
          div(class = "stat-label", icon("check-circle"), " Validated")
        ),
        div(class = "stat-card warning",
          div(class = "stat-value", textOutput("avg_toxicity_stat", inline = TRUE)),
          div(class = "stat-label", icon("skull-crossbones"), " Avg Toxicity")
        )
      ),

      div(style = "display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-bottom: 16px;",
        card(class = "card-custom",
          card_header(icon("chart-area", style = "margin-right: 8px;"), "Cases Over Time"),
          card_body(plotlyOutput("cases_timeline", height = "280px"))
        ),
        card(class = "card-custom",
          card_header(icon("smile", style = "margin-right: 8px;"), "Sentiment Distribution"),
          card_body(plotlyOutput("sentiment_pie", height = "280px"))
        )
      ),

      div(style = "display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px;",
        card(class = "card-custom",
          card_header(icon("comments", style = "margin-right: 8px;"), "Narrative Types Trend"),
          card_body(plotlyOutput("narrative_trend", height = "280px"))
        ),
        card(class = "card-custom",
          card_header(icon("map", style = "margin-right: 8px;"), "Top Counties by Cases"),
          card_body(plotlyOutput("county_bar", height = "280px"))
        )
      ),

      div(style = "display: grid; grid-template-columns: 1fr 1fr; gap: 16px;",
        card(class = "card-custom",
          card_header(icon("biohazard", style = "margin-right: 8px;"), "Toxicity by Data Source"),
          card_body(plotlyOutput("source_toxicity", height = "260px"))
        ),
        card(class = "card-custom",
          card_header(icon("users", style = "margin-right: 8px;"), "Target Groups Distribution"),
          card_body(plotlyOutput("target_pie", height = "260px"))
        )
      )
    )
  ),

  # ==================== ALERTS TABLE TAB ====================
  nav_panel(
    title = "Alerts",
    icon = icon("bell"),

    div(style = "padding: 20px 24px;",
      card(class = "card-custom",
        card_header(
          div(style = "display: flex; justify-content: space-between; align-items: center;",
            span(icon("table", style = "margin-right: 8px;"), "All Alerts"),
            div(
              downloadButton("download_csv", "Export CSV", class = "btn-sm btn-outline-primary", style = "margin-right: 8px;"),
              downloadButton("download_excel", "Export Excel", class = "btn-sm btn-outline-success")
            )
          )
        ),
        card_body(DTOutput("alerts_table"))
      )
    )
  ),

  # ==================== REPORTS TAB ====================
  nav_panel(
    title = "Reports",
    icon = icon("file-alt"),

    div(style = "padding: 20px 24px;",
      div(style = "display: grid; grid-template-columns: 350px 1fr; gap: 20px;",

        card(class = "card-custom",
          card_header(icon("cog", style = "margin-right: 8px;"), "Report Configuration"),
          card_body(
            tags$label(style = "font-size: 0.82em; font-weight: 500; color: #2c3e50;", "Report Type"),
            selectInput("report_type", NULL,
                        c("Executive Summary", "Detailed Analysis", "County Breakdown", "Trend Report"),
                        width = "100%"),

            tags$label(style = "font-size: 0.82em; font-weight: 500; color: #2c3e50; margin-top: 10px;", "Date Range"),
            dateRangeInput("report_dates", NULL,
                           start = Sys.Date() - 30, end = Sys.Date(), width = "100%"),

            tags$label(style = "font-size: 0.82em; font-weight: 500; color: #2c3e50; margin-top: 10px;", "County Filter"),
            selectInput("report_county", NULL,
                        c("All Counties", unique(kenya_cases$county)), width = "100%"),

            tags$label(style = "font-size: 0.82em; font-weight: 500; color: #2c3e50; margin-top: 10px;", "Include Sections"),
            checkboxGroupInput("report_sections", NULL,
                               c("Summary Statistics", "Trend Charts", "Top Alerts",
                                 "County Breakdown", "Recommendations"),
                               selected = c("Summary Statistics", "Trend Charts", "Top Alerts")),
            hr(),
            div(style = "display: flex; flex-direction: column; gap: 10px;",
              downloadButton("download_html", "Generate HTML Report", class = "btn-primary", style = "width: 100%;"),
              downloadButton("download_pdf", "Generate PDF Report", class = "btn-outline-secondary", style = "width: 100%;")
            )
          )
        ),

        card(class = "card-custom",
          card_header(icon("eye", style = "margin-right: 8px;"), "Report Preview"),
          card_body(style = "background: #fafbfc; min-height: 500px;",
            uiOutput("report_preview")
          )
        )
      )
    )
  ),

  # ==================== SETTINGS TAB ====================
  nav_panel(
    title = "Settings",
    icon = icon("cog"),

    div(style = "padding: 20px 24px;",
      div(style = "display: grid; grid-template-columns: 1fr 1fr; gap: 20px;",

        card(class = "card-custom",
          card_header(icon("bell", style = "margin-right: 8px;"), "Alert Thresholds"),
          card_body(
            tags$label(style = "font-size: 0.82em; font-weight: 500;", "Low Risk Threshold"),
            sliderInput("threshold_low", NULL, 0, 100, 50, width = "100%"),
            tags$label(style = "font-size: 0.82em; font-weight: 500; margin-top: 8px;", "High Risk Threshold"),
            sliderInput("threshold_high", NULL, 50, 150, 100, width = "100%"),
            tags$label(style = "font-size: 0.82em; font-weight: 500; margin-top: 8px;", "Toxicity Alert Level"),
            sliderInput("toxicity_alert", NULL, 0, 100, 70, width = "100%"),
            hr(),
            checkboxInput("email_alerts", "Enable Email Alerts", FALSE),
            conditionalPanel(
              condition = "input.email_alerts == true",
              textInput("alert_email", NULL, placeholder = "your@email.com", width = "100%")
            )
          )
        ),

        card(class = "card-custom",
          card_header(icon("palette", style = "margin-right: 8px;"), "Display Preferences"),
          card_body(
            tags$label(style = "font-size: 0.82em; font-weight: 500;", "Default Map Style"),
            selectInput("default_map_style", NULL,
                        c("Light (Positron)", "Dark (Dark Matter)", "Streets (OSM)", "Satellite"),
                        width = "100%"),
            tags$label(style = "font-size: 0.82em; font-weight: 500; margin-top: 8px;", "Chart Color Theme"),
            selectInput("chart_theme", NULL,
                        c("Default", "Viridis", "Plasma", "Blues", "Reds"),
                        width = "100%"),
            tags$label(style = "font-size: 0.82em; font-weight: 500; margin-top: 8px;", "Table Rows Per Page"),
            sliderInput("items_per_page", NULL, 10, 100, 25, 5, width = "100%"),
            hr(),
            checkboxInput("show_tooltips", "Show Help Tooltips", TRUE),
            checkboxInput("compact_mode", "Compact Mode", FALSE)
          )
        ),

        card(class = "card-custom",
          card_header(icon("database", style = "margin-right: 8px;"), "Data Settings"),
          card_body(
            tags$label(style = "font-size: 0.82em; font-weight: 500;", "Auto-Refresh Interval"),
            selectInput("refresh_interval", NULL,
                        c("Off", "1 minute", "5 minutes", "15 minutes", "30 minutes"),
                        width = "100%"),
            tags$label(style = "font-size: 0.82em; font-weight: 500; margin-top: 8px;", "Default Time Range"),
            selectInput("default_time_range", NULL,
                        time_periods, selected = "Last 7 days", width = "100%"),
            hr(),
            div(style = "display: flex; flex-direction: column; gap: 10px;",
              actionButton("clear_cache", "Clear Cache", icon = icon("trash"), class = "btn-outline-warning", style = "width: 100%;"),
              actionButton("save_settings", "Save Settings", icon = icon("save"), class = "btn-primary", style = "width: 100%;")
            )
          )
        ),

        card(class = "card-custom",
          card_header(icon("info-circle", style = "margin-right: 8px;"), "About"),
          card_body(
            div(style = "text-align: center; padding: 10px 0;",
              icon("shield-alt", style = "font-size: 2.5em; color: #2c3e50; margin-bottom: 12px;"),
              h5(style = "margin: 0 0 5px 0; color: #2c3e50;", "Kenya Early Warning System"),
              p(style = "font-size: 0.8em; color: #7f8c8d; margin: 0;", "Version 1.0.0")
            ),
            hr(),
            p(style = "font-size: 0.82em; color: #5d6d7e; line-height: 1.6;",
              "An AI-powered semi-agentic early warning system for detecting harmful narratives and radicalization signals across Kenya."),
            hr(),
            div(style = "font-size: 0.78em; color: #7f8c8d;",
              p(style = "margin-bottom: 4px;", tags$strong("Developed for:")),
              tags$ul(style = "margin: 0; padding-left: 18px;",
                tags$li("National Counter Terrorism Centre"),
                tags$li("County CVE Committees"),
                tags$li("Civil Society Organizations")
              )
            ),
            hr(),
            p(style = "font-size: 0.72em; color: #95a5a6; text-align: center; margin: 0;",
              "© 2025 All rights reserved")
          )
        )
      )
    )
  ),

  nav_spacer(),

  nav_item(
    div(class = "last-update-text",
      icon("clock", style = "margin-right: 5px;"),
      "Last updated: ", textOutput("last_update", inline = TRUE)
    )
  )
)
