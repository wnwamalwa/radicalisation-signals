# ================================================================
#  R/server_misc.R — forecast, reports, S13 queue, audit log
#  Sourced inside server() in app.R
# ================================================================

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
    
    result <- db_check_password(un, pw)
    
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
          con <- db_connect(); on.exit(dbDisconnect(con))
          flagged_df <- tryCatch(dbGetQuery(con,
            "SELECT case_id,officer_a,level_a,officer_b,level_b,level_delta,ts FROM officer_agreement WHERE flagged=1 ORDER BY ts DESC LIMIT 20"),
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
