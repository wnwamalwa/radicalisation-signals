# ================================================================
#  R/server_init.R — rv init, session, dashboard, classify, bulk, network
#  Sourced inside server() in app.R
# ================================================================

  rv <- reactiveValues(
    authenticated  = FALSE,
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
  
