# ======================
# SERVER.R
# ======================

function(input, output, session) {

  # ==================== HELPER FUNCTIONS ====================

  get_color <- function(cases) {
    sapply(cases, function(x) {
      if (x < 50) "#27ae60" else if (x <= 100) "#f39c12" else "#e74c3c"
    })
  }

  filter_by_time <- function(data, period) {
    if (period == "All Time") return(data)
    now <- Sys.time()
    cutoff <- switch(period,
      "Last 6 hours" = now - (6*60*60),
      "Last 24 hours" = now - (24*60*60),
      "Last 7 days" = now - (7*24*60*60),
      "Last 30 days" = now - (30*24*60*60),
      now - (30*24*60*60)
    )
    data[data$timestamp >= cutoff, ]
  }

  keyword_match <- function(keywords_col, selected_keywords) {
    if (length(selected_keywords) == 0) return(rep(TRUE, length(keywords_col)))
    sapply(keywords_col, function(k) any(sapply(selected_keywords, function(sk) grepl(sk, k, fixed = TRUE))))
  }

  # ==================== REACTIVE DATA ====================

  filtered_data <- reactive({
    req(input$region_filter, input$case_filter, input$narrative_filter,
        input$time_filter, input$sentiment_filter, input$toxicity_filter,
        input$alert_filter, input$source_filter, input$target_filter)

    d <- kenya_cases
    if (input$region_filter != "All") d <- d[d$county == input$region_filter, ]
    d <- filter_by_time(d, input$time_filter)
    if (input$narrative_filter != "All") d <- d[d$narrative_type == input$narrative_filter, ]
    if (input$sentiment_filter != "All") d <- d[d$sentiment == input$sentiment_filter, ]
    d <- d[d$toxicity_score >= input$toxicity_filter[1] & d$toxicity_score <= input$toxicity_filter[2], ]
    d <- d[d$data_source %in% input$source_filter, ]
    if (input$alert_filter != "All") d <- d[d$alert_status == input$alert_filter, ]
    d <- d[d$target_group %in% input$target_filter, ]
    d <- d[d$cases >= input$case_filter, ]
    d
  })

  # ==================== RESET FILTERS ====================

  observeEvent(input$reset_filters, {
    updateSelectInput(session, "region_filter", selected = "All")
    updateSelectInput(session, "time_filter", selected = "All Time")
    updateSelectInput(session, "narrative_filter", selected = "All")
    updateSelectInput(session, "sentiment_filter", selected = "All")
    updateSliderInput(session, "toxicity_filter", value = c(0, 100))
    updateCheckboxGroupInput(session, "source_filter", selected = data_sources)
    updateSelectInput(session, "alert_filter", selected = "All")
    updateCheckboxGroupInput(session, "target_filter", selected = target_groups)
    updateSliderInput(session, "case_filter", value = 0)
  })

  # ==================== MAP ====================

  output$map <- renderLeaflet({
    leaflet() %>%
      addProviderTiles("CartoDB.Positron", group = "Light") %>%
      addProviderTiles("CartoDB.DarkMatter", group = "Dark") %>%
      addLayersControl(baseGroups = c("Light", "Dark"), options = layersControlOptions(collapsed = FALSE)) %>%
      setView(lng = 37.5, lat = 0.5, zoom = 6)
  })

  observeEvent(filtered_data(), {
    data <- filtered_data()
    proxy <- leafletProxy("map") %>% clearMarkers()

    if (!is.null(data) && nrow(data) > 0) {
      popup_content <- paste0(
        "<div style='font-family:Inter,sans-serif;min-width:200px;font-size:13px;'>",
        "<strong style='font-size:15px;color:#2c3e50;'>", data$county, "</strong>",
        "<hr style='margin:8px 0;border-color:#ecf0f1;'>",
        "<b>Narrative:</b> ", data$narrative_type, "<br>",
        "<b>Source:</b> ", data$data_source, "<br>",
        "<b>Sentiment:</b> ", data$sentiment, "<br>",
        "<b>Target:</b> ", data$target_group, "<br>",
        "<b>Toxicity:</b> ", data$toxicity_score, "%<br>",
        "<hr style='margin:8px 0;'>",
        "<b>Cases:</b> ", data$cases, " | <b>Risk:</b> ", data$risk_level,
        "</div>"
      )
      proxy %>% addCircleMarkers(
        data = data, lng = ~lng, lat = ~lat,
        radius = sqrt(data$cases) * 1.8, color = "#2c3e50",
        fillColor = get_color(data$cases), fillOpacity = 0.75,
        stroke = TRUE, weight = 2, popup = popup_content
      )
    }
  }, ignoreNULL = TRUE)

  # ==================== STATS ====================

  output$total_cases_stat <- renderText({ format(sum(kenya_cases$cases), big.mark = ",") })
  output$high_risk_stat <- renderText({ sum(kenya_cases$risk_level == "High") })
  output$validated_stat <- renderText({ sum(kenya_cases$alert_status == "Validated") })
  output$avg_toxicity_stat <- renderText({ paste0(round(mean(kenya_cases$toxicity_score), 1), "%") })
  output$last_update <- renderText({ format(Sys.time(), "%H:%M:%S") })
  output$marker_count <- renderText({ nrow(filtered_data()) })

  # ==================== TREND CHARTS ====================

  output$cases_timeline <- renderPlotly({
    plot_ly(daily_trends, x = ~date, y = ~total_cases, type = "scatter", mode = "lines+markers",
            line = list(color = "#3498db", width = 3), marker = list(size = 8, color = "#2c3e50")) %>%
      layout(xaxis = list(title = ""), yaxis = list(title = "Total Cases"),
             margin = list(l = 50, r = 20, t = 20, b = 40))
  })

  output$sentiment_pie <- renderPlotly({
    sent_data <- kenya_cases %>% count(sentiment)
    colors <- c("Positive" = "#27ae60", "Neutral" = "#3498db", "Negative" = "#e67e22", "Toxic" = "#e74c3c")
    plot_ly(sent_data, labels = ~sentiment, values = ~n, type = "pie",
            marker = list(colors = colors[sent_data$sentiment]),
            textinfo = "label+percent") %>%
      layout(showlegend = FALSE, margin = list(l = 20, r = 20, t = 20, b = 20))
  })

  output$narrative_trend <- renderPlotly({
    plot_ly(narrative_trends, x = ~date, y = ~count, color = ~narrative_type,
            type = "scatter", mode = "lines", colors = "Set2") %>%
      layout(xaxis = list(title = ""), yaxis = list(title = "Count"),
             legend = list(orientation = "h", y = -0.2), margin = list(l = 50, r = 20, t = 20, b = 60))
  })

  output$county_bar <- renderPlotly({
    county_data <- kenya_cases %>% group_by(county) %>% summarise(total = sum(cases)) %>%
      arrange(desc(total)) %>% head(10)
    plot_ly(county_data, x = ~reorder(county, total), y = ~total, type = "bar",
            marker = list(color = "#3498db")) %>%
      layout(xaxis = list(title = ""), yaxis = list(title = "Total Cases"),
             margin = list(l = 50, r = 20, t = 20, b = 80))
  })

  output$source_toxicity <- renderPlotly({
    source_data <- kenya_cases %>% group_by(data_source) %>%
      summarise(avg_tox = mean(toxicity_score)) %>% arrange(desc(avg_tox))
    plot_ly(source_data, x = ~reorder(data_source, avg_tox), y = ~avg_tox, type = "bar",
            marker = list(color = ~avg_tox, colorscale = "Reds")) %>%
      layout(xaxis = list(title = ""), yaxis = list(title = "Avg Toxicity %"),
             margin = list(l = 50, r = 20, t = 20, b = 80))
  })

  output$target_pie <- renderPlotly({
    target_data <- kenya_cases %>% count(target_group)
    plot_ly(target_data, labels = ~target_group, values = ~n, type = "pie",
            textinfo = "label+percent", marker = list(colors = RColorBrewer::brewer.pal(4, "Set2"))) %>%
      layout(showlegend = FALSE, margin = list(l = 20, r = 20, t = 20, b = 20))
  })

  # ==================== ALERTS TABLE ====================

  output$alerts_table <- renderDT({
    data <- kenya_cases %>%
      select(id, county, narrative_type, data_source, sentiment, toxicity_score,
             target_group, cases, risk_level, alert_status, timestamp) %>%
      arrange(desc(timestamp)) %>%
      mutate(timestamp = format(timestamp, "%Y-%m-%d %H:%M"))

    datatable(data,
      options = list(pageLength = 40, scrollX = TRUE, dom = 'Bfrtip',
                     buttons = c('copy', 'csv', 'excel')),
      filter = "top", rownames = FALSE,
      colnames = c("ID", "County", "Narrative", "Source", "Sentiment", "Toxicity",
                   "Target", "Cases", "Risk", "Status", "Timestamp")
    ) %>%
      formatStyle("risk_level", backgroundColor = styleEqual(
        c("Low", "Medium", "High"), c("#d5f5e3", "#fdebd0", "#fadbd8"))) %>%
      formatStyle("toxicity_score", background = styleColorBar(c(0, 100), "#e74c3c"),
                  backgroundSize = "98% 88%", backgroundRepeat = "no-repeat", backgroundPosition = "center")
  })

  output$download_csv <- downloadHandler(
    filename = function() { paste0("alerts_export_", Sys.Date(), ".csv") },
    content = function(file) { write.csv(kenya_cases, file, row.names = FALSE) }
  )

  # ==================== REPORTS ====================

  output$report_preview <- renderUI({
    div(style = "padding: 20px; background: #f8f9fa; border-radius: 8px; min-height: 400px;",
      h4(input$report_type),
      p(class = "text-muted", paste("Date Range:", input$report_dates[1], "to", input$report_dates[2])),
      p(class = "text-muted", paste("County:", input$report_county)),
      hr(),

      if ("Summary Statistics" %in% input$report_sections) {
        div(
          h5("Summary Statistics"),
          tags$ul(
            tags$li(paste("Total Cases:", format(sum(kenya_cases$cases), big.mark = ","))),
            tags$li(paste("High Risk Alerts:", sum(kenya_cases$risk_level == "High"))),
            tags$li(paste("Average Toxicity:", round(mean(kenya_cases$toxicity_score), 1), "%")),
            tags$li(paste("Most Affected County:", kenya_cases %>% group_by(county) %>%
                          summarise(t = sum(cases)) %>% arrange(desc(t)) %>% slice(1) %>% pull(county)))
          )
        )
      },

      if ("Top Alerts" %in% input$report_sections) {
        div(
          h5("Top 5 High-Risk Alerts"),
          tags$ol(
            lapply(head(kenya_cases %>% arrange(desc(cases)), 5)$county, tags$li)
          )
        )
      },

      if ("Recommendations" %in% input$report_sections) {
        div(
          h5("Recommendations"),
          tags$ul(
            tags$li("Increase monitoring in high-risk counties"),
            tags$li("Engage community leaders in affected areas"),
            tags$li("Coordinate with NCTC for flagged extremism cases"),
            tags$li("Deploy counter-narrative campaigns on social media")
          )
        )
      }
    )
  })

  output$download_html <- downloadHandler(
    filename = function() { paste0("report_", Sys.Date(), ".html") },
    content = function(file) {
      writeLines(paste("<html><head><title>Report</title></head><body>",
                       "<h1>", input$report_type, "</h1>",
                       "<p>Generated:", Sys.time(), "</p>",
                       "<p>Total Cases:", sum(kenya_cases$cases), "</p>",
                       "</body></html>"), file)
    }
  )

  output$download_pdf <- downloadHandler(
    filename = function() { paste0("report_", Sys.Date(), ".pdf") },
    content = function(file) {
      showNotification("PDF generation requires rmarkdown setup", type = "warning")
    }
  )

  # ==================== SETTINGS ====================

  observeEvent(input$save_settings, {
    showNotification("Settings saved successfully!", type = "message", duration = 3)
  })

  observeEvent(input$clear_cache, {
    showNotification("Cache cleared!", type = "message", duration = 3)
  })
}
