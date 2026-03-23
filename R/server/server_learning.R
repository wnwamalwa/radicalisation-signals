# ================================================================
#  R/server_learning.R — Learning Centre + v6 Outputs + Keywords
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

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
  
