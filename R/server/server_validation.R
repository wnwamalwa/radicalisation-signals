# ================================================================
#  R/server_validation.R — Validation UI + do_validate + Observers
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

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
  
