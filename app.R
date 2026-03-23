# ================================================================
#  app.R — Entry Point
#  Radicalisation Signals v6 · IEA Kenya NIRU AI Hackathon
#
#  global.R runs first (packages, modules, shared globals)
#  This file only wires server + shinyApp()
# ================================================================

server <- function(input, output, session) {
  source("R/server_init.R",        local = TRUE)
  source("R/server_validation.R",  local = TRUE)
  source("R/server_learning.R",    local = TRUE)
  source("R/server_misc.R",        local = TRUE)
}

shinyApp(ui, server)
