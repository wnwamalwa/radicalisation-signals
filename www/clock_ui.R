# clock_ui.R
library(htmltools)

clock_ui <- tags$div(
  style = "display:flex; flex-direction:column; align-items:center; padding:0.25rem 1.025rem 0.275rem; margin-top:5%;",
  tags$canvas(
    id    = "analogClock",
    width = 230,
    height = 230,
    style = "border:1px solid #ccc; border-radius:50%;"
  )
)
