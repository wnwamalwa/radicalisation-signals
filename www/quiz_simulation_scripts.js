/* ====================================
   QUIZZES & SIMULATIONS JAVASCRIPT
   File: quiz_simulation_scripts.js
   ==================================== */

// Handle tab click events
$(document).on('click', '.quiz-sim-tab', function() {
  // Remove active class from all tabs
  $('.quiz-sim-tab').removeClass('active');

  // Add active class to clicked tab
  $(this).addClass('active');
});

// Set default active tab on page load
$(document).ready(function() {
  // Check if simulation tab exists
  if ($('#simulation_tab').length) {
    // Set simulation as default active tab
    $('#simulation_tab').addClass('active');

    // Initialize Shiny input value
    if (typeof Shiny !== 'undefined') {
      Shiny.setInputValue('quiz_sim_mode', 'simulation', {priority: 'event'});
    }
  }
});

// Handle page navigation - reset to simulation tab when navigating to quizzes section
$(document).on('shiny:inputchanged', function(event) {
  if (event.name === 'current_page' && event.value.includes('quizzes')) {
    // Small delay to ensure DOM is ready
    setTimeout(function() {
      if ($('#simulation_tab').length && !$('.quiz-sim-tab.active').length) {
        $('#simulation_tab').addClass('active');

        if (typeof Shiny !== 'undefined') {
          Shiny.setInputValue('quiz_sim_mode', 'simulation', {priority: 'event'});
        }
      }
    }, 100);
  }
});

// Optional: Add keyboard navigation
$(document).on('keydown', function(e) {
  // Check if we're on the quizzes page
  if ($('.quiz-sim-layout').length) {
    // Press '1' for Simulation
    if (e.key === '1') {
      $('#simulation_tab').click();
    }
    // Press '2' for Quiz
    if (e.key === '2') {
      $('#quiz_tab').click();
    }
  }
});

// Smooth scroll to top when switching tabs
$(document).on('click', '.quiz-sim-tab', function() {
  $('.quiz-sim-main').animate({
    scrollTop: 0
  }, 300);
});


/* ============================================================
   QUIZ-SIM PAGE — HIDE LEFT SIDEBAR & REMOVE RESERVED SPACE
   ============================================================ */

body[data-page="quiz-sim"] .desktop-left-sidebar {
  display: none !important;
  width: 0 !important;
}

/* Expand main layout to full width */
body[data-page="quiz-sim"] .main-content {
  margin-left: 0 !important;
  max-width: 100% !important;
  width: 100% !important;
  padding-left: 0 !important;
  padding-right: 0 !important;
}

/* Ensure quiz wrapper touches left edge */
body[data-page="quiz-sim"] .quiz-sim-wrapper {
  padding-left: 0 !important;
  margin-left: 0 !important;
}
