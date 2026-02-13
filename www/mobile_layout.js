// responsive.js

function updateLayout() {
  const container = document.getElementById('app-container');
  if (!container) return;
  if (window.innerWidth <= 768) {
    container.className = 'mobile-layout';
  } else {
    container.className = 'desktop-layout';
  }
}

window.addEventListener('resize', updateLayout);
window.addEventListener('load', updateLayout);
