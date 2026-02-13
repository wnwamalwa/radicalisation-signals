function autoScaleText() {
  // Scale value text
  const containers = document.querySelectorAll('.responsive-text-container');

  containers.forEach(container => {
    const textElement = container.querySelector('.extra-responsive-text');
    if (!textElement) return;

    // Reset scale to measure natural width
    textElement.style.transform = 'scale(1)';

    // Get dimensions
    const containerWidth = container.offsetWidth - 32; // Account for padding
    const textWidth = textElement.scrollWidth;

    // Calculate scale factor
    const scale = Math.min(1, containerWidth / textWidth);

    // Apply scale
    textElement.style.transform = `scale(${scale})`;
  });

  // Scale custom title text
  const titles = document.querySelectorAll('.responsive-vb-title');

  titles.forEach(title => {
    // Reset scale to measure natural width
    title.style.transform = 'scale(1)';

    // Get the value box container width
    const valueBox = title.closest('.bslib-value-box');
    if (!valueBox) return;

    const containerWidth = valueBox.offsetWidth - 32; // Account for padding and margins
    const titleWidth = title.scrollWidth;

    // Calculate scale factor
    const scale = Math.min(1, containerWidth / titleWidth);

    // Apply scale
    title.style.transform = `scale(${scale})`;
  });
}

// Run on load and resize
document.addEventListener('DOMContentLoaded', autoScaleText);
window.addEventListener('resize', autoScaleText);

// For Shiny - run when outputs update
$(document).on('shiny:value', function() {
  setTimeout(autoScaleText, 50);
});
