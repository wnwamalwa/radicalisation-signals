$(document).ready(function () {
  $.getJSON('http://ip-api.com/json/?fields=status,country,city,regionName,lat,lon', function (data) {
    if (data.status === 'success') {
      Shiny.setInputValue('ip_country', data.country);
      Shiny.setInputValue('ip_region', data.regionName);
      Shiny.setInputValue('ip_city', data.city);
      Shiny.setInputValue('ip_lat', data.lat);
      Shiny.setInputValue('ip_lon', data.lon);
    }
  });
});
