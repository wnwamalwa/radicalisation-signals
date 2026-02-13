Shiny.addCustomMessageHandler('setCookie', function(message) {
  document.cookie = message.name + '=' + message.value + ';path=/;max-age=31536000';
});

Shiny.addCustomMessageHandler('getCookie', function(message) {
  var name = message.name + '=';
  var decodedCookie = decodeURIComponent(document.cookie);
  var ca = decodedCookie.split(';');
  var value = '';
  for (var i = 0; i < ca.length; i++) {
    var c = ca[i];
    while (c.charAt(0) == ' ') {
      c = c.substring(1);
    }
    if (c.indexOf(name) == 0) {
      value = c.substring(name.length, c.length);
    }
  }
  Shiny.setInputValue(message.input, value);
});
