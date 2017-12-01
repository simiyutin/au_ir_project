var button = document.getElementById('searchButton');
var queryInput = document.getElementById('queryInput');

var xhttp = new XMLHttpRequest();
xhttp.onreadystatechange = function () {
  if (this.readyState === 4 && this.status === 200) {
    document.getElementById("results").innerHTML = this.responseText;
  }
};


button.onclick = function () {
    var query = queryInput.value;
    xhttp.open("GET", "ask?query=" + encodeURIComponent(query), true);
    xhttp.send();
};