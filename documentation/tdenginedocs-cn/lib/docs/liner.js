/*JS to determine how many lines used in pre/code block, sets CSS appropriately. MUST be placed after elements with prettyprint class are loaded*/
$('.prettyprint').toArray().forEach(function(element){
    let linenums = element.clientHeight / 25.2;
    if (linenums > 99) {
      $(element).addClass('threec');
    }
    else if (linenums > 9) {
      $(element).addClass('twoc');
    }
  });
$('.prettyprint').toArray().forEach(function(element){
    let linenums = element.clientHeight / 25.2;
    if (linenums > 99) {
      $(element).addClass('threec');
    }
    else if (linenums > 9) {
      $(element).addClass('twoc');
    }
});