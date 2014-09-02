/* jshint jquery: true */
/* global $, _, Bloodhound, document, areaFullNameIndex,
   observationsByArea, periods */
$(function() {

function $f(el) {
  return $(document.createElement(el));
}

function getObservationsByFullName(fullName) {
  var id = areaFullNameIndex[fullName];
  return observationsByArea[id];
}

var areas = new Bloodhound({
  datumTokenizer: Bloodhound.tokenizers.obj.whitespace('value'),
  queryTokenizer: Bloodhound.tokenizers.whitespace,
  local: _.map(areaFullNameIndex, function(value, key) {
    return { value: key };
  })
});
areas.initialize();

var fauxDatacube = {
  dimensions: [
    {
      '@id': 'bm:refPeriod',
      literalValues: periods.reverse(),
      values: _.map(periods, function(p) {
        return {
          '@value': p,
          '@type': 'xsd:gYear'
        };
      })
    }
  ],
  measures: [],
  observations: []
};

function activateTypeahead(el) {
  if (!el) {
    el = this;
  }

  el = $(el);

  $(el).typeahead({
    hint: true,
    highlight: true,
    minLength: 1
  },
  {
    name: 'areas',
    displayKey: 'value',
    source: areas.ttAdapter()
  });

  var button = $('button', $(el).parents('.input-group').first());
  if (areaFullNameIndex[el.val()]) {
    button.removeAttr('disabled');
  }
  else {
    button.attr('disabled', 'disabled');
  }

  $(el).on('keyup typeahead:selected', function(e) {
    var target = e.target;
    if (areaFullNameIndex[target.value]) {
      button.removeAttr('disabled');
    }
    else {
      button.attr('disabled', 'disabled');
    }
  });

  var addHandler = function(e) {
    if (e.target.disabled) {
      return;
    }

    var areaId = areaFullNameIndex[el.val()];
    var areaName = el.val();

    el.val('');

    setTimeout(function() {
      el.val('');
    }, 200);

    fauxDatacube.measures.push({
      '@id': areaId,
      'rdfs:label': areaName
    });

    var tbody = $('#view-list');
    var tr = $f('tr');
    var td1 = $f('td');
    var td2 = $f('td');
    var btn = $f('btn').addClass('btn').addClass('btn-danger').addClass('btn-block').addClass('btn-xs');
    var icon = $f('i').addClass('glyphicon').addClass('glyphicon-eye-close');
    btn.append(icon);
    td1.text(areaName);
    td2.append(btn);
    tr.append(td1).append(td2);
    tbody.append(tr);
    btn.click(function(e) {
      e.preventDefault();
      fauxDatacube.observations = _.filter(fauxDatacube.observations, function(o) {
        return o['bm:refArea'] !== areaId;
      });
      fauxDatacube.measures = _.filter(fauxDatacube.measures, function(m) {
        return m['@id'] !== areaId;
      });
      redrawChart();
      tr.fadeOut('fast', function() { tr.detach() });
    });

    _.forEach(observationsByArea[areaId], function(obs) {
      fauxDatacube.observations.push(obs);
    });

    fauxDatacube.observations = _.sortBy(fauxDatacube.observations, function(o) {
      return o['bm:refPeriod']['@value'];
    });

    el.focus();
    redrawChart();
  };

  button.click(addHandler);
  el.on('typeahead:selected', addHandler);
}

function redrawChart() {
  var outer = $('#chart-container-outer');
  outer.empty();
  var container = $f('div').addClass('chart-container');
  outer.append(container);
  var chart = new bm.Chart(container, fauxDatacube);
  chart.draw();
}

activateTypeahead('.area-entry-input');
redrawChart();


});