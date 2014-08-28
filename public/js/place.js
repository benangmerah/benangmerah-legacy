var visibleDatasets, visibleTopics;
var showTopic, addTopic, clearTopic, enforceFacet, addAllTopics, clearAllTopics;
var $datasets;
$(function() {
  $('[data-dataset-id]').each(function() {
    var el = $(this);
    var datasetId = el.attr('data-dataset-id');
    var dataset = datasets[datasetId];
    if (dataset.dimensions.length === 1) {
      var header = $('header', el);
      var container = $('<div>').addClass('chart-container');
      container.insertAfter(header);
      var chart = new bm.Chart(container, dataset);
      chart.draw();
    }
  });

  visibleTopics = _.keys(topics);
  visibleDatasets = [];
  _.forEach(visibleTopics, function(tpc) {
    visibleDatasets = _.union(visibleDatasets, topics[tpc].datasets);
  });

  $datasets = function (datasetIds) {
    var els = _(datasetIds).map(function(id) {
      return '#' + id;
    }).join(', ');
    return $(els);
  };

  $datasetTOCs = function (datasetIds) {
    var els = _(datasetIds).map(function(id) {
      return '#toc-' + id;
    }).join(', ');
    return $(els);
  };

  addTopic = function(topicId) {
    var topic = topics[topicId];
    var newFacet = _.union(visibleDatasets, topic.datasets);
    visibleTopics.push(topicId);
    enforceFacet(newFacet);
  };

  clearTopic = function(topicId) {
    var topic = topics[topicId];
    var newFacet = [];
    visibleTopics = _.without(visibleTopics, topicId);
    _.forEach(visibleTopics, function(tpc) {
      newFacet = _.union(newFacet, topics[tpc].datasets);
    });
    enforceFacet(newFacet);
  };

  addAllTopics = function() {
    visibleTopics = _.keys(topics);
    var newFacet = [];
    _.forEach(visibleTopics, function(tpc) {
      newFacet = _.union(newFacet, topics[tpc].datasets);
    });
    enforceFacet(newFacet);
  };

  clearAllTopics = function() {
    visibleTopics = [];
    enforceFacet([]);
  };

  enforceFacet = function(newFacet) {
    var oldFacet = visibleDatasets;
    var toHide = _.difference(oldFacet, newFacet);
    var toShow = _.difference(newFacet, oldFacet);
    $datasets(toHide).hide();
    $datasets(toShow).slideDown();
    $datasetTOCs(toHide).slideUp();
    $datasetTOCs(toShow).slideDown();
    visibleDatasets = newFacet;
    $('#dataset-count').text(visibleDatasets.length.toString());
  };

  $('#datasets > header').after(
    '<section class="filterbox infobox">' +
    '  <div class="row">' +
    '    <div class="col-md-4">' +
    '      <h5><i class="glyphicon glyphicon-filter"></i>' +
    '          Saring berdasarkan topik</h5>' +
    '      <div id="topic-list">' +
    '      </div>' +
    '    </div>' +
    '    <div class="col-md-8">' +
    '      <h5>Lompat menuju kumpulan data tertentu (<span id="dataset-count">0</span> tersedia)</h5>' +
    '      <ul class="link-list" id="dataset-toc">' +
    '      </ul>' +
    '    </div>' +
    '  </div>' +
    '</section>');

  $f = function(el) {
    return $(document.createElement(el));
  };

  _.forEach(topics, function(topic) {
    var id = topic['@id'];
    var div = $f('div').addClass('checkbox');
    var label = $f('label');
    var checkbox =
      $f('input').attr('type', 'checkbox').attr('checked', 'checked');
    checkbox.on('click', function() {
      if (this.checked) {
        addTopic(id);
      }
      else {
        clearTopic(id);
      }
    });

    label.append(checkbox);
    label.append(' ' + topic['rdfs:label']);
    div.append(label);

    $('#topic-list').append(div);
  });

  _.forEach(datasetTitles, function(title, id) {
    var li = $f('li');
    var a = $f('a');
    a.attr('href', '#' + id);
    a.text(title);
    li.append(a);
    li.attr('id', 'toc-' + id);
    $('#dataset-toc').append(li);
  });

  $('#dataset-count').text(visibleDatasets.length.toString());
});