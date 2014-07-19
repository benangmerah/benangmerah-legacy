function getLdValue(ldObj) {
  if (typeof ldObj == 'string') {
    return ldObj;
  }
  if (ldObj['@type'] === 'xsd:decimal') {
    return parseFloat(ldObj['@value']);
  }
  if (ldObj['@value']) {
    return ldObj['@value'];
  }
};

function displayLineChart(chartElement, dataset, dimensionId, measureId) {
  var margin = { top: 15, right: 20, bottom: 30, left: 60 };
  var width = Math.floor($(chartElement).innerWidth() - margin.left - margin.right);
  var height = Math.floor($(chartElement).innerHeight() - margin.top - margin.bottom);
  var svg = d3.select(chartElement);
  var chart = svg.append('g')
              .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
              .attr('width', width).attr('height', height);

  if (!dimensionId) {
    dimensionId = dataset.dimensions[0]['@id'];
  }

  // var observations = _.sortBy(dataset.observations, function(val) {
  //   return getLdValue(val[dimensionId]);
  // });
  var observations = dataset.observations;

  var dimensionPlot = [];
  dataset.dimensions.forEach(function(dimension) {
    var id = dimension['@id'];
    if (id !== dimensionId) {
      return;
    }

    var values = dimension.values;
    values.forEach(function(value, idx) {
      value = getLdValue(value);
      dimensionPlot[value] = idx;
    });
  });

  var maxValue = 0;
  var minValue;
  var measureValues = {};
  var dimensionValues = [];
  dataset.measures.forEach(function(measure) {
    var id = measure['@id'];
    if (measureId && id !== measureId) {
      return;
    }
    measureValues[measure['@id']] = [];
  });
  observations.forEach(function(observation) {
    dataset.measures.forEach(function(measure) {
      var id = measure['@id'];
      if (measureId && id !== measureId) {
        return;
      }

      var dimensionValue = getLdValue(observation[dimensionId]);
      var value = observation[id] = getLdValue(observation[id]);

      if (!_.contains(dimensionValues, dimensionValue)) {
        dimensionValues.push(dimensionValue);
      }

      if (value > maxValue) {
        maxValue = value;
      }
      if (value < minValue || minValue === undefined) {
        minValue = value;
      }

      measureValues[id].push({
        dimensionValue: dimensionValue,
        value: value
      });
    });
  });
  dimensionValues.sort();

  var interval = width / (dimensionValues.length - 1);
  var dimensionRange = [];
  dimensionValues.forEach(function(val) {
    var x = dimensionPlot[val] * interval;
    dimensionRange.push(x);
  });

  var minMaxDelta = maxValue - minValue;
  var logMin = Math.log(minValue);
  var logMax = Math.log(maxValue);
  // var lowerLimit = Math.max(1 - (logMax - logMin), 0) * minValue;
  var lowerLimit = 0;

  var logMax10 = logMax * Math.LOG10E;
  var upperLimit;
  if (Math.ceil(logMax10) === 2) {
    upperLimit = 100;
  }
  else if (Math.ceil(logMax10) === 0) {
    upperLimit = 1;
  }
  else {
    upperLimit = 1.005 + maxValue;
  }

  var x = d3.scale.ordinal().range(dimensionRange).domain(dimensionValues);
  var y = d3.scale.linear().range([height,0]).domain([lowerLimit, upperLimit]);

  var getX = function(d) {
    return x(d.dimensionValue);
  };
  var getY = function(d) {
    return y(d.value);
  };

  var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom");
  var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left");

  chart.append('g')         
    .attr('class', 'grid')
    .attr('transform', 'translate(0,' + height + ')')
    .call(d3.svg.axis()
        .scale(x)
        .orient("bottom")
        .tickSize(-height, 0, 0)
        .tickFormat(''));

  chart.append('g')         
    .attr('class', 'grid')
    .call(d3.svg.axis()
        .scale(y)
        .orient("left")
        .tickSize(-width, 0, 0)
        .tickFormat(''));

  chart.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

  chart.append("g")
      .attr("class", "y axis")
      .call(yAxis);

  dataset.measures.forEach(function(measure) {
    var id = measure['@id'];
    var values = measureValues[id];
    var interval = width / values.length;
    var line = d3.svg.line().x(getX).y(getY);

    chart.append('path')
      .datum(values)
      .attr('class', 'line')
      .attr('d', line);

    values.forEach(function(datum) {
      var circle = chart.append('circle')
        .datum(datum)
        .attr('cx', getX)
        .attr('cy', getY)
        .attr('cy', getY)
        .attr('r', 3)
        .attr('title', function(d) { return d.value });
    })
  });
}