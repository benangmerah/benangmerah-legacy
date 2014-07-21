var bm = {};

bm.getLdValue = function(ldObj) {
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

bm.Chart = function Chart(chartContainerElement, dataset, dimensionId, measureId) {
  this.containerElement = $(chartContainerElement);
  this.dataset = dataset;
  this.dimensionId = dimensionId;
  this.measureId = measureId;

  this.determineChartType();

  bm.Chart.instances.push(this);
}

bm.Chart.instances = [];

bm.Chart.prototype.determineChartType = function() {
  if (this.dataset.dimensions.length === 1 || this.dimensionId) {
    if (bm.getLdValue(this.dataset.dimensions[0].values[0]).match(/^[0-9]/)) {
      this.chartType = 'LineChart';
    }
    else {
      this.chartType = 'BarChart';
    }
  }
}

bm.Chart.prototype.draw = function() {
  if (!this['draw' + this.chartType]) {
    return false;
  }

  this.svgElement = d3.select(this.containerElement[0]).append('svg');
  this['draw' + this.chartType]();

  if (!this['resize' + this.chartType]) {
    return true;
  }

  this.oldWidth = this.getContainerWidth();
  this.oldHeight = this.getContainerHeight();
  var self = this;
  $(window).on('resize', function() {
    var newWidth = self.getContainerWidth();
    var newHeight = self.getContainerHeight();
    if (newWidth !== self.oldWidth || newHeight !== self.oldHeight) {
      self['resize' + self.chartType]();
      self.oldWidth = newWidth;
      self.oldHeight = newHeight;
    }
  });

  return true;
};

bm.Chart.prototype.getContainerWidth = function() {
  return Math.floor(this.containerElement.innerWidth());
};

bm.Chart.prototype.getContainerHeight = function() {
  return Math.floor(this.containerElement.innerHeight());
};

bm.Chart.prototype.margins = { top: 15, right: 20, bottom: 30, left: 60 };

bm.Chart.prototype.drawLineChart = function() {
  var svgElement = this.svgElement;
  var dataset = this.dataset;
  var dimensionId = this.dimensionId;
  var measureId = this.measureId;
  var outerWidth = this.getContainerWidth();
  var outerHeight = this.getContainerHeight();
  var margins = this.margins;

  var width = outerWidth - margins.left - margins.right;
  var height = outerHeight - margins.top - margins.bottom;

  var svg =
    svgElement
      .attr('class', 'line-chart')
      .attr('width', outerWidth)
      .attr('height', outerHeight);

  var chart = this.lineChart =
    svg.append('g')
      .attr('transform', 'translate(' + margins.left + ',' + margins.top + ')')
      .attr('width', width).attr('height', height);

  if (!dimensionId) {
    dimensionId = dataset.dimensions[0]['@id'];
  }

  var observations = dataset.observations;

  var dimensionPlot = this.dimensionPlot = [];
  dataset.dimensions.forEach(function(dimension) {
    var id = dimension['@id'];
    if (id !== dimensionId) {
      return;
    }

    var values = dimension.values;
    values.forEach(function(value, idx) {
      value = bm.getLdValue(value);
      dimensionPlot[value] = idx;
    });
  });

  var maxValue = 0;
  var minValue;
  var measureValues = this.measureValues = {};
  var dimensionValues = this.dimensionValues = [];
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

      var dimensionValue = bm.getLdValue(observation[dimensionId]);
      var value = bm.getLdValue(observation[id]);

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

  var interval = width / (dimensionValues.length - 1);
  var dimensionRange = [];
  dimensionValues.forEach(function(val) {
    var x = dimensionPlot[val] * interval;
    dimensionRange.push(x);
  });

  var minMaxDelta = maxValue - minValue;
  var logMin = Math.log(minValue);
  var logMax = Math.log(maxValue);
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

  var x = this.x = d3.scale.ordinal().range(dimensionRange).domain(dimensionValues);
  var y = this.y = d3.scale.linear().range([height,0]).domain([lowerLimit, upperLimit]);

  var getX = this.getX = function(d) {
    return x(d.dimensionValue);
  };
  var getY = this.getY = function(d) {
    return y(d.value);
  };

  var line = d3.svg.line().x(getX).y(getY);

  var xAxis = this.xAxis = d3.svg.axis()
      .scale(x).orient('bottom');
  var yAxis = this.yAxis = d3.svg.axis()
      .scale(y).orient('left');

  var xGridLines = this.xGridLines = d3.svg.axis()
      .scale(x).orient('bottom')
      .tickSize(-height, 0, 0).tickFormat('');
  var yGridLines = this.yGridLines = d3.svg.axis()
      .scale(y).orient('left')
      .tickSize(-width, 0, 0).tickFormat('')

  chart.append('g')         
    .attr('class', 'x grid')
    .attr('transform', 'translate(0,' + height + ')')
    .call(xGridLines);
  chart.append('g')         
    .attr('class', 'y grid')
    .call(yGridLines);

  chart.append('g')
      .attr('class', 'x axis')
      .attr('transform', 'translate(0,' + height + ')')
      .call(xAxis);
  chart.append('g')
      .attr('class', 'y axis')
      .call(yAxis);

  dataset.measures.forEach(function(measure, n) {
    var values = measureValues[measure['@id']];

    chart.append('path')
      .datum(values)
      .attr('class', 'line m' + n)
      .attr('d', line);

    chart.selectAll('circle.m' + n)
      .data(values)
      .enter()
      .append('circle')
      .attr('class', 'm' + n)
      .attr('cx', getX)
      .attr('cy', getY)
      .attr('r', 4);
  });
}

bm.Chart.prototype.resizeLineChart = function() {
  var outerWidth = this.getContainerWidth();
  var outerHeight = this.getContainerHeight();
  var margins = this.margins;
  var chart = this.lineChart;
  var dataset = this.dataset;
  var measureValues = this.measureValues;

  var width = outerWidth - margins.left - margins.right;
  var height = outerHeight - margins.top - margins.bottom;

  this.svgElement
    .attr('width', outerWidth)
    .attr('height', outerHeight);

  this.lineChart
    .attr('width', width)
    .attr('height', height);

  var dimensionValues = this.dimensionValues;
  var dimensionPlot = this.dimensionPlot;
  var interval = width / (dimensionValues.length - 1);
  var dimensionRange = [];
  this.dimensionValues.forEach(function(val) {
    var x = dimensionPlot[val] * interval;
    dimensionRange.push(x);
  });

  var x = this.x.range(dimensionRange);
  var y = this.y.range([height,0]);
  this.xGridLines.tickSize(-height, 0, 0);
  this.yGridLines.tickSize(-width, 0, 0);
  var getX = this.getX = function(d) {
    return x(d.dimensionValue);
  };
  var getY = this.getY = function(d) {
    return y(d.value);
  };
  var line = d3.svg.line().x(getX).y(getY);

  chart.select('.x.axis').call(this.xAxis);
  chart.select('.y.axis').call(this.yAxis);
  chart.select('.x.grid').call(this.xGridLines);
  chart.select('.y.grid').call(this.yGridLines);
  chart.selectAll('.line').attr('d', line);
  chart.selectAll('circle').attr('cx', this.getX).attr('cy', this.getY);
}