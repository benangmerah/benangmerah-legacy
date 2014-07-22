var bm = {};

bm.d3locale = d3.locale({
  decimal: ',',
  thousands: '.',
  grouping: [3],
  currency: ['Rp', ''],
  dateTime: '%a %b %e %X %Y',
  date: '%d/%m/%Y',
  time: '%H:%M:%S',
  periods: ['AM', 'PM'],
  days: ['Minggu', 'Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu'],
  shortDays: ['Mgg', 'Sen', 'Sel', 'Rab', 'Kam', 'Jum', 'Sab'],
  months: ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'],
  shortMonths: ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agt', 'Sep', 'Okt', 'Nov', 'Des']
});

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

bm.getPreferredLabel = function(jsonLdResource) {
  var labels;
  if (jsonLdResource['rdfs:label']) {
    labels = jsonLdResource['rdfs:label'];
  }
  else if (jsonLdResource[shared.RDFS_NS + 'label']) {
    labels = jsonLdResource[shared.RDFS_NS + 'label'];
  }
  else if (jsonLdResource['@id']) {
    return shared.getPropertyName(jsonLdResource['@id']);
  }
  else {
    return '';
  }

  if (typeof labels == 'string') {
    return labels;
  }

  if (!(labels instanceof Array)) {
    console.log(labels);
    return shared.getLdValue(labels);
  }

  var preferredLabel = '';

  labels.forEach(function(label) {
    if (label['@lang'] !== 'id' && preferredLabel) {
      return;
    }

    var labelValue = shared.getLdValue(label);
    if (labelValue.length > preferredLabel.length) {
      preferredLabel = labelValue;
    }
  });

  return preferredLabel;
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
  var self = this;

  // Setup data
  var dataset = self.dataset;
  var dimensionId = self.dimensionId;
  var measureId = self.measureId;

  // Setup dimensions
  var dimension;
  if (self.dimensionId) {
    _.forEach(dataset.dimensions, function(dim) {
      if (dim['@id'] === self.dimensionId) {
        dimension = dim;
      }
    });
  }
  else {
    dimension = dataset.dimensions[0];
    dimensionId = dimension['@id'];
  }

  var dimensionValues = self.dimensionValues = dimension.literalValues;
  var dimensionPlot = _.invert(dimensionValues);

  var measures = measureId ? [measureId] : dataset.measures;

  var measureValues = {};
  dataset.measures.forEach(function(measure) {
    measureValues[measure['@id']] = [];
  });

  var maxValue = -Infinity;
  var observations = dataset.observations;
  observations.forEach(function(observation) {
    measures.forEach(function(measure) {
      var measureId = measure['@id'];
      var dimensionValue = bm.getLdValue(observation[dimensionId]);
      var value = bm.getLdValue(observation[measureId]);

      measureValues[measureId].push({
        measureLabel: bm.getPreferredLabel(measure),
        dimensionValue: dimensionValue,
        value: value
      });

      maxValue = value > maxValue ? value : maxValue;
    });
  });

  // Setup chart
  var outerWidth = self.getContainerWidth();
  var outerHeight = self.getContainerHeight();
  var margins = self.margins;

  var chartWidth = outerWidth - margins.left - margins.right;
  var chartHeight = outerHeight - margins.top - margins.bottom;

  var formatNumber = bm.d3locale.numberFormat(',');

  self.svgElement.attr('class', 'line-chart')
    .attr('width', outerWidth).attr('height', outerHeight);
  var chart = self.lineChart = self.svgElement.append('g')
    .attr('transform', 'translate(' + margins.left + ',' + margins.top + ')')
    .attr('width', chartWidth).attr('height', chartHeight);

  var logMax10 = Math.log(maxValue) * Math.LOG10E;
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

  self.xScale = d3.scale.ordinal()
    .domain(dimensionValues)
    .range(_.map(dimensionValues, function(v, i) {
      return i * chartWidth / (dimensionValues.length - 1);
    }));
  self.yScale = d3.scale.linear()
    .domain([0, upperLimit])
    .range([chartHeight,0]);

  self.getX = function(d) {
    return self.xScale(d.dimensionValue);
  }
  self.getY = function(d) {
    return self.yScale(d.value);
  }
  self.getBarHeight = function(d) {
    return chartHeight - self.getY(d);
  }

  self.line = d3.svg.line().x(self.getX).y(self.getY);

  // Setup axes & gridlines
  self.xAxis = d3.svg.axis()
    .scale(self.xScale).orient('bottom');
  self.yAxis = d3.svg.axis()
    .scale(self.yScale).orient('left').tickFormat(formatNumber);
  self.vGridLines = d3.svg.axis()
    .scale(self.xScale).orient('bottom')
    .tickSize(-chartHeight, 0, 0).tickFormat('');
  self.hGridLines = d3.svg.axis()
    .scale(self.yScale).orient('left')
    .tickSize(-chartWidth, 0, 0).tickFormat('');

  chart.append('g')
    .attr('class', 'v grid')
    .attr('transform', 'translate(0,' + chartHeight + ')')
    .call(self.vGridLines);
  chart.append('g')
    .attr('class', 'h grid')
    .call(self.hGridLines);
  chart.append('g')
    .attr('class', 'x axis')
    .attr('transform', 'translate(0,' + chartHeight + ')')
    .call(self.xAxis);
  chart.append('g')
    .attr('class', 'y axis')
    .call(self.yAxis);

  self.makeTooltip = function(d) {
    return '<strong>' + d.measureLabel + '</strong><br>' + formatNumber(d.value);
  }

  var n = 0;
  _.forEach(measureValues, function(values) {
    var line = chart.append('g').attr('class', 'measure');

    line.on('mouseover', function() {
      var parent = this.parentNode;
      $(this).detach().appendTo(parent);
      d3.select(this.parentNode).selectAll('.measure').attr('class', 'measure away');
      d3.select(this).attr('class', 'measure hover');
    }).on('mouseout', function() {
      d3.select(this.parentNode).selectAll('.measure').attr('class', 'measure');
      d3.select(this).attr('class', 'measure');
    });

    line.append('path')
      .datum(values)
      .attr('class', 'line m' + n)
      .attr('d', self.line);

    line.selectAll('circle.m' + n)
      .data(values)
      .enter()
      .append('circle')
      .attr('class', 'm' + n)
      .attr('cx', self.getX)
      .attr('cy', self.getY)
      .attr('r', 4)
      .attr('title', self.makeTooltip)
      .each(function() {
        $(this).tipsy({ opacity: 1, html: true });
      });

    ++n;
  });

  // function foT() { $('svg.line-chart circle').tipsy({opacity:.9, gravity:'n', html:true}); }
  // setTimeout(foT, 2000);
}

bm.Chart.prototype.resizeLineChart = function() {
  var self = this;
  var measureValues = self.measureValues;

  var outerWidth = self.getContainerWidth();
  var outerHeight = self.getContainerHeight();
  var margins = self.margins;
  var chart = self.lineChart;

  var chartWidth = outerWidth - margins.left - margins.right;
  var chartHeight = outerHeight - margins.top - margins.bottom;

  self.svgElement
    .attr('width', outerWidth)
    .attr('height', outerHeight);

  self.lineChart
    .attr('width', chartWidth)
    .attr('height', chartHeight);

  self.xScale.range(_.map(self.dimensionValues, function(v, i) {
    return i * chartWidth / (self.dimensionValues.length - 1);
  }));
  self.yScale.range([chartHeight,0]);

  self.vGridLines.tickSize(-chartHeight, 0, 0);
  self.hGridLines.tickSize(-chartWidth, 0, 0);

  chart.select('.x.axis').call(self.xAxis);
  chart.select('.y.axis').call(self.yAxis);
  chart.select('.v.grid').call(self.vGridLines);
  chart.select('.h.grid').call(self.hGridLines);
  chart.selectAll('.line').attr('d', self.line);
  chart.selectAll('circle').attr('cx', self.getX).attr('cy', self.getY);
}

bm.Chart.prototype.drawBarChart = function() {
  var self = this;

  // Setup data
  var dataset = self.dataset;
  var dimensionId = self.dimensionId;
  var measureId = self.measureId;

  // Setup dimensions
  var dimension;
  if (self.dimensionId) {
    _.forEach(dataset.dimensions, function(dim) {
      if (dim['@id'] === self.dimensionId) {
        dimension = dim;
      }
    });
  }
  else {
    dimension = dataset.dimensions[0];
    dimensionId = dimension['@id'];
  }

  var dimensionValues = self.dimensionValues = dimension.literalValues;
  var dimensionPlot = _.invert(dimensionValues);

  var measures = measureId ? [measureId] : dataset.measures;

  var measureValues = {};
  dataset.measures.forEach(function(measure) {
    measureValues[measure['@id']] = [];
  });

  var maxValue = -Infinity;
  var observations = dataset.observations;
  observations.forEach(function(observation) {
    measures.forEach(function(measure) {
      var measureId = measure['@id'];
      var dimensionValue = bm.getLdValue(observation[dimensionId]);
      var value = bm.getLdValue(observation[measureId]);

      measureValues[measureId].push({
        dimensionValue: dimensionValue,
        value: value
      });

      maxValue = value > maxValue ? value : maxValue;
    });
  });

  // Setup chart
  var outerWidth = self.getContainerWidth();
  var outerHeight = self.getContainerHeight();
  var margins = self.margins;

  var chartWidth = outerWidth - margins.left - margins.right;
  var chartHeight = outerHeight - margins.top - margins.bottom;

  var formatNumber = bm.d3locale.numberFormat(',');

  self.svgElement.attr('class', 'bar-chart')
    .attr('width', outerWidth).attr('height', outerHeight);
  var chart = self.barChart = self.svgElement.append('g')
    .attr('transform', 'translate(' + margins.left + ',' + margins.top + ')')
    .attr('width', chartWidth).attr('height', chartHeight);

  self.xScale = d3.scale.ordinal()
    .domain(dimensionValues)
    .rangeRoundBands([0, chartWidth], 0.1);
  self.yScale = d3.scale.linear()
    .domain([0, 1.05 * maxValue])
    .range([chartHeight,0]);

  self.getX = function(d) {
    return self.xScale(d.dimensionValue);
  }
  self.getY = function(d) {
    return self.yScale(d.value);
  }
  self.getBarHeight = function(d) {
    return chartHeight - self.getY(d);
  }

  // Setup axes & gridlines
  self.xAxis = d3.svg.axis()
    .scale(self.xScale).orient('bottom');
  self.yAxis = d3.svg.axis()
    .scale(self.yScale).orient('left').tickFormat(formatNumber);
  self.hGridLines = d3.svg.axis()
    .scale(self.yScale).orient('left')
    .tickSize(-chartWidth, 0, 0).tickFormat('');

  chart.append('g')
    .attr('class', 'h grid')
    .call(self.hGridLines);
  chart.append('g')
    .attr('class', 'x axis')
    .attr('transform', 'translate(0,' + chartHeight + ')')
    .call(self.xAxis);
  chart.append('g')
    .attr('class', 'y axis')
    .call(self.yAxis);

  _.forEach(measureValues, function(values) {
    var bar = chart.selectAll('g.bar')
      .data(values).enter()
      .append('g').attr('class', 'bar')
      .attr('transform', function(d) {
        return 'translate(' + self.getX(d) + ',' + self.getY(d) + ')';
      })
      .attr('width', self.xScale.rangeBand())
      .attr('height', self.getBarHeight);

    var rect = bar.append('rect')
      .attr('width', self.xScale.rangeBand())
      .attr('height', self.getBarHeight);

    bar.append('text')
      .attr('x', self.xScale.rangeBand() / 2)
      .attr('y', 0)
      .attr('text-anchor', 'middle')
      .attr('dy', function(d) {
        var text = d3.select(this);
        if (parseInt(text.style('font-size')) * 2 > self.getBarHeight(d)) {
          text.attr('class', 'outside');
          return '-0.75em';
        }
        else {
          text.attr('class', 'inside');
          return '2em';
        }
      })
      .text(function(d) {
        return formatNumber(d.value);
      });
  })
};

bm.Chart.prototype.resizeBarChart = function() {
  var self = this;

  var chart = self.barChart;

  var outerWidth = self.getContainerWidth();
  var outerHeight = self.getContainerHeight();
  var margins = self.margins;

  var chartWidth = outerWidth - margins.left - margins.right;
  var chartHeight = outerHeight - margins.top - margins.bottom;

  self.xScale.rangeRoundBands([0,chartWidth], 0.1);
  self.hGridLines.tickSize(-chartWidth, 0, 0);

  chart.select('.x.axis').call(self.xAxis);
  chart.select('.y.axis').call(self.yAxis);
  chart.select('.h.grid').call(self.hGridLines);
  chart.selectAll('g.bar')
    .attr('transform', function(d) {
      return 'translate(' + self.getX(d) + ',' + self.getY(d) + ')';
    });

  chart.selectAll('g.bar, g.bar rect')
    .attr('width', self.xScale.rangeBand())
    .attr('height', self.getBarHeight);

  chart.selectAll('g.bar text')
    .attr('x', self.xScale.rangeBand() / 2);
}