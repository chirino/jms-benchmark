(function ($) {
  function init(plot) {
    var count = 0;
    var names = [];
    var show = false;

    function processRawData(plot, series, data, datapoints) {
       if(show) {
           for (var i = 0; i < data.length; ++i) {
               data[i][0] = count;
           }
           data[3][0] = count - 1;
           data[4][0] = count + 1;
           names[count] = series.label;
           count += 1;
       }
    }

    function boxWhiskerTickGenerator(axis) {
        var res = [];
        for (var i = 0; i < names.length; ++i) {
            if (i >= axis.min && i <= axis.max)
                res.push([i, names[i]]);
        }
        return res;
    }

    function processDatapoints(plot, series, datapoints) {
        if (show) {
            series["xaxis"].options.ticks = boxWhiskerTickGenerator;
        }
    }

    function checkBoxWhiskerEnabled(plot, options) {
        if (options.series.boxwhisker.show) {
            show = true;
        }
    }

    function drawSeries(plot, ctx, series) {
      if (show) {
        var pts = series.datapoints.points;
        var offset = plot.getPlotOffset();
        var s_x = series.xaxis.p2c(pts[0]);
        var s_w = series.xaxis.p2c(series.boxwhisker.boxWidth / 2) - series.xaxis.p2c(0);
        var s_min = series.yaxis.p2c(pts[1]);
        var s_lq = series.yaxis.p2c(pts[3]);
        var s_med = series.yaxis.p2c(pts[5]);
        var s_uq = series.yaxis.p2c(pts[7]);
        var s_max = series.yaxis.p2c(pts[9]);
        ctx.save()
        ctx.translate(offset.left, offset.top)

        function drawBoxWhisker(lineColor, fillColor, s_x, s_w, s_min, s_lq, s_med, s_uq, s_max) {
            ctx.strokeStyle = lineColor;
            ctx.fillStyle = fillColor;
            ctx.fillRect(s_x - s_w, s_lq, s_w * 2, s_uq - s_lq);
            ctx.beginPath();
            ctx.strokeRect(s_x - s_w, s_lq, s_w * 2, s_uq - s_lq);

            ctx.beginPath();
            ctx.moveTo(s_x - s_w, s_min);
            ctx.lineTo(s_x + s_w, s_min);

            ctx.moveTo(s_x, s_min);
            ctx.lineTo(s_x, s_lq);

            ctx.moveTo(s_x - s_w, s_med);
            ctx.lineTo(s_x + s_w, s_med);

            ctx.moveTo(s_x, s_uq);
            ctx.lineTo(s_x, s_max);

            ctx.moveTo(s_x - s_w, s_max);
            ctx.lineTo(s_x + s_w, s_max);
            ctx.stroke();
        }
        var shadowOffset = (series.shadowSize != null) ? series.shadowSize : 0;
        if(shadowOffset > 0) {
            shadowColor = "rgba(0,0,0,0.1)";
            ctx.lineWidth = series.lineWidth * 2;
            drawBoxWhisker(shadowColor, shadowColor, s_x+shadowOffset, s_w, s_min+shadowOffset, s_lq+shadowOffset, s_med+shadowOffset, s_uq+shadowOffset, s_max+shadowOffset);
            ctx.lineWidth = series.lineWidth;
            drawBoxWhisker(shadowColor, shadowColor, s_x+shadowOffset, s_w, s_min+shadowOffset, s_lq+shadowOffset, s_med+shadowOffset, s_uq+shadowOffset, s_max+shadowOffset);
        }
        var col = series.color;
        if (!series.boxwhisker.useColor)
        {
            col = "rgba(255,255,255,1)";
        }
        ctx.lineWidth = series.lineWidth;
        drawBoxWhisker("rgba(0,0,0,1)", col, s_x, s_w, s_min, s_lq, s_med, s_uq, s_max);

        ctx.restore()
      }
    }
    
    plot.hooks.processOptions.push(checkBoxWhiskerEnabled);
    plot.hooks.processRawData.push(processRawData);
    plot.hooks.processDatapoints.push(processDatapoints);
    plot.hooks.drawSeries.push(drawSeries);
  }

  var options = {
    series: {
      boxwhisker: {
        show: false,
        useColor: true,
        boxWidth: 0.4,
        lineWidth: 0.5
      }
    }
  };	

  $.plot.plugins.push({
      init: init,
      options: options,
      name: "boxwhisker",
      version: "0.1"
  });
})(jQuery);

