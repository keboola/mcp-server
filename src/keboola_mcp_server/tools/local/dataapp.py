"""Local data app: configuration-driven HTML dashboards backed by pre-computed DuckDB queries."""

import json
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class DataAppChartConfig(BaseModel):
    id: str = Field(description='Unique chart identifier (slug, no spaces).')
    title: str = Field(description='Chart display title.')
    sql: str = Field(description='DuckDB SQL query that produces the chart data.')
    type: Literal['bar', 'line', 'scatter', 'pie', 'table', 'heatmap'] = Field(description='Visualization type.')
    x_column: str | None = Field(default=None, description='Column for X axis or pie labels. Defaults to first column.')
    y_column: str | None = Field(
        default=None, description='Column for Y axis or pie values. Defaults to second column.'
    )
    color_column: str | None = Field(default=None, description='Column for color grouping (scatter/bar/line).')


class DataAppConfig(BaseModel):
    name: str = Field(description='App identifier used as directory name (lowercase, hyphens).')
    title: str = Field(description='Dashboard display title.')
    description: str = Field(default='', description='Short dashboard description shown as subtitle.')
    tables: list[str] = Field(description='Table names (CSV stems) the charts query.')
    charts: list[DataAppChartConfig] = Field(description='Ordered list of chart definitions.')
    created_at: str = Field(default='', description='ISO 8601 creation timestamp.')
    updated_at: str = Field(default='', description='ISO 8601 last-update timestamp.')


class DataAppInfo(BaseModel):
    name: str = Field(description='App identifier.')
    title: str = Field(description='Dashboard title.')
    description: str = Field(description='Dashboard description.')
    chart_count: int = Field(description='Number of charts in the dashboard.')
    status: Literal['running', 'stopped'] = Field(description='Whether an HTTP server is active.')
    app_url: str | None = Field(default=None, description='URL if running, None otherwise.')
    port: int | None = Field(default=None, description='Port if running, None otherwise.')


class DataAppsOutput(BaseModel):
    apps: list[DataAppInfo] = Field(description='All local data apps.')
    total: int = Field(description='Total number of apps.')


class DataAppRunResult(BaseModel):
    name: str = Field(description='App name.')
    status: Literal['started', 'already_running', 'error'] = Field(description='Outcome.')
    app_url: str | None = Field(default=None, description='URL to open in the browser.')
    port: int | None = Field(default=None, description='Local port.')
    message: str | None = Field(default=None, description='Additional message or error details.')


class DataAppStopResult(BaseModel):
    name: str = Field(description='App name.')
    stopped: bool = Field(description='True if a running server was stopped.')
    message: str = Field(description='Human-readable outcome.')


# ---------------------------------------------------------------------------
# HTML template — single-file dashboard with ECharts + Pico CSS
# All dynamic content is embedded as JSON (never injected into HTML context).
# innerHTML is only used with static string literals in the template.
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>__TITLE__</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.classless.min.css">
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
:root { color-scheme: light dark; }
body { padding: 0; }
main { max-width: 1400px; margin: 0 auto; padding: 1.5rem; }
header { margin-bottom: 1.5rem; border-bottom: 1px solid var(--pico-muted-border-color); padding-bottom: 1rem; }
header h1 { margin-bottom: 0.25rem; }
header p { margin: 0; color: var(--pico-muted-color); }
.status-bar { font-size: 0.8rem; color: var(--pico-muted-color); margin-top: 0.5rem; min-height: 1.2em; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 550px), 1fr)); gap: 1.25rem; }
.card {
  background: var(--pico-card-background-color);
  border: 1px solid var(--pico-card-border-color);
  border-radius: var(--pico-border-radius);
  padding: 1.25rem 1.25rem 1rem;
}
.card h3 { margin: 0 0 0.75rem; font-size: 1rem; font-weight: 600; }
.chart-box { height: 320px; position: relative; }
.loading { display: flex; align-items: center; justify-content: center; height: 100%;
  color: var(--pico-muted-color); font-size: 0.875rem; gap: 0.5rem; }
.error-msg { color: #c0392b; font-size: 0.85rem; padding: 0.5rem 0; }
.table-wrap { overflow: auto; max-height: 320px; }
.table-wrap table { font-size: 0.8rem; margin: 0; }
.table-wrap th { position: sticky; top: 0; background: var(--pico-card-background-color); }
@keyframes spin { to { transform: rotate(360deg); } }
.spinner { width: 18px; height: 18px; border: 2px solid var(--pico-muted-border-color);
  border-top-color: var(--pico-primary); border-radius: 50%; animation: spin 0.7s linear infinite; }
</style>
</head>
<body>
<main>
  <header>
    <h1 id="app-title"></h1>
    <p id="app-desc"></p>
    <div class="status-bar" id="status"></div>
  </header>
  <div class="grid" id="grid"></div>
</main>

<script id="app-config" type="application/json">__CONFIG_JSON__</script>
<script id="chart-data" type="application/json">__DATA_JSON__</script>
<script>
(function () {
  var APP_CONFIG = JSON.parse(document.getElementById('app-config').textContent);
  var CHART_DATA = JSON.parse(document.getElementById('chart-data').textContent);

  var PALETTE = [
    '#5470c6','#91cc75','#fac858','#ee6666','#73c0de',
    '#3ba272','#fc8452','#9a60b4','#ea7ccc','#37a2da'
  ];

  document.getElementById('app-title').textContent = APP_CONFIG.title || '';
  document.getElementById('app-desc').textContent = APP_CONFIG.description || '';

  function status(msg) {
    document.getElementById('status').textContent = msg;
  }

  function makeCard(chart) {
    var card = document.createElement('div');
    card.className = 'card';

    var h3 = document.createElement('h3');
    h3.textContent = chart.title;
    card.appendChild(h3);

    var box = document.createElement('div');
    box.className = 'chart-box';
    box.id = 'box-' + chart.id;

    var loading = document.createElement('div');
    loading.className = 'loading';
    var spinner = document.createElement('div');
    spinner.className = 'spinner';
    var label = document.createElement('span');
    label.textContent = 'Loading\u2026';
    loading.appendChild(spinner);
    loading.appendChild(label);
    box.appendChild(loading);
    card.appendChild(box);
    return card;
  }

  function setError(id, msg) {
    var box = document.getElementById('box-' + id);
    if (!box) return;
    while (box.firstChild) box.removeChild(box.firstChild);
    var div = document.createElement('div');
    div.className = 'error-msg';
    div.textContent = '\u26a0 ' + msg;
    box.appendChild(div);
  }

  function clearBox(id) {
    var box = document.getElementById('box-' + id);
    if (box) while (box.firstChild) box.removeChild(box.firstChild);
    return box;
  }

  function renderPie(box, chart, data) {
    var xc = chart.x_column || data.columns[0];
    var yc = chart.y_column || data.columns[1] || data.columns[0];
    var ec = echarts.init(box);
    ec.setOption({
      tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      legend: {
        orient: 'vertical', left: '5%', top: 'center',
        textStyle: { overflow: 'truncate', width: 120 }
      },
      series: [{
        type: 'pie', radius: ['38%', '68%'], center: ['65%', '50%'],
        data: data.rows.map(function(r) { return { name: String(r[xc]), value: Number(r[yc]) }; }),
        label: { formatter: '{b}\n{d}%' },
        emphasis: { itemStyle: { shadowBlur: 8, shadowColor: 'rgba(0,0,0,0.3)' } }
      }]
    });
    return ec;
  }

  function renderBar(box, chart, data) {
    var xc = chart.x_column || data.columns[0];
    var ycols = chart.y_column ? [chart.y_column] : data.columns.slice(1);
    var ec = echarts.init(box);
    ec.setOption({
      tooltip: { trigger: 'axis' },
      legend: { data: ycols, bottom: 0 },
      grid: { left: '3%', right: '3%', bottom: ycols.length > 1 ? '12%' : '8%', containLabel: true },
      xAxis: {
        type: 'category',
        data: data.rows.map(function(r) { return String(r[xc]); }),
        axisLabel: { rotate: data.rows.length > 8 ? 30 : 0, overflow: 'truncate', width: 80 }
      },
      yAxis: { type: 'value' },
      series: ycols.map(function(col, i) {
        return {
          name: col, type: 'bar',
          data: data.rows.map(function(r) { return Number(r[col]); }),
          itemStyle: { color: PALETTE[i % PALETTE.length] }
        };
      })
    });
    return ec;
  }

  function renderLine(box, chart, data) {
    var xc = chart.x_column || data.columns[0];
    var ycols = chart.y_column ? [chart.y_column] : data.columns.slice(1);
    var ec = echarts.init(box);
    ec.setOption({
      tooltip: { trigger: 'axis' },
      legend: { data: ycols, bottom: 0 },
      grid: { left: '3%', right: '3%', bottom: '8%', containLabel: true },
      xAxis: {
        type: 'category',
        data: data.rows.map(function(r) { return String(r[xc]); }),
        boundaryGap: false
      },
      yAxis: { type: 'value' },
      series: ycols.map(function(col, i) {
        return {
          name: col, type: 'line', smooth: true,
          data: data.rows.map(function(r) { return Number(r[col]); }),
          itemStyle: { color: PALETTE[i % PALETTE.length] },
          areaStyle: { opacity: 0.08 }
        };
      })
    });
    return ec;
  }

  function renderScatter(box, chart, data) {
    var xc = chart.x_column || data.columns[0];
    var yc = chart.y_column || data.columns[1];
    var cc = chart.color_column;
    var ec = echarts.init(box);
    var series;
    if (cc) {
      var groups = {};
      data.rows.forEach(function(r) {
        var k = String(r[cc]);
        if (!groups[k]) groups[k] = [];
        groups[k].push([Number(r[xc]), Number(r[yc])]);
      });
      var keys = Object.keys(groups);
      series = keys.map(function(k, i) {
        return {
          name: k, type: 'scatter', data: groups[k], symbolSize: 7,
          itemStyle: { color: PALETTE[i % PALETTE.length], opacity: 0.8 }
        };
      });
    } else {
      series = [{
        type: 'scatter',
        data: data.rows.map(function(r) { return [Number(r[xc]), Number(r[yc])]; }),
        symbolSize: 7
      }];
    }
    ec.setOption({
      tooltip: {
        trigger: 'item',
        formatter: function(p) {
          return (cc ? p.seriesName + '<br/>' : '') + xc + ': ' + p.value[0] + '<br/>' + yc + ': ' + p.value[1];
        }
      },
      legend: cc ? { bottom: 0 } : undefined,
      grid: { left: '3%', right: '3%', bottom: cc ? '12%' : '8%', containLabel: true },
      xAxis: { name: xc, type: 'value', scale: true },
      yAxis: { name: yc, type: 'value', scale: true },
      series: series
    });
    return ec;
  }

  function renderHeatmap(box, chart, data) {
    var xc = chart.x_column || data.columns[0];
    var yc = chart.y_column || data.columns[1];
    var vc = data.columns[2] || data.columns[1];
    var xs = [], ys = [];
    data.rows.forEach(function(r) {
      var xv = String(r[xc]), yv = String(r[yc]);
      if (xs.indexOf(xv) < 0) xs.push(xv);
      if (ys.indexOf(yv) < 0) ys.push(yv);
    });
    var vals = data.rows.map(function(r) {
      return [xs.indexOf(String(r[xc])), ys.indexOf(String(r[yc])), Number(r[vc])];
    });
    var mx = Math.max.apply(null, vals.map(function(v) { return v[2]; })) || 1;
    var ec = echarts.init(box);
    ec.setOption({
      tooltip: { position: 'top' },
      grid: { left: '3%', right: '3%', bottom: '15%', containLabel: true },
      xAxis: { type: 'category', data: xs },
      yAxis: { type: 'category', data: ys },
      visualMap: { min: 0, max: mx, calculable: true, orient: 'horizontal', left: 'center', bottom: '2%' },
      series: [{ type: 'heatmap', data: vals, label: { show: xs.length <= 10 } }]
    });
    return ec;
  }

  function renderTable(box, chart, data) {
    var wrap = document.createElement('div');
    wrap.className = 'table-wrap';
    var tbl = document.createElement('table');
    var thead = document.createElement('thead');
    var hrow = document.createElement('tr');
    data.columns.forEach(function(c) {
      var th = document.createElement('th');
      th.textContent = c;
      hrow.appendChild(th);
    });
    thead.appendChild(hrow);
    tbl.appendChild(thead);
    var tbody = document.createElement('tbody');
    data.rows.forEach(function(r) {
      var tr = document.createElement('tr');
      data.columns.forEach(function(c) {
        var td = document.createElement('td');
        td.textContent = r[c] === null ? '' : String(r[c]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    tbl.appendChild(tbody);
    wrap.appendChild(tbl);
    box.appendChild(wrap);
  }

  function renderChart(chart) {
    var data = CHART_DATA[chart.id];
    if (!data) { setError(chart.id, 'No data for chart "' + chart.id + '"'); return; }
    if (data.error) { setError(chart.id, data.error); return; }
    var box = clearBox(chart.id);
    if (!box) return;
    try {
      switch (chart.type) {
        case 'pie':     renderPie(box, chart, data);     break;
        case 'bar':     renderBar(box, chart, data);     break;
        case 'line':    renderLine(box, chart, data);    break;
        case 'scatter': renderScatter(box, chart, data); break;
        case 'heatmap': renderHeatmap(box, chart, data); break;
        case 'table':   renderTable(box, chart, data);   break;
        default:        setError(chart.id, 'Unknown chart type: ' + chart.type);
      }
    } catch (e) {
      setError(chart.id, e.message);
    }
  }

  function init() {
    var grid = document.getElementById('grid');
    APP_CONFIG.charts.forEach(function(chart) { grid.appendChild(makeCard(chart)); });
    APP_CONFIG.charts.forEach(function(chart) { renderChart(chart); });

    var errors = APP_CONFIG.charts.filter(function(c) {
      return CHART_DATA[c.id] && CHART_DATA[c.id].error;
    }).length;
    var ok = APP_CONFIG.charts.length - errors;
    var tables = (APP_CONFIG.tables || []).join(', ') || 'none';
    status('Tables: ' + tables + ' \u2022 ' + ok + '/' + APP_CONFIG.charts.length + ' charts rendered'
      + (errors ? ' (' + errors + ' error' + (errors > 1 ? 's' : '') + ')' : ''));

    window.addEventListener('resize', function() {
      document.querySelectorAll('.chart-box').forEach(function(box) {
        var ec = echarts.getInstanceByDom(box);
        if (ec) ec.resize();
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
</script>
</body>
</html>
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_dashboard_html(config: DataAppConfig, chart_data: dict) -> str:
    """Generate a self-contained HTML dashboard.

    :param config: App configuration (title, description, charts).
    :param chart_data: Mapping of chart_id to {columns, rows} or {error} dicts.
    :returns: Complete HTML string ready to write to disk.
    """
    config_dict = config.model_dump()
    # Escape </script> to prevent early tag termination inside the JSON script block.
    config_json = json.dumps(config_dict, ensure_ascii=False).replace('</', '<\\/')
    data_json = json.dumps(chart_data, ensure_ascii=False).replace('</', '<\\/')

    return (
        _HTML_TEMPLATE.replace('__TITLE__', config.title)
        .replace('__CONFIG_JSON__', config_json)
        .replace('__DATA_JSON__', data_json)
    )
