"""Local data app: configuration-driven HTML dashboards with live DuckDB queries."""

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
# HTML template — live-query dashboard.
#
# Charts fetch data on load and on each Refresh click by calling the local
# Query Service emulator (appserver.py) at window.location.origin.
#
# The emulator implements the same HTTP API contract as the production Query
# Service (https://query.keboola.com), so migrating to production means only
# changing the base_url + token in the embedded config — no JS rewrite needed.
#
# API calls made by this template (matching the production contract):
#   POST {base_url}/api/v1/branches/{branchId}/workspaces/{workspaceId}/queries
#   GET  {base_url}/api/v1/queries/{jobId}
#   GET  {base_url}/api/v1/queries/{jobId}/{stmtId}/results
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
.refresh-btn { margin-top: 0.5rem; font-size: 0.8rem; padding: 0.25rem 0.75rem; cursor: pointer; }
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
    <button class="refresh-btn" id="refresh-btn">&#8635; Refresh</button>
  </header>
  <div class="grid" id="grid"></div>
</main>

<script id="app-config" type="application/json">__CONFIG_JSON__</script>
<script>
(function () {
  'use strict';

  var APP_CONFIG = JSON.parse(document.getElementById('app-config').textContent);
  var QS = APP_CONFIG.queryService;  // {token, branchId, workspaceId}
  // base_url auto-detected from browser URL — works for any port
  var BASE_URL = window.location.origin;

  var PALETTE = [
    '#5470c6','#91cc75','#fac858','#ee6666','#73c0de',
    '#3ba272','#fc8452','#9a60b4','#ea7ccc','#37a2da'
  ];

  document.getElementById('app-title').textContent = APP_CONFIG.title || '';
  document.getElementById('app-desc').textContent = APP_CONFIG.description || '';

  // -----------------------------------------------------------------------
  // Query Service client — calls the same HTTP API as production
  // POST /api/v1/branches/{branchId}/workspaces/{workspaceId}/queries
  // GET  /api/v1/queries/{jobId}
  // GET  /api/v1/queries/{jobId}/{stmtId}/results
  // -----------------------------------------------------------------------

  function qs_headers() {
    return { 'Content-Type': 'application/json', 'X-StorageAPI-Token': QS.token };
  }

  async function qs_execute(sql) {
    var submitUrl = BASE_URL + '/api/v1/branches/' + QS.branchId + '/workspaces/' + QS.workspaceId + '/queries';
    var submitRes = await fetch(submitUrl, {
      method: 'POST',
      headers: qs_headers(),
      body: JSON.stringify({ statements: [sql], transactional: false, actorType: 'user' })
    });
    if (!submitRes.ok) throw new Error('query submit failed: ' + submitRes.status);
    var submitBody = await submitRes.json();
    var jobId = submitBody.queryJobId;

    // Poll until completed (emulator returns completed immediately)
    var statusUrl = BASE_URL + '/api/v1/queries/' + jobId;
    var stmts;
    for (var i = 0; i < 30; i++) {
      var statusRes = await fetch(statusUrl, { headers: qs_headers() });
      if (!statusRes.ok) throw new Error('status poll failed: ' + statusRes.status);
      var statusBody = await statusRes.json();
      if (statusBody.status === 'completed' || statusBody.status === 'failed') {
        stmts = statusBody.statements || [];
        break;
      }
      await new Promise(function(r) { setTimeout(r, 200); });
    }
    if (!stmts) throw new Error('query timed out');

    var stmt = stmts[0];
    if (!stmt) throw new Error('no statement in response');
    if (stmt.status === 'error') throw new Error(stmt.error || 'statement error');

    var resultsUrl = BASE_URL + '/api/v1/queries/' + jobId + '/' + stmt.id + '/results?offset=0&pageSize=1000';
    var resultsRes = await fetch(resultsUrl, { headers: qs_headers() });
    if (!resultsRes.ok) throw new Error('results fetch failed: ' + resultsRes.status);
    var body = await resultsRes.json();

    // Normalize to {columns: [str], rows: [{col: val}]}
    var colNames = (body.columns || []).map(function(c) { return c.name || c; });
    var rows = (body.data || []).map(function(row) {
      var obj = {};
      colNames.forEach(function(c, i) { obj[c] = row[i]; });
      return obj;
    });
    return { columns: colNames, rows: rows };
  }

  // -----------------------------------------------------------------------
  // Chart rendering (ECharts)
  // -----------------------------------------------------------------------

  function status(msg) { document.getElementById('status').textContent = msg; }

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

  function disposeBox(box) {
    var ec = echarts.getInstanceByDom(box);
    if (ec) ec.dispose();
    while (box.firstChild) box.removeChild(box.firstChild);
  }

  function setLoading(id) {
    var box = document.getElementById('box-' + id);
    if (!box) return;
    disposeBox(box);
    var loading = document.createElement('div');
    loading.className = 'loading';
    var spinner = document.createElement('div');
    spinner.className = 'spinner';
    var label = document.createElement('span');
    label.textContent = 'Loading\u2026';
    loading.appendChild(spinner);
    loading.appendChild(label);
    box.appendChild(loading);
  }

  function setError(id, msg) {
    var box = document.getElementById('box-' + id);
    if (!box) return;
    disposeBox(box);
    var div = document.createElement('div');
    div.className = 'error-msg';
    div.textContent = '\u26a0 ' + msg;
    box.appendChild(div);
  }

  function clearBox(id) {
    var box = document.getElementById('box-' + id);
    if (box) disposeBox(box);
    return box;
  }

  function renderPie(box, chart, data) {
    var xc = chart.x_column || data.columns[0];
    var yc = chart.y_column || data.columns[1] || data.columns[0];
    var ec = echarts.init(box);
    ec.setOption({
      tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
      legend: { orient: 'vertical', left: '5%', top: 'center',
        textStyle: { overflow: 'truncate', width: 120 } },
      series: [{
        type: 'pie', radius: ['38%', '68%'], center: ['65%', '50%'],
        data: data.rows.map(function(r) { return { name: String(r[xc]), value: Number(r[yc]) }; }),
        label: { formatter: '{b}\\n{d}%' },
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
      xAxis: { type: 'category',
        data: data.rows.map(function(r) { return String(r[xc]); }),
        axisLabel: { rotate: data.rows.length > 8 ? 30 : 0, overflow: 'truncate', width: 80 } },
      yAxis: { type: 'value' },
      series: ycols.map(function(col, i) {
        return { name: col, type: 'bar',
          data: data.rows.map(function(r) { return Number(r[col]); }),
          itemStyle: { color: PALETTE[i % PALETTE.length] } };
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
      xAxis: { type: 'category',
        data: data.rows.map(function(r) { return String(r[xc]); }), boundaryGap: false },
      yAxis: { type: 'value' },
      series: ycols.map(function(col, i) {
        return { name: col, type: 'line', smooth: true,
          data: data.rows.map(function(r) { return Number(r[col]); }),
          itemStyle: { color: PALETTE[i % PALETTE.length] }, areaStyle: { opacity: 0.08 } };
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
      series = Object.keys(groups).map(function(k, i) {
        return { name: k, type: 'scatter', data: groups[k], symbolSize: 7,
          itemStyle: { color: PALETTE[i % PALETTE.length], opacity: 0.8 } };
      });
    } else {
      series = [{ type: 'scatter',
        data: data.rows.map(function(r) { return [Number(r[xc]), Number(r[yc])]; }),
        symbolSize: 7 }];
    }
    ec.setOption({
      tooltip: { trigger: 'item', formatter: function(p) {
        return (cc ? p.seriesName + '<br\\/>' : '') + xc + ': ' + p.value[0] + '<br\\/>' + yc + ': ' + p.value[1];
      }},
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

  function renderChart(chart, data) {
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

  async function loadAll() {
    document.getElementById('refresh-btn').disabled = true;
    status('Loading\u2026');
    var errors = 0;
    var promises = APP_CONFIG.charts.map(async function(chart) {
      setLoading(chart.id);
      try {
        var data = await qs_execute(chart.sql);
        renderChart(chart, data);
      } catch (e) {
        setError(chart.id, e.message);
        errors++;
      }
    });
    await Promise.all(promises);
    var tables = (APP_CONFIG.tables || []).join(', ') || 'none';
    var ok = APP_CONFIG.charts.length - errors;
    status('Tables: ' + tables + ' \u2022 ' + ok + '/' + APP_CONFIG.charts.length + ' charts'
      + (errors ? ' (' + errors + ' error' + (errors > 1 ? 's' : '') + ')' : ''));
    document.getElementById('refresh-btn').disabled = false;
  }

  function init() {
    var grid = document.getElementById('grid');
    APP_CONFIG.charts.forEach(function(chart) { grid.appendChild(makeCard(chart)); });

    document.getElementById('refresh-btn').addEventListener('click', loadAll);

    window.addEventListener('resize', function() {
      document.querySelectorAll('.chart-box').forEach(function(box) {
        var ec = echarts.getInstanceByDom(box);
        if (ec) ec.resize();
      });
    });

    loadAll();
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


def generate_dashboard_html(config: DataAppConfig) -> str:
    """Generate a self-contained HTML dashboard with live DuckDB queries.

    Charts load data at runtime by calling the local Query Service emulator
    (appserver.py) at window.location.origin. The same HTML works with the
    production Query Service when base_url/token/branchId/workspaceId are
    updated in the embedded app-config.

    :param config: App configuration (title, description, charts, queryService).
    :returns: Complete HTML string ready to write to disk.
    """
    qs_config = {
        'token': 'local',
        'branchId': 'local',
        'workspaceId': 'local',
    }
    config_dict = config.model_dump()
    config_dict['queryService'] = qs_config
    config_json = json.dumps(config_dict, ensure_ascii=False).replace('</', '<\\/')

    return _HTML_TEMPLATE.replace('__TITLE__', config.title).replace('__CONFIG_JSON__', config_json)
