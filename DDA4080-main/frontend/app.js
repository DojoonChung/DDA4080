let windMode = false;
let tick = 0;
let timer = null;
let isPaused = false;

const historyStore = {
  time: [],
  connectivity: [],
  avgFlow: [],
  crowdedCount: [],
  faultCount: []
};

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`${url} -> ${res.status}`);
  return res.json();
}

function statusColor(status) {
  if (status === "fault") return "#ff1f1f";
  if (status === "crowded") return "#ff8a00";
  if (status === "vulnerable") return "#ffd54f";
  return "#39d66b";
}

function statusSize(status) {
  if (status === "fault") return 18;
  if (status === "crowded") return 14;
  if (status === "vulnerable") return 11;
  return 8;
}

function formatPlaybackTime(data) {
  if (data.playback_time) return data.playback_time;
  if (!data.date || data.slot === undefined || data.slot === null) return "--";

  const slot = Number(data.slot) || 0;
  const hour = 4 + Math.floor(slot / 6);
  const minute = (slot % 6) * 10;
  return `${data.date} ${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function summarizeCurrent(points) {
  const summary = {
    stationCount: points.length,
    crowdedCount: 0,
    vulnerableCount: 0,
    faultCount: 0,
    totalFlowSum: 0,
    predictedFlowSum: 0,
    cascadeFlowSum: 0,
    avgFlow: 0,
    connectivity: 100
  };

  for (const p of points) {
    const status = p.status || "normal";
    if (status === "crowded") summary.crowdedCount += 1;
    if (status === "vulnerable") summary.vulnerableCount += 1;
    if (status === "fault") summary.faultCount += 1;

    summary.totalFlowSum += Number(p.total_flow || 0);
    summary.predictedFlowSum += Number(p.predicted_flow || 0);
    summary.cascadeFlowSum += Number(p.cascade_flow || 0);
  }

  summary.avgFlow = summary.stationCount
    ? Math.round(summary.totalFlowSum / summary.stationCount)
    : 0;

  // 优先使用后端给的真实指标；没有就前端估算
  const maybeConnectivity = Number.isFinite(Number(window.__latestConnectivityFromBackend))
    ? Number(window.__latestConnectivityFromBackend)
    : null;

  if (maybeConnectivity !== null) {
    summary.connectivity = maybeConnectivity;
  } else {
    const estimated =
      100
      - summary.faultCount * 2.0
      - summary.crowdedCount * 0.35
      - summary.vulnerableCount * 0.15;
    summary.connectivity = Number(clamp(estimated, 0, 100).toFixed(1));
  }

  return summary;
}

function pushHistory(timeLabel, summary) {
  historyStore.time.push(timeLabel);
  historyStore.connectivity.push(summary.connectivity);
  historyStore.avgFlow.push(summary.avgFlow);
  historyStore.crowdedCount.push(summary.crowdedCount);
  historyStore.faultCount.push(summary.faultCount);

  const maxLen = 24;
  for (const key of Object.keys(historyStore)) {
    while (historyStore[key].length > maxLen) {
      historyStore[key].shift();
    }
  }
}

function startAutoRefresh() {
  if (timer) clearInterval(timer);
  timer = setInterval(async () => {
    tick += 1;
    await refresh();
  }, 5000);
}

function stopAutoRefresh() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}

function updatePausePlayButtonText() {
  const pausePlayBtn = document.getElementById("pausePlayBtn");
  if (pausePlayBtn) {
    pausePlayBtn.textContent = isPaused ? "播放" : "暂停";
  }
}

async function bootstrapAndRefresh() {
  const kpisEl = document.getElementById("kpis");
  if (kpisEl) {
    kpisEl.innerHTML = `<div class="kpi-card">正在清洗北京OD并训练模型，请稍候…</div>`;
  }

  historyStore.time = [];
  historyStore.connectivity = [];
  historyStore.avgFlow = [];
  historyStore.crowdedCount = [];
  historyStore.faultCount = [];

  await fetchJSON("/api/bootstrap", { method: "POST" });

  tick = 0;
  await refresh();

  if (!isPaused) {
    startAutoRefresh();
  }
  updatePausePlayButtonText();
}

async function refresh() {
  const modelSelect = document.getElementById("modelSelect");
  const model = modelSelect ? modelSelect.value : "lightgbm";
  const data = await fetchJSON(`/api/live?step=${tick}&model=${model}&wind_mode=${windMode}`);
  render(data);
}

function render(data) {
  const current = data.current || [];
  const lineGeometries = data.line_geometries || [];
  const failedSet = new Set((data.cascade?.failed_stations || []).map(x => x.station_key));
  const windSet = new Set((data.wind_markers || []).map(x => x.station_key));
  const playbackLabel = formatPlaybackTime(data);

  window.__latestConnectivityFromBackend =
    data.kpis?.connectivity_index ??
    data.kpis?.connectivity ??
    null;

  const points = current.filter(r => Number.isFinite(r.lon) && Number.isFinite(r.lat));
  const summary = summarizeCurrent(points);
  pushHistory(playbackLabel, summary);

  renderMap(points, lineGeometries, failedSet, windSet);
  renderKpis(data, summary, playbackLabel);
  renderTopStations(data, points);
  renderTrendChart();
  renderFlowChart();
  renderStatusPie(points);
  renderAlerts(data, points);

  const playbackTimeEl = document.getElementById("playbackTime");
  if (playbackTimeEl) {
    playbackTimeEl.textContent = `回放时间：${playbackLabel}`;
  }
}

function renderMap(points, lineGeometries, failedSet, windSet) {
  const traces = [];

  lineGeometries.forEach(line => {
    const rows = (line.stations || []).filter(r => Number.isFinite(r.lon) && Number.isFinite(r.lat));
    if (!rows.length) return;

    traces.push({
      type: "scattermapbox",
      mode: "lines",
      lon: rows.map(r => r.lon),
      lat: rows.map(r => r.lat),
      line: { width: 4, color: line.color || "#6ea8ff" },
      hoverinfo: "skip",
      showlegend: false
    });
  });

  traces.push({
    type: "scattermapbox",
    mode: "markers",
    lon: points.map(r => r.lon),
    lat: points.map(r => r.lat),
    text: points.map(r =>
      `<b>${r.station_name}</b><br>${r.line_name || ""}<br>当前客流: ${r.total_flow || 0}<br>预测客流: ${r.predicted_flow || 0}<br>级联后客流: ${r.cascade_flow || 0}`
    ),
    hoverinfo: "text",
    marker: {
      size: points.map(r => statusSize(r.status)),
      color: points.map(r => statusColor(r.status)),
      opacity: 0.96
    },
    showlegend: false
  });

  if (windMode) {
    const windPoints = points.filter(r => windSet.has(r.station_key));
    traces.push({
      type: "scattermapbox",
      mode: "text",
      lon: windPoints.map(r => r.lon),
      lat: windPoints.map(r => r.lat),
      text: windPoints.map(() => "×"),
      textfont: { size: 22, color: "#ff2020" },
      hoverinfo: "skip",
      showlegend: false
    });
  }

  const failedPoints = points.filter(r => failedSet.has(r.station_key));
  if (failedPoints.length) {
    traces.push({
      type: "scattermapbox",
      mode: "markers",
      lon: failedPoints.map(r => r.lon),
      lat: failedPoints.map(r => r.lat),
      marker: { size: 24, color: "#ff2020", opacity: 0.35 },
      hoverinfo: "skip",
      showlegend: false
    });
  }

  Plotly.newPlot("map", traces, {
    mapbox: {
      style: "carto-darkmatter",
      center: { lon: 116.39, lat: 39.92 },
      zoom: 10.3
    },
    paper_bgcolor: "#0b1628",
    plot_bgcolor: "#0b1628",
    font: { color: "#eef5ff" },
    margin: { l: 0, r: 0, t: 0, b: 0 }
  }, { responsive: true, displayModeBar: false });
}

function sparklineSVG(values, color = "#6bb6ff") {
  if (!values || !values.length) {
    return `<svg class="sparkline" viewBox="0 0 120 32" preserveAspectRatio="none"></svg>`;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const points = values.map((v, i) => {
    const x = (i / Math.max(values.length - 1, 1)) * 120;
    const y = 28 - ((v - min) / range) * 22;
    return `${x},${y}`;
  }).join(" ");

  return `
    <svg class="sparkline" viewBox="0 0 120 32" preserveAspectRatio="none">
      <polyline
        fill="none"
        stroke="${color}"
        stroke-width="2"
        points="${points}"
      />
    </svg>
  `;
}

function renderKpis(data, summary, playbackLabel) {
  const stationCount = data.kpis?.station_count ?? summary.stationCount ?? 0;
  const lineCount = data.kpis?.line_count ?? (data.line_geometries?.length || 0);
  const crowdedCount = data.kpis?.crowded_count ?? summary.crowdedCount;
  const faultCount = data.kpis?.fault_count ?? summary.faultCount;
  const avgFlow = data.kpis?.avg_flow ?? summary.avgFlow;
  const connectivity = data.kpis?.connectivity_index ?? data.kpis?.connectivity ?? summary.connectivity;

  const kpisEl = document.getElementById("kpis");
  if (!kpisEl) return;

  kpisEl.innerHTML = `
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">网络连通指数</div>
        <div class="kpi-main">
          <div class="kpi-value">${Number(connectivity).toFixed(1)}</div>
          <div class="kpi-unit">/100</div>
        </div>
        ${sparklineSVG(historyStore.connectivity, "#00e5ff")}
      </div>

      <div class="kpi-card">
        <div class="kpi-label">平均站点客流</div>
        <div class="kpi-main">
          <div class="kpi-value">${avgFlow}</div>
          <div class="kpi-unit">人次</div>
        </div>
        ${sparklineSVG(historyStore.avgFlow, "#69ff47")}
      </div>

      <div class="kpi-card">
        <div class="kpi-label">拥堵站点数</div>
        <div class="kpi-main">
          <div class="kpi-value bad-orange">${crowdedCount}</div>
          <div class="kpi-unit">个</div>
        </div>
        ${sparklineSVG(historyStore.crowdedCount, "#ff9a2f")}
      </div>

      <div class="kpi-card">
        <div class="kpi-label">严重拥堵站点数</div>
        <div class="kpi-main">
          <div class="kpi-value bad-red">${faultCount}</div>
          <div class="kpi-unit">个</div>
        </div>
        ${sparklineSVG(historyStore.faultCount, "#ff5252")}
      </div>

      <div class="kpi-card">
        <div class="kpi-label">系统概况</div>
        <div class="kpi-main">
          <div class="kpi-value">${stationCount}/${lineCount}</div>
          <div class="kpi-unit">站/线</div>
        </div>
        <div style="margin-top:8px;font-size:11px;color:#9ebff0;">
          模型：${data.prediction?.model || "-"}<br>
          时间：${playbackLabel}
        </div>
      </div>
    </div>
  `;
}

function renderTopStations(data, points) {
  const topStationsEl = document.getElementById("topStations");
  if (!topStationsEl) return;

  let risks = data.risk_top || [];

  if (!risks.length) {
    risks = [...points]
      .sort((a, b) => (Number(b.cascade_flow || b.predicted_flow || 0) - Number(a.cascade_flow || a.predicted_flow || 0)))
      .slice(0, 5);
  } else {
    risks = risks.slice(0, 5);
  }

  if (!risks.length) {
    topStationsEl.innerHTML = `<div class="top-station-item">暂无风险站点数据</div>`;
    return;
  }

  topStationsEl.innerHTML = risks.map((r, idx) => `
    <div class="top-station-item">
      <div class="station-row1">
        <span class="station-rank">#${idx + 1}</span>
        <span class="station-name">${r.station_name || "-"}</span>
      </div>
      <div class="station-line">${r.line_name || "未知线路"}</div>
      <div class="station-metrics">
        <span>当前 ${r.total_flow || 0}</span>
        <span>预测 ${r.predicted_flow || 0}</span>
        <span class="${r.status === "fault" ? "bad-red" : "bad-orange"}">级联 ${r.cascade_flow || 0}</span>
      </div>
    </div>
  `).join("");
}

function renderTrendChart() {
  Plotly.newPlot("trendChart", [
    {
      x: historyStore.time,
      y: historyStore.connectivity,
      type: "scatter",
      mode: "lines",
      name: "连通指数",
      line: { width: 2.4, color: "#00e5ff" },
      fill: "tozeroy",
      fillcolor: "rgba(0,229,255,0.08)"
    },
    {
      x: historyStore.time,
      y: historyStore.faultCount,
      type: "scatter",
      mode: "lines",
      name: "严重拥堵站点",
      yaxis: "y2",
      line: { width: 2.1, color: "#ff5252" }
    }
  ], {
    paper_bgcolor: "#0b1628",
    plot_bgcolor: "#0f1f35",
    font: { color: "#eef5ff", size: 11 },
    margin: { l: 36, r: 36, t: 12, b: 28 },
    height: 210,
    legend: {
      orientation: "h",
      x: 0,
      y: 1.15,
      font: { size: 10 }
    },
    xaxis: {
      tickfont: { size: 9 },
      gridcolor: "rgba(160,190,230,0.10)"
    },
    yaxis: {
      title: "",
      gridcolor: "rgba(160,190,230,0.10)",
      range: [0, 100]
    },
    yaxis2: {
      overlaying: "y",
      side: "right",
      showgrid: false
    }
  }, { responsive: true, displayModeBar: false });
}

function renderFlowChart() {
  Plotly.newPlot("flowChart", [
    {
      x: historyStore.time,
      y: historyStore.avgFlow,
      type: "scatter",
      mode: "lines+markers",
      name: "平均客流",
      line: { width: 2.4, color: "#69ff47" },
      marker: { size: 4 }
    },
    {
      x: historyStore.time,
      y: historyStore.crowdedCount,
      type: "bar",
      name: "拥堵站点",
      yaxis: "y2",
      opacity: 0.35,
      marker: { color: "#ff9a2f" }
    }
  ], {
    paper_bgcolor: "#0b1628",
    plot_bgcolor: "#0f1f35",
    font: { color: "#eef5ff", size: 11 },
    margin: { l: 36, r: 36, t: 12, b: 28 },
    height: 210,
    legend: {
      orientation: "h",
      x: 0,
      y: 1.15,
      font: { size: 10 }
    },
    xaxis: {
      tickfont: { size: 9 },
      gridcolor: "rgba(160,190,230,0.10)"
    },
    yaxis: {
      gridcolor: "rgba(160,190,230,0.10)"
    },
    yaxis2: {
      overlaying: "y",
      side: "right",
      showgrid: false
    },
    barmode: "overlay"
  }, { responsive: true, displayModeBar: false });
}

function renderStatusPie(points) {
  const stats = { normal: 0, crowded: 0, vulnerable: 0, fault: 0 };

  points.forEach(r => {
    const s = r.status || "normal";
    if (stats[s] !== undefined) stats[s] += 1;
  });

  Plotly.newPlot("statusPie", [{
    type: "pie",
    labels: ["正常", "拥堵", "脆弱", "严重拥堵"],
    values: [stats.normal, stats.crowded, stats.vulnerable, stats.fault],
    hole: 0.58,
    textinfo: "label+percent",
    marker: {
      colors: ["#39d66b", "#ff8a00", "#ffd54f", "#ff1f1f"]
    }
  }], {
    paper_bgcolor: "#0b1628",
    font: { color: "#eef5ff", size: 11 },
    margin: { l: 10, r: 10, t: 8, b: 8 },
    height: 220,
    showlegend: false
  }, { responsive: true, displayModeBar: false });
}

function renderAlerts(data, points) {
  const alertListEl = document.getElementById("alertList");
  if (!alertListEl) return;

  const faultPoints = points.filter(p => p.status === "fault").slice(0, 3);
  const waveAlerts = (data.cascade?.waves || []).slice(0, 2);

  const blocks = [];

  faultPoints.forEach(p => {
    blocks.push(`
      <div class="alert-item">
        <div class="alert-title-line">⚠ ${p.station_name}</div>
        <div class="alert-sub">
          ${p.line_name || "未知线路"} ｜ 当前 ${p.total_flow || 0} ｜ 级联后 ${p.cascade_flow || 0}
        </div>
      </div>
    `);
  });

  waveAlerts.forEach(w => {
    blocks.push(`
      <div class="alert-item">
        <div class="alert-title-line">⚠ 第 ${w.wave} 波传播</div>
        <div class="alert-sub">${(w.stations || []).length} 个站点被带动拥堵</div>
      </div>
    `);
  });

  if (!blocks.length) {
    blocks.push(`
      <div class="alert-item good">
        <div class="alert-title-line" style="color:#5de38d;">✓ 当前无重点告警</div>
        <div class="alert-sub" style="color:#9ce7b7;">未发现明显拥堵传播</div>
      </div>
    `);
  }

  alertListEl.innerHTML = blocks.join("");
}

const cleanBtn = document.getElementById("cleanBtn");
if (cleanBtn) {
  cleanBtn.textContent = "数据清洗+训练";
  cleanBtn.addEventListener("click", bootstrapAndRefresh);
}

const refreshBtn = document.getElementById("refreshBtn");
if (refreshBtn) {
  refreshBtn.addEventListener("click", async () => {
    tick += 1;
    await refresh();
  });
}

const pausePlayBtn = document.getElementById("pausePlayBtn");
if (pausePlayBtn) {
  pausePlayBtn.addEventListener("click", () => {
    isPaused = !isPaused;
    updatePausePlayButtonText();

    if (isPaused) {
      stopAutoRefresh();
    } else {
      startAutoRefresh();
    }
  });
}

const modelSelect = document.getElementById("modelSelect");
if (modelSelect) {
  modelSelect.addEventListener("change", async () => {
    tick = 0;
    historyStore.time = [];
    historyStore.connectivity = [];
    historyStore.avgFlow = [];
    historyStore.crowdedCount = [];
    historyStore.faultCount = [];

    await refresh();

    if (!isPaused) {
      stopAutoRefresh();
      startAutoRefresh();
    }
  });
}

const windBtn = document.getElementById("windBtn");
if (windBtn) {
  windBtn.addEventListener("click", async () => {
    windMode = !windMode;
    windBtn.textContent = `大风模式：${windMode ? "开" : "关"}`;
    await refresh();
  });
}

bootstrapAndRefresh().catch(err => {
  console.error(err);
  const kpisEl = document.getElementById("kpis");
  if (kpisEl) {
    kpisEl.innerHTML = `<div class="kpi-card">后端返回异常：${err.message}</div>`;
  }
});