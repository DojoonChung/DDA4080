let windMode = false;
let tick = 0;
let timer = null; // 用于存储 setInterval 的引用
let isPaused = false; // 新增：播放/暂停状态

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

// 新增：启动自动刷新的函数
function startAutoRefresh() {
  if (timer) clearInterval(timer); // 先清除任何现有的计时器
  timer = setInterval(async () => {
    tick += 1;
    await refresh();
  }, 5000); // 刷新频率，单位毫秒
}

// 新增：停止自动刷新的函数
function stopAutoRefresh() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}

// 新增：更新暂停/播放按钮文本的函数
function updatePausePlayButtonText() {
  const pausePlayBtn = document.getElementById("pausePlayBtn");
  if (pausePlayBtn) {
    pausePlayBtn.textContent = isPaused ? "播放" : "暂停";
  }
}

async function bootstrapAndRefresh() {
  const kpisEl = document.getElementById("kpis");
  if (kpisEl) {
    kpisEl.innerHTML = `<div class="card">正在清洗北京OD并训练模型，请稍候…</div>`;
  }

  await fetchJSON("/api/bootstrap", { method: "POST" });

  tick = 0; // 启动时重置时间片计数
  await refresh(); // 立即刷新一次

  // 只有在非暂停状态下才启动自动刷新
  if (!isPaused) {
    startAutoRefresh();
  }
  updatePausePlayButtonText(); // 确保按钮文本初始状态正确
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
  const traces = [];

  const playbackTimeEl = document.getElementById("playbackTime");
  if (playbackTimeEl) {
    playbackTimeEl.textContent = `回放时间：${formatPlaybackTime(data)}`;
  }

  // 先画线路
  lineGeometries.forEach(line => {
    const rows = (line.stations || []).filter(r => r.lon && r.lat);
    if (!rows.length) return;

    traces.push({
      type: "scattermapbox",
      mode: "lines",
      lon: rows.map(r => r.lon),
      lat: rows.map(r => r.lat),
      line: { width: 4, color: line.color || "#6ea8ff" }, // 线路颜色
      hoverinfo: "skip",
      showlegend: false
    });
  });

  // 再画站点
  const points = current.filter(r => r.lon && r.lat);
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

  // 大风模式红叉
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

  // 严重拥堵高亮
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
    margin: { l: 0, r: 0, t: 20, b: 0 }
  }, { responsive: true });

  const kpisEl = document.getElementById("kpis");
  if (kpisEl) {
    kpisEl.innerHTML = `
      <div class="kpi"><span>站点总数</span><b>${data.kpis?.station_count || 0}</b></div>
      <div class="kpi"><span>线路总数</span><b>${data.kpis?.line_count || 0}</b></div>
      <div class="kpi"><span>当前时间片</span><b>${formatPlaybackTime(data)}</b></div>
      <div class="kpi"><span>拥堵站点</span><b class="bad-orange">${data.kpis?.crowded_count || 0}</b></div>
      <div class="kpi"><span>严重拥堵站点</span><b class="bad-red">${data.kpis?.fault_count || 0}</b></div>
      <div class="kpi"><span>预测模型</span><b>${data.prediction?.model || "-"}</b></div>
    `;
  }

  const risks = data.risk_top || [];
  const riskListEl = document.getElementById("riskList");
  if (riskListEl) {
    riskListEl.innerHTML = risks.length ? risks.map(r => `
      <div class="risk-item">
        <div><b>${r.station_name}</b></div>
        <div>${r.line_name || ""}</div>
        <div>当前: ${r.total_flow || 0}</div>
        <div>预测: ${r.predicted_flow || 0}</div>
        <div class="${r.status === "fault" ? "bad-red" : "bad-orange"}">级联后: ${r.cascade_flow || 0}</div>
      </div>
    `).join("") : '<div class="card">暂无预测结果</div>';
  }

  const waves = data.cascade?.waves || [];
  const cascadeBoxEl = document.getElementById("cascadeBox");
  if (cascadeBoxEl) {
    cascadeBoxEl.innerHTML = waves.length ? waves.map(w => `
      <div class="cascade-item">
        <div><b>第 ${w.wave} 波</b></div>
        <div>${w.stations.length} 个站点被带动拥堵</div>
      </div>
    `).join("") : '<div class="card">当前时间片无明显拥堵传播</div>';
  }
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

// 新增：暂停/播放按钮的事件监听器
const pausePlayBtn = document.getElementById("pausePlayBtn");
if (pausePlayBtn) {
  pausePlayBtn.addEventListener("click", () => {
    isPaused = !isPaused; // 切换暂停状态
    updatePausePlayButtonText(); // 更新按钮文本

    if (isPaused) {
      stopAutoRefresh(); // 暂停时停止自动刷新
    } else {
      startAutoRefresh(); // 播放时重新启动自动刷新（从当前 tick 继续）
    }
  });
}

const modelSelect = document.getElementById("modelSelect");
if (modelSelect) {
  modelSelect.addEventListener("change", async () => {
    tick = 0; // 切换模型时重置时间片计数
    await refresh(); // 立即刷新一次

    // 如果非暂停状态，则停止当前计时器并重新启动，以确保从新的 tick=0 开始自动刷新
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
    kpisEl.innerHTML = `<div class="card">后端返回异常：${err.message}</div>`;
  }
});