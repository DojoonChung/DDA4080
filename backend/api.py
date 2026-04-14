from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from .config import BASE_DIR
from .utils import records, safe_json_value
from .data_pipeline import clean_raw_od, build_adjacency, build_line_geometries
from .predictor import train_realtime_model, build_realtime_snapshot

app = FastAPI(title="Beijing Metro Realtime Resilience API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = BASE_DIR / "frontend"

APP_STATE = {
    "bootstrapped": False,
    "master": None,
    "ts": None,
    "od": None,
    "adjacency": None,
    "line_geometries": None,
    "models": {},
}


def bootstrap():
    master, ts, od_agg = clean_raw_od()
    APP_STATE["master"] = master
    APP_STATE["ts"] = ts
    APP_STATE["od"] = od_agg
    APP_STATE["adjacency"] = build_adjacency(master)
    APP_STATE["line_geometries"] = build_line_geometries(master)
    APP_STATE["models"]["lightgbm"] = train_realtime_model(ts, "lightgbm", train_days=3)
    APP_STATE["models"]["xgboost"] = train_realtime_model(ts, "xgboost", train_days=3)
    APP_STATE["models"]["baseline"] = train_realtime_model(ts, "baseline", train_days=3)
    APP_STATE["bootstrapped"] = True
    return True


def ensure_bootstrap():
    if not APP_STATE["bootstrapped"]:
        bootstrap()


@app.get("/")
def root():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/app.js")
def app_js():
    return FileResponse(FRONTEND_DIR / "app.js")


@app.get("/styles.css")
def styles():
    return FileResponse(FRONTEND_DIR / "styles.css")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/bootstrap")
def api_bootstrap():
    bootstrap()
    model_info = {
        k: {
            "model": v["model_name"],
            "train_dates": v["train_dates"],
            "playback_date": v["playback_date"],
            "metrics": v["metrics"],
        }
        for k, v in APP_STATE["models"].items()
    }
    return safe_json_value({"ok": True, "models": model_info})


@app.get("/api/live")
def api_live(step: int = 0, model: str = "lightgbm", wind_mode: bool = False):
    ensure_bootstrap()

    model_bundle = APP_STATE["models"].get(model) or APP_STATE["models"]["lightgbm"]
    snap_bundle = build_realtime_snapshot(
        APP_STATE["master"], APP_STATE["ts"], model_bundle, APP_STATE["adjacency"], step
    )
    snap = snap_bundle["snapshot"].copy()

    # ===== 关键补丁：统一兜底列，避免 display_flow / cascade_flow / predicted_flow 缺失 =====
    if "total_flow" not in snap.columns:
        snap["total_flow"] = 0
    if "predicted_flow" not in snap.columns:
        snap["predicted_flow"] = snap["total_flow"]
    if "cascade_flow" not in snap.columns:
        snap["cascade_flow"] = snap["predicted_flow"]
    if "display_flow" not in snap.columns:
        snap["display_flow"] = snap["cascade_flow"]
    if "status" not in snap.columns:
        snap["status"] = "normal"
    if "wind_exposed" not in snap.columns:
        snap["wind_exposed"] = False

    failed = snap[snap["status"] == "fault"][["station_key", "station_name", "line_name", "lon", "lat"]]
    impacted = snap[snap["status"].isin(["fault", "crowded", "vulnerable"])][["station_key", "station_name", "line_name", "lon", "lat"]]
    wind_markers = snap[snap["wind_exposed"] == True][["station_key", "station_name", "line_name", "lon", "lat"]]

    risk_cols = [c for c in ["station_key", "station_name", "line_name", "total_flow", "predicted_flow", "cascade_flow", "status"] if c in snap.columns]
    risk_top = snap.sort_values("cascade_flow", ascending=False).head(10)[risk_cols]

    payload = {
        "tick": step,
        "date": snap_bundle["playback_date"],
        "slot": snap_bundle["slot"],
        "playback_time": f'{snap_bundle["playback_date"]} {4 + snap_bundle["slot"] // 6:02d}:{(snap_bundle["slot"] % 6) * 10:02d}',
        "line_geometries": APP_STATE["line_geometries"],
        "current": records(snap),
        "risk_top": records(risk_top),
        "prediction": {
            "model": model_bundle["model_name"],
            "metrics": model_bundle["metrics"],
            "train_dates": model_bundle["train_dates"],
            "playback_date": model_bundle["playback_date"],
        },
        "cascade": {
            "source_station": snap_bundle["cascade_info"]["source_station"],
            "failed_count": int(len(failed)),
            "impacted_count": int(len(impacted)),
            "failed_stations": records(failed),
            "impacted_stations": records(impacted),
            "waves": safe_json_value(snap_bundle["cascade_info"]["waves"]),
        },
        "wind_markers": records(wind_markers),
        "kpis": {
            "station_count": int(snap["station_key"].nunique()) if "station_key" in snap.columns else 0,
            "line_count": int(len(APP_STATE["line_geometries"])),
            "crowded_count": int((snap["status"] == "crowded").sum()),
            "fault_count": int((snap["status"] == "fault").sum()),
            "avg_display_flow": float(snap["display_flow"].mean()) if len(snap) else 0.0,
        },
    }
    return safe_json_value(payload)