from __future__ import annotations

import pandas as pd


def _safe_int(v, default=0):
    try:
        if pd.isna(v):
            return default
        return int(v)
    except Exception:
        return default


def _safe_float(v, default=0.0):
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def calc_connectivity_index(
    station_count: int,
    fault_count: int,
    crowded_count: int,
    vulnerable_count: int,
) -> float:
    """
    面向可视化的连通指数（0~100）
    不是严格图论连通率，而是用于监控大屏展示的综合运行健康度。
    """
    if station_count <= 0:
        return 0.0

    score = (
        100.0
        - fault_count * 2.0
        - crowded_count * 0.35
        - vulnerable_count * 0.15
    )
    score = max(0.0, min(100.0, score))
    return round(score, 1)


def build_status_distribution(snap: pd.DataFrame) -> dict:
    if snap is None or snap.empty or "status" not in snap.columns:
        return {
            "normal": 0,
            "crowded": 0,
            "vulnerable": 0,
            "fault": 0,
        }

    return {
        "normal": int((snap["status"] == "normal").sum()),
        "crowded": int((snap["status"] == "crowded").sum()),
        "vulnerable": int((snap["status"] == "vulnerable").sum()),
        "fault": int((snap["status"] == "fault").sum()),
    }


def build_plot_kpis(snap: pd.DataFrame, line_count: int = 0) -> dict:
    if snap is None or snap.empty:
        return {
            "station_count": 0,
            "line_count": int(line_count),
            "crowded_count": 0,
            "vulnerable_count": 0,
            "fault_count": 0,
            "avg_flow": 0.0,
            "avg_display_flow": 0.0,
            "avg_predicted_flow": 0.0,
            "avg_cascade_flow": 0.0,
            "connectivity_index": 0.0,
        }

    station_count = int(snap["station_key"].nunique()) if "station_key" in snap.columns else len(snap)
    crowded_count = int((snap["status"] == "crowded").sum()) if "status" in snap.columns else 0
    vulnerable_count = int((snap["status"] == "vulnerable").sum()) if "status" in snap.columns else 0
    fault_count = int((snap["status"] == "fault").sum()) if "status" in snap.columns else 0

    avg_flow = float(snap["total_flow"].mean()) if "total_flow" in snap.columns and len(snap) else 0.0
    avg_display_flow = float(snap["display_flow"].mean()) if "display_flow" in snap.columns and len(snap) else 0.0
    avg_predicted_flow = float(snap["predicted_flow"].mean()) if "predicted_flow" in snap.columns and len(snap) else 0.0
    avg_cascade_flow = float(snap["cascade_flow"].mean()) if "cascade_flow" in snap.columns and len(snap) else 0.0

    connectivity_index = calc_connectivity_index(
        station_count=station_count,
        fault_count=fault_count,
        crowded_count=crowded_count,
        vulnerable_count=vulnerable_count,
    )

    return {
        "station_count": station_count,
        "line_count": int(line_count),
        "crowded_count": crowded_count,
        "vulnerable_count": vulnerable_count,
        "fault_count": fault_count,
        "avg_flow": round(avg_flow, 2),
        "avg_display_flow": round(avg_display_flow, 2),
        "avg_predicted_flow": round(avg_predicted_flow, 2),
        "avg_cascade_flow": round(avg_cascade_flow, 2),
        "connectivity_index": connectivity_index,
    }


def build_plot_series(
    snap: pd.DataFrame,
    playback_time: str,
    kpis: dict,
) -> dict:
    """
    给前端 plot 用的单帧摘要。
    当前版本前端会自己累计 history，但后端也输出结构化数据，方便后续扩展。
    """
    return {
        "time_label": playback_time,
        "connectivity_index": _safe_float(kpis.get("connectivity_index")),
        "avg_flow": _safe_float(kpis.get("avg_flow")),
        "avg_display_flow": _safe_float(kpis.get("avg_display_flow")),
        "avg_predicted_flow": _safe_float(kpis.get("avg_predicted_flow")),
        "avg_cascade_flow": _safe_float(kpis.get("avg_cascade_flow")),
        "crowded_count": _safe_int(kpis.get("crowded_count")),
        "vulnerable_count": _safe_int(kpis.get("vulnerable_count")),
        "fault_count": _safe_int(kpis.get("fault_count")),
    }


def build_alerts(
    snap: pd.DataFrame,
    cascade_info: dict | None = None,
    top_n: int = 5,
) -> list:
    if snap is None or snap.empty:
        return []

    alerts = []

    if "status" in snap.columns:
        fault_df = snap[snap["status"] == "fault"].copy()
        if not fault_df.empty:
            fault_df = fault_df.sort_values(
                ["cascade_flow", "predicted_flow", "total_flow"],
                ascending=False
            )
            for _, r in fault_df.head(top_n).iterrows():
                alerts.append({
                    "type": "fault",
                    "level": "high",
                    "station_key": r.get("station_key"),
                    "station_name": r.get("station_name"),
                    "line_name": r.get("line_name"),
                    "current_flow": _safe_int(r.get("total_flow")),
                    "predicted_flow": _safe_int(r.get("predicted_flow")),
                    "cascade_flow": _safe_int(r.get("cascade_flow")),
                    "message": f"{r.get('station_name', '-') }发生严重拥堵/失效风险",
                })

    crowded_df = pd.DataFrame()
    if "status" in snap.columns:
        crowded_df = snap[snap["status"] == "crowded"].copy()

    if not crowded_df.empty:
        crowded_df = crowded_df.sort_values(
            ["cascade_flow", "predicted_flow", "total_flow"],
            ascending=False
        )
        for _, r in crowded_df.head(max(0, top_n - len(alerts))).iterrows():
            alerts.append({
                "type": "crowded",
                "level": "medium",
                "station_key": r.get("station_key"),
                "station_name": r.get("station_name"),
                "line_name": r.get("line_name"),
                "current_flow": _safe_int(r.get("total_flow")),
                "predicted_flow": _safe_int(r.get("predicted_flow")),
                "cascade_flow": _safe_int(r.get("cascade_flow")),
                "message": f"{r.get('station_name', '-') }处于高拥堵状态",
            })

    if cascade_info:
        waves = cascade_info.get("waves", []) or []
        for w in waves[:2]:
            alerts.append({
                "type": "cascade_wave",
                "level": "medium",
                "wave": _safe_int(w.get("wave")),
                "station_count": len(w.get("stations", []) or []),
                "stations": w.get("stations", []) or [],
                "message": f"第 {w.get('wave', '-') } 波级联传播已触发",
            })

    return alerts[:top_n + 2]