from __future__ import annotations
from typing import Optional
from collections import defaultdict, deque

import pandas as pd


def build_line_graph(master: pd.DataFrame) -> dict:
    graph = defaultdict(set)
    if master is None or master.empty:
        return graph

    tmp = master.copy()
    if "display_order" not in tmp.columns:
        tmp["display_order"] = tmp.get("map_seq_id", tmp.get("line_order", 0))

    tmp = tmp.dropna(subset=["线路名称", "站点名称"])
    for line, sub in tmp.groupby("线路名称"):
        sub = sub.sort_values("display_order")
        keys = sub["station_key"].tolist()
        for a, b in zip(keys[:-1], keys[1:]):
            graph[a].add(b)
            graph[b].add(a)
    return graph


def run_cascade(master: pd.DataFrame, ts: pd.DataFrame, source_station_key: Optional[str] = None, wind_mode: bool = False) -> dict:
    graph = build_line_graph(master)

    if ts is None or ts.empty or "station_key" not in ts.columns:
        return {
            "source_station_key": source_station_key,
            "failed_count": 0,
            "impacted_count": 0,
            "failed_stations": [],
            "impacted_stations": [],
            "waves": [],
        }

    latest = ts.sort_values(["date", "slot"]).groupby("station_key", as_index=False).tail(1)
    if latest.empty:
        return {
            "source_station_key": source_station_key,
            "failed_count": 0,
            "impacted_count": 0,
            "failed_stations": [],
            "impacted_stations": [],
            "waves": [],
        }

    latest["total_flow"] = latest["total_flow"].fillna(0)

    if source_station_key is None:
        source_station_key = latest.sort_values("total_flow", ascending=False)["station_key"].iloc[0]

    flow_map = latest.set_index("station_key")["total_flow"].to_dict()

    meta = (
        master.groupby("station_key", as_index=False)
        .agg(
            station_name=("站点名称", "first"),
            line_name=("线路名称", lambda s: "、".join(sorted(set([x for x in s if pd.notna(x) and x])))),
            lon=("经度", "first"),
            lat=("纬度", "first"),
        )
        .set_index("station_key")
        .to_dict("index")
    )

    failed = {source_station_key}
    impacted = {source_station_key}
    queue = deque([(source_station_key, 0)])
    visited = {source_station_key}
    wave_records = []

    threshold_q = 0.35 if wind_mode else 0.55
    threshold_value = latest["total_flow"].quantile(threshold_q)

    while queue:
        cur, step = queue.popleft()
        if step >= 5:
            continue

        this_wave = []
        for nb in graph.get(cur, []):
            if nb in visited:
                continue
            visited.add(nb)

            nb_flow = flow_map.get(nb, 0)
            if nb_flow >= threshold_value:
                failed.add(nb)
                impacted.add(nb)
                this_wave.append(nb)
                queue.append((nb, step + 1))
            else:
                impacted.add(nb)

        if this_wave:
            wave_records.append({"wave": step + 1, "stations": this_wave})

    return {
        "source_station_key": source_station_key,
        "failed_count": len(failed),
        "impacted_count": len(impacted),
        "failed_stations": [{"station_key": k, **meta.get(k, {})} for k in failed],
        "impacted_stations": [{"station_key": k, **meta.get(k, {})} for k in impacted],
        "waves": wave_records,
    }


def wind_markers(master: pd.DataFrame) -> list:
    if master is None or master.empty or "wind_exposed" not in master.columns:
        return []

    df = (
        master[master["wind_exposed"] == True]
        .groupby("station_key", as_index=False)
        .agg(
            station_name=("站点名称", "first"),
            line_name=("线路名称", lambda s: "、".join(sorted(set([x for x in s if pd.notna(x) and x])))),
            lon=("经度", "first"),
            lat=("纬度", "first"),
        )
    )
    return df.fillna("").to_dict("records")