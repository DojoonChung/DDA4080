from __future__ import annotations
import numpy as np
import pandas as pd


def _prepare_frame(ts: pd.DataFrame) -> pd.DataFrame:
    df = ts.copy()
    df = df.sort_values(["station_key", "date", "slot"]).reset_index(drop=True)
    df["station_code"] = pd.factorize(df["station_key"])[0]
    df["day_index"] = pd.factorize(df["date"])[0]
    df["is_peak"] = df["slot"].between(18, 24) | df["slot"].between(42, 54)

    for lag in [1, 2, 3]:
        df[f"lag_{lag}"] = df.groupby("station_key")["total_flow"].shift(lag)

    prev_day = df[["station_key", "day_index", "slot", "total_flow"]].copy()
    prev_day["day_index"] = prev_day["day_index"] + 1
    prev_day = prev_day.rename(columns={"total_flow": "prev_day_same_slot"})
    df = df.merge(prev_day, on=["station_key", "day_index", "slot"], how="left")
    df["target_next"] = df.groupby(["station_key", "date"])["total_flow"].shift(-1)
    return df


def train_realtime_model(ts: pd.DataFrame, model_name: str = "lightgbm", train_days: int = 3) -> dict:
    df = _prepare_frame(ts)
    unique_dates = sorted(df["date"].dropna().unique().tolist())
    if len(unique_dates) < 2:
        return {"model_name": "no_data", "train_dates": [], "playback_date": None, "metrics": {"mae": None, "r2": None}, "predictions": pd.DataFrame()}

    train_dates = unique_dates[:-1] if len(unique_dates) <= train_days else unique_dates[:train_days]
    playback_date = unique_dates[-1] if len(unique_dates) <= train_days else unique_dates[train_days]

    feature_cols = ["slot", "station_code", "day_index", "is_peak", "lag_1", "lag_2", "lag_3", "prev_day_same_slot"]
    train_df = df[df["date"].isin(train_dates)].dropna(subset=feature_cols + ["target_next"]).copy()
    test_df = df[df["date"] == playback_date].dropna(subset=feature_cols).copy()

    if train_df.empty or test_df.empty:
        return {"model_name": "no_data", "train_dates": train_dates, "playback_date": playback_date, "metrics": {"mae": None, "r2": None}, "predictions": pd.DataFrame()}

    X_train, y_train = train_df[feature_cols], train_df["target_next"]
    X_test = test_df[feature_cols]
    y_test = test_df["target_next"].fillna(test_df["lag_1"])

    model_used = "baseline"
    try:
        if model_name == "xgboost":
            from xgboost import XGBRegressor
            model = XGBRegressor(objective="reg:squarederror", n_estimators=180, learning_rate=0.06, max_depth=6, subsample=0.9, colsample_bytree=0.9, random_state=42)
            model.fit(X_train, y_train)
            pred = model.predict(X_test)
            model_used = "xgboost"
        elif model_name == "lightgbm":
            try:
                from lightgbm import LGBMRegressor
                model = LGBMRegressor(objective="regression", n_estimators=220, learning_rate=0.06, num_leaves=31, random_state=42, verbose=-1)
                model.fit(X_train, y_train)
                pred = model.predict(X_test)
                model_used = "lightgbm"
            except Exception:
                from sklearn.ensemble import HistGradientBoostingRegressor
                model = HistGradientBoostingRegressor(max_depth=6, learning_rate=0.06, random_state=42)
                model.fit(X_train, y_train)
                pred = model.predict(X_test)
                model_used = "hgb_fallback"
        else:
            pred = X_test["lag_1"].to_numpy()
            model_used = "baseline"
    except Exception:
        pred = X_test["lag_1"].to_numpy()
        model_used = "baseline"

    pred = np.maximum(np.round(pred), 0).astype(int)
    test_df = test_df.copy()
    test_df["predicted_next_flow"] = pred

    from sklearn.metrics import mean_absolute_error, r2_score
    return {
        "model_name": model_used,
        "train_dates": train_dates,
        "playback_date": playback_date,
        "metrics": {
            "mae": float(mean_absolute_error(y_test, pred)) if len(y_test) else None,
            "r2": float(r2_score(y_test, pred)) if len(y_test) else None,
        },
        "predictions": test_df,
    }


def build_realtime_snapshot(master: pd.DataFrame, ts: pd.DataFrame, prediction_bundle: dict, adjacency: dict, step: int):
    pred_df = prediction_bundle["predictions"]
    playback_date = prediction_bundle["playback_date"]

    station_meta = master.groupby("station_key", as_index=False).agg(
        station_name=("站点名称", "first"),
        line_name=("线路名称", lambda s: "、".join(sorted(set([x for x in s if pd.notna(x) and x])))),
        lon=("经度", "first"),
        lat=("纬度", "first"),
        wind_exposed=("wind_exposed", "max"),
    )

    if playback_date is None or ts.empty:
        cur = station_meta.copy()
        cur["date"] = ""
        cur["slot"] = 0
        cur["total_flow"] = 0
        cur["predicted_flow"] = 0
        cur["cascade_flow"] = 0
        cur["load_ratio"] = 0.0
        cur["status"] = "normal"
        return {"snapshot": cur, "slot": 0, "playback_date": playback_date, "cascade_info": {"source_station": None, "waves": []}}

    day_slots = sorted(ts.loc[ts["date"] == playback_date, "slot"].dropna().unique().tolist())
    if not day_slots:
        day_slots = sorted(ts["slot"].dropna().unique().tolist())
    slot = day_slots[step % len(day_slots)]

    current = ts[(ts["date"] == playback_date) & (ts["slot"] == slot)].copy()
    pred_slot = pred_df[(pred_df["date"] == playback_date) & (pred_df["slot"] == slot)].copy()

    cur = station_meta.merge(current[["station_key", "date", "slot", "total_flow"]], on="station_key", how="left")
    cur = cur.merge(pred_slot[["station_key", "predicted_next_flow"]], on="station_key", how="left")
    cur["date"] = cur["date"].fillna(playback_date)
    cur["slot"] = cur["slot"].fillna(slot).astype(int)
    cur["total_flow"] = cur["total_flow"].fillna(0).astype(int)
    cur["predicted_flow"] = cur["predicted_next_flow"].fillna(cur["total_flow"]).astype(int)
    cur = cur.drop(columns=["predicted_next_flow"], errors="ignore")

    cap = ts.groupby("station_key")["total_flow"].quantile(0.95).reset_index().rename(columns={"total_flow": "capacity_base"})
    cur = cur.merge(cap, on="station_key", how="left")
    cur["capacity_base"] = cur["capacity_base"].fillna(cur["total_flow"].replace(0, 1))
    cur["load_ratio"] = cur["predicted_flow"] / cur["capacity_base"].replace(0, 1)

    base_over = dict(zip(cur["station_key"], np.maximum(cur["load_ratio"] - 0.85, 0)))
    cascade_add = {k: 0.0 for k in cur["station_key"]}
    waves = []
    source_station = max(base_over, key=lambda x: base_over[x]) if base_over else None

    frontier = [(k, v) for k, v in base_over.items() if v > 0]
    visited = set(k for k, _ in frontier)

    for wave_idx in range(1, 4):
        next_frontier = []
        wave_nodes = []
        for node, overload in frontier:
            for nb in adjacency.get(node, []):
                transmit = overload * (0.55 if wave_idx == 1 else 0.35 if wave_idx == 2 else 0.20)
                if transmit <= 0:
                    continue
                cascade_add[nb] = cascade_add.get(nb, 0.0) + transmit
                if nb not in visited:
                    visited.add(nb)
                    next_frontier.append((nb, transmit * 0.8))
                    wave_nodes.append(nb)
        if wave_nodes:
            waves.append({"wave": wave_idx, "stations": sorted(set(wave_nodes))})
        frontier = next_frontier

    cur["cascade_flow"] = (cur["predicted_flow"] * (1 + cur["station_key"].map(cascade_add).fillna(0))).round().astype(int)
    cur["cascade_ratio"] = cur["cascade_flow"] / cur["capacity_base"].replace(0, 1)

    def _status(r):
        if r["cascade_ratio"] >= 1.15:
            return "fault"
        if r["cascade_ratio"] >= 0.95:
            return "crowded"
        if r["cascade_ratio"] >= 0.75:
            return "vulnerable"
        return "normal"

    cur["status"] = cur.apply(_status, axis=1)
    cur["display_flow"] = cur["cascade_flow"]

    return {
        "snapshot": cur,
        "slot": int(slot),
        "playback_date": playback_date,
        "cascade_info": {"source_station": source_station, "waves": waves},
    }