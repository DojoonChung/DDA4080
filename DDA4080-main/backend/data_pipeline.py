from __future__ import annotations

import re
from collections import defaultdict

import pandas as pd

from .config import (
    BASE_DIR, DATA_DIR, RAW_OD_GLOB, STATION_INFO_XLSX, MAP_SEQ_XLSX,
    CLEANED_GEO_CSV, STATION_GEO_TXT, CACHE_DIR, TIME_BIN_MINUTES,
    MIN_TRAVEL_MINUTES, MAX_TRAVEL_MINUTES, WIND_EXPOSED_LINES, LINE_COLORS,
)
from .utils import normalize_station_name, safe_read_csv, safe_read_excel


def normalize_line_name(x: str) -> str:
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return ""
    s = s.replace("地铁", "").replace("轨道交通", "").replace("线路", "")
    s = s.replace("（", "(").replace("）", ")").strip()
    if re.fullmatch(r"\d+", s):
        return f"{s}号线"
    if re.fullmatch(r"\d+号线", s):
        return s
    if s in {"大兴线"} or ("4号线" in s and "大兴" in s):
        return "4号线/大兴线"
    if "8号线" in s:
        return "8号线"
    if "14号线" in s:
        return "14号线"
    return s


def _find_raw_od_files():
    candidates = (
        list(DATA_DIR.glob(RAW_OD_GLOB))
        + list(BASE_DIR.glob(RAW_OD_GLOB))
        + list(BASE_DIR.parent.glob(RAW_OD_GLOB))
    )
    files = sorted({p.resolve() for p in candidates if p.exists()})
    print("RAW OD files found:", files)
    return files


def load_station_info() -> pd.DataFrame:
    if not STATION_INFO_XLSX.exists():
        return pd.DataFrame(columns=["线路名称", "站点名称", "line_order"])

    df = safe_read_excel(STATION_INFO_XLSX)
    rename = {}
    for c in df.columns:
        col = str(c).strip()
        if "线路" in col:
            rename[c] = "线路名称"
        if "车站" in col or "站点" in col:
            rename[c] = "站点名称"

    df = df.rename(columns=rename)
    df = df[[c for c in ["线路名称", "站点名称"] if c in df.columns]].copy()
    df["线路名称"] = df["线路名称"].map(normalize_line_name)
    df["站点名称"] = df["站点名称"].map(normalize_station_name)
    df = df.dropna().drop_duplicates().reset_index(drop=True)
    df["line_order"] = df.groupby("线路名称").cumcount() + 1
    return df


def load_geo_table() -> pd.DataFrame:
    if CLEANED_GEO_CSV.exists():
        geo = safe_read_csv(CLEANED_GEO_CSV, encoding="utf-8", low_memory=False)
        geo = geo.rename(columns={"车站名称": "站点名称"})
        if "站点名称" in geo.columns:
            geo["站点名称"] = geo["站点名称"].map(normalize_station_name)
        if "线路名称" in geo.columns:
            geo["线路名称"] = geo["线路名称"].map(normalize_line_name)
        return geo

    rows = []
    if STATION_GEO_TXT.exists():
        with open(STATION_GEO_TXT, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                m = re.match(r"^([^\d]+)\s+([\d.]+),([\d.]+)$", line)
                if m:
                    rows.append(
                        {
                            "站点名称": normalize_station_name(m.group(1)),
                            "经度": float(m.group(2)),
                            "纬度": float(m.group(3)),
                        }
                    )
    return pd.DataFrame(rows).drop_duplicates(subset=["站点名称"])


def load_map_seq() -> pd.DataFrame:
    if not MAP_SEQ_XLSX.exists():
        return pd.DataFrame(columns=["线路名称", "站点名称", "map_seq_id"])

    df = safe_read_excel(MAP_SEQ_XLSX)
    rename = {}
    for c in df.columns:
        col = str(c).strip()
        if col == "线路":
            rename[c] = "线路"
        elif col == "站点":
            rename[c] = "站点名称"
        elif col == "序号":
            rename[c] = "map_seq_id"
    df = df.rename(columns=rename)

    if "线路" in df.columns:
        df["线路名称"] = df["线路"].map(normalize_line_name)
    else:
        df["线路名称"] = ""

    if "站点名称" in df.columns:
        df["站点名称"] = df["站点名称"].map(normalize_station_name)

    keep = [c for c in ["线路名称", "站点名称", "map_seq_id"] if c in df.columns]
    return df[keep].dropna(subset=["站点名称"]).drop_duplicates()

def load_station_code_map() -> pd.DataFrame:
    station_csv = DATA_DIR / "station.csv"
    if not station_csv.exists():
        print("station.csv not found")
        return pd.DataFrame(columns=["raw_station_id", "站点名称", "线路名称"])

    # 强制按字符串读取，避免编码列被转成浮点或科学计数法
    df = safe_read_csv(station_csv, encoding="utf-8", low_memory=False, dtype=str)
    print("station.csv columns:", list(df.columns))

    line_col = None
    name_col = None
    id_col = None

    # 1) 先最优先精确识别“车站编码”
    for c in df.columns:
        cs = str(c).strip()
        if cs == "车站编码":
            id_col = c
            break

    # 2) 识别站点名称列，优先 STATION_CNAME / 站点名称 / 车站名称
    for c in df.columns:
        cs = str(c).strip()
        cu = cs.upper()
        if cs in {"站点名称", "车站名称"} or cu == "STATION_CNAME" or cu == "STATION_NAME":
            name_col = c
            break

    # 3) 识别线路列，优先 LINE_CHNAME，但跳过 LINE_CHNAME.1 这种重复列
    for c in df.columns:
        cs = str(c).strip()
        cu = cs.upper()
        if cu == "LINE_CHNAME" or cs == "线路名称" or cs == "线路":
            line_col = c
            break

    # 4) 如果上面没识别出来，再兜底
    if name_col is None:
        for c in df.columns:
            cs = str(c).strip()
            cu = cs.upper()
            if ("站" in cs or "STATION" in cu) and ("编码" not in cs and "ID" not in cu):
                name_col = c
                break

    if line_col is None:
        for c in df.columns:
            cs = str(c).strip()
            cu = cs.upper()
            if "LINE" in cu or "线路" in cs:
                # 避免误选重复列 LINE_CHNAME.1
                if ".1" not in cs:
                    line_col = c
                    break

    if id_col is None:
        raise ValueError("station.csv 中未识别到“车站编码”列")
    if name_col is None:
        raise ValueError("station.csv 中未识别到站点名称列")

    if line_col is not None:
        out = df[[line_col, name_col, id_col]].copy()
        out.columns = ["线路名称", "站点名称", "raw_station_id"]
        out["线路名称"] = out["线路名称"].map(normalize_line_name)
    else:
        out = df[[name_col, id_col]].copy()
        out.columns = ["站点名称", "raw_station_id"]
        out["线路名称"] = ""

    out["站点名称"] = out["站点名称"].map(normalize_station_name)

    # 只保留数字编码
    out["raw_station_id"] = out["raw_station_id"].astype(str).str.extract(r"(\d+)")[0]

    out = out.dropna(subset=["raw_station_id", "站点名称"]).drop_duplicates(subset=["raw_station_id"])

    print("station code map rows:", len(out))
    print("station code map sample:")
    print(out.head(5).to_string())

    return out

def build_master_station_table() -> pd.DataFrame:
    info = load_station_info()
    geo = load_geo_table()
    map_seq = load_map_seq()
    code_map = load_station_code_map()

    master = info.copy()

    if not geo.empty:
        geo_name = geo[["站点名称", "经度", "纬度"]].drop_duplicates(subset=["站点名称"])
        master = master.merge(geo_name, on="站点名称", how="left")

    if not map_seq.empty:
        master = master.merge(map_seq, on=["线路名称", "站点名称"], how="left")

    # 这里只保留 raw_station_id 作为补充，不再让线路名不一致影响主表
    if not code_map.empty:
        code_name = code_map[["raw_station_id", "站点名称"]].drop_duplicates(subset=["站点名称"])
        master = master.merge(code_name, on="站点名称", how="left")

    master["station_key"] = master["站点名称"]
    master["display_order"] = master["map_seq_id"].fillna(master["line_order"])
    master["wind_exposed"] = master["线路名称"].isin(WIND_EXPOSED_LINES)
    master["line_color"] = master["线路名称"].map(lambda x: LINE_COLORS.get(normalize_line_name(x), "#6ea8ff"))
    master = master.drop_duplicates(subset=["线路名称", "站点名称"]).reset_index(drop=True)

    master.to_csv(CACHE_DIR / "master_station_table.csv", index=False, encoding="utf-8-sig")
    print("master rows:", len(master))
    return master


def build_adjacency(master: pd.DataFrame) -> dict:
    adj = defaultdict(set)
    if master is None or master.empty:
        return {}
    tmp = master.copy()
    if "display_order" not in tmp.columns:
        tmp["display_order"] = tmp["map_seq_id"].fillna(tmp["line_order"])
    tmp = tmp.dropna(subset=["线路名称", "站点名称"])
    for line, sub in tmp.groupby("线路名称"):
        sub = sub.sort_values("display_order")
        stations = sub["station_key"].tolist()
        for a, b in zip(stations[:-1], stations[1:]):
            adj[a].add(b)
            adj[b].add(a)
    return {k: sorted(v) for k, v in adj.items()}


def build_line_geometries(master: pd.DataFrame) -> list:
    if master is None or master.empty:
        return []
    tmp = master.copy()
    if "display_order" not in tmp.columns:
        tmp["display_order"] = tmp["map_seq_id"].fillna(tmp["line_order"])
    tmp = tmp.dropna(subset=["线路名称", "站点名称", "经度", "纬度"])
    out = []
    for line, sub in tmp.groupby("线路名称"):
        sub = sub.sort_values("display_order").drop_duplicates(subset=["站点名称"])
        out.append({
            "line_name": line,
            "color": LINE_COLORS.get(normalize_line_name(line), "#6ea8ff"),
            "stations": sub[["站点名称", "经度", "纬度", "display_order"]]
                .rename(columns={"站点名称": "station_name", "经度": "lon", "纬度": "lat"})
                .to_dict("records"),
        })
    return out


def clean_raw_od():
    files = _find_raw_od_files()
    if not files:
        raise FileNotFoundError("未找到 sim_od_*.csv 原始OD文件。")

    frames = []
    for p in files:
        df = safe_read_csv(p, encoding="utf-8", low_memory=False)
        cols = {str(c).lower(): c for c in df.columns}
        d = df[[cols["in_time"], cols["out_time"], cols["in_station_id"], cols["out_station_id"]]].copy()
        d.columns = ["in_time", "out_time", "in_station_id", "out_station_id"]
        frames.append(d)

    od = pd.concat(frames, ignore_index=True).dropna()
    print("raw od rows:", len(od))

    od["in_time"] = pd.to_datetime(od["in_time"], errors="coerce")
    od["out_time"] = pd.to_datetime(od["out_time"], errors="coerce")
    od["in_station_id"] = od["in_station_id"].astype(str).str.extract(r"(\d+)")[0]
    od["out_station_id"] = od["out_station_id"].astype(str).str.extract(r"(\d+)")[0]
    od = od.dropna(subset=["in_time", "out_time", "in_station_id", "out_station_id"])
    od = od[od["in_station_id"] != od["out_station_id"]].copy()

    od["travel_minutes"] = (od["out_time"] - od["in_time"]).dt.total_seconds() / 60.0
    od = od[(od["travel_minutes"] >= MIN_TRAVEL_MINUTES) & (od["travel_minutes"] <= MAX_TRAVEL_MINUTES)].copy()
    print("od after basic clean:", len(od))

    master = build_master_station_table()
    code_map = load_station_code_map()

    # 关键改动：只按 raw_station_id -> 站点名称 映射
    id_to_name = code_map[["raw_station_id", "站点名称"]].drop_duplicates(subset=["raw_station_id"])
    in_map = id_to_name.rename(columns={"raw_station_id": "in_station_id", "站点名称": "in_station_name"})
    out_map = id_to_name.rename(columns={"raw_station_id": "out_station_id", "站点名称": "out_station_name"})

    od = od.merge(in_map, on="in_station_id", how="left")
    od = od.merge(out_map, on="out_station_id", how="left")

    print("matched in_station_name:", od["in_station_name"].notna().sum(), "/", len(od))
    print("matched out_station_name:", od["out_station_name"].notna().sum(), "/", len(od))

    od = od.dropna(subset=["in_station_name", "out_station_name"]).copy()
    print("od after station match:", len(od))

    od["station_key_in"] = od["in_station_name"]
    od["station_key_out"] = od["out_station_name"]
    od["date"] = od["in_time"].dt.strftime("%Y-%m-%d")
    base_time = od["in_time"].dt.normalize() + pd.Timedelta(hours=4)
    od["slot"] = (((od["in_time"] - base_time).dt.total_seconds() // (TIME_BIN_MINUTES * 60)).clip(lower=0)).astype(int)

    station_in = od.groupby(["date", "slot", "station_key_in"], as_index=False).size().rename(columns={"station_key_in": "station_key", "size": "in_flow"})
    station_out = od.groupby(["date", "slot", "station_key_out"], as_index=False).size().rename(columns={"station_key_out": "station_key", "size": "out_flow"})

    ts = station_in.merge(station_out, on=["date", "slot", "station_key"], how="outer")
    ts["in_flow"] = ts["in_flow"].fillna(0).astype(int)
    ts["out_flow"] = ts["out_flow"].fillna(0).astype(int)
    ts["total_flow"] = ts["in_flow"] + ts["out_flow"]

    meta = master.groupby("station_key", as_index=False).agg(
        station_name=("站点名称", "first"),
        line_name=("线路名称", lambda s: "、".join(sorted(set([x for x in s if pd.notna(x) and x])))),
        lon=("经度", "first"),
        lat=("纬度", "first"),
        wind_exposed=("wind_exposed", "max"),
    )
    ts = ts.merge(meta, on="station_key", how="left")
    ts["slot"] = ts["slot"].astype(int)
    ts["date"] = ts["date"].astype(str)

    od_agg = od.groupby(["date", "slot", "station_key_in", "station_key_out"], as_index=False).size().rename(
        columns={"station_key_in": "origin_station", "station_key_out": "destination_station", "size": "flow"}
    )

    print("timeseries rows:", len(ts))
    print("od_agg rows:", len(od_agg))

    master.to_csv(CACHE_DIR / "master_station_table.csv", index=False, encoding="utf-8-sig")
    ts.to_csv(CACHE_DIR / "timeseries_flow.csv", index=False, encoding="utf-8-sig")
    od_agg.to_csv(CACHE_DIR / "od_agg.csv", index=False, encoding="utf-8-sig")

    return master, ts, od_agg


def get_or_build():
    master_path = CACHE_DIR / "master_station_table.csv"
    ts_path = CACHE_DIR / "timeseries_flow.csv"
    od_path = CACHE_DIR / "od_agg.csv"
    if not (master_path.exists() and ts_path.exists() and od_path.exists()):
        return clean_raw_od()
    return (
        safe_read_csv(master_path, encoding="utf-8", low_memory=False),
        safe_read_csv(ts_path, encoding="utf-8", low_memory=False),
        safe_read_csv(od_path, encoding="utf-8", low_memory=False),
    )