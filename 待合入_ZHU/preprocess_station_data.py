from backend.data_pipeline import build_station_level_cache_from_raw


def main():
    print("开始从原始 sim_od_*.csv 构建站点级缓存...")
    master, ts, od_agg = build_station_level_cache_from_raw()
    print("构建完成：")
    print(f"master_station_table.csv rows: {len(master)}")
    print(f"timeseries_flow.csv rows: {len(ts)}")
    print(f"od_agg.csv rows: {len(od_agg)}")


if __name__ == "__main__":
    main()