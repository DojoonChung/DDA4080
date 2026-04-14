#!/usr/bin/env python3
"""verify_migration.py — 一键验证模型预测+级联移植"""
import sys, os
sys.path.insert(0, os.getcwd())

from backend.data_pipeline import clean_raw_od, build_adjacency
from backend.predictor import train_realtime_model, build_realtime_snapshot
from backend.cascade import run_cascade

def main():
    errors = []
    
    # Step 1: 数据
    print("=" * 50)
    print("Step 1: 数据管线")
    master, ts, od = clean_raw_od()
    adj = build_adjacency(master)
    if len(ts) == 0:
        errors.append("ts 为空，OD 清洗失败")
    if ts["total_flow"].sum() == 0:
        errors.append("total_flow 全为 0")
    if len(adj) < 10:
        errors.append(f"邻接表仅 {len(adj)} 个节点")
    print(f"  ts={len(ts)} rows, adj={len(adj)} nodes")
    
    # Step 2: 预测
    print("=" * 50)
    print("Step 2: 模型预测")
    for m in ["lightgbm", "xgboost", "baseline"]:
        b = train_realtime_model(ts, m, train_days=3)
        pred = b["predictions"]
        if pred.empty:
            errors.append(f"{m}: 预测结果为空")
        elif "predicted_next_flow" not in pred.columns:
            errors.append(f"{m}: 缺少 predicted_next_flow")
        else:
            neg = (pred["predicted_next_flow"] < 0).sum()
            if neg > 0:
                errors.append(f"{m}: {neg} 个负预测值")
        print(f"  {m}: model={b['model_name']}, rows={len(pred)}, "
              f"MAE={b['metrics']['mae']}, R2={b['metrics']['r2']}")
    
    # Step 3: 级联快照
    print("=" * 50)
    print("Step 3: 级联快照")
    bundle = train_realtime_model(ts, "lightgbm", train_days=3)
    has_cascade = False
    for step in range(0, 30, 3):
        res = build_realtime_snapshot(master, ts, bundle, adj, step)
        snap = res["snapshot"]
        fc = (snap["status"] == "fault").sum()
        cc = (snap["status"] == "crowded").sum()
        waves = len(res["cascade_info"]["waves"])
        if waves > 0:
            has_cascade = True
        print(f"  step={step:2d} slot={res['slot']:2d} | "
              f"fault={fc} crowded={cc} waves={waves}")
    
    if not has_cascade:
        errors.append("所有 step 均未产生级联波，阈值可能过高")
    
    # Step 4: BFS 级联
    print("=" * 50)
    print("Step 4: BFS 级联 (cascade.py)")
    top = ts.groupby("station_key")["total_flow"].sum().idxmax()
    r = run_cascade(master, ts, top, wind_mode=False)
    print(f"  source={top}, failed={r['failed_count']}, impacted={r['impacted_count']}")
    if r["failed_count"] == 0:
        errors.append("BFS 级联失效数为 0")
    
    # 结果
    print("\n" + "=" * 50)
    if errors:
        print(f"❌ 发现 {len(errors)} 个问题:")
        for e in errors:
            print(f"   • {e}")
    else:
        print("✅ 全部验证通过")
    
    return len(errors) == 0

if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)