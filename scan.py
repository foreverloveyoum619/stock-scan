#!/usr/bin/env python3
"""手機版掃描：沿用 app.py 的邏輯，做多/做空各掃一次，結果寫入 result.json"""
import datetime, json, os, sys
sys.argv = sys.argv[:1]
import app

TZ = datetime.timezone(datetime.timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "result.json")
# 固定條件：上市櫃個股、日K四項全開、5日內交叉、量>500張、週K前兩項
BASE = dict(ma=None, wma=None, universe="all", lookback=5, c1=True, c2=True, c3=True, c4=True,
            c5=True, minvol=500.0, w1=True, w2=True, w3=False, w4=False, wlookback=3, wdir="up", asof="")

now = datetime.datetime.now(TZ).isoformat(timespec="seconds")
res = {"updated": now}
for side, direction in (("long", "up"), ("short", "down")):
    app.run_scan({**BASE, "side": side, "dir": direction})
    j = app.JOBS[side]
    res[side] = {k: j[k] for k in ("results", "asof", "error", "fetched", "total", "note")}
bad = [s for s in ("long", "short") if res[s]["fetched"] == 0 or res[s]["error"]]
if bad:  # 失敗：保留上次成功的結果，只標記錯誤
    old = {}
    if os.path.exists(OUT):
        old = json.load(open(OUT, encoding="utf-8"))
    old.setdefault("updated", "")
    old["failed_at"] = now
    old["error"] = "資料抓取失敗（" + "、".join(bad) + "），顯示的是上次成功的結果；" + (res[bad[0]]["error"] or "")
    res = old
else:
    res["error"] = ""
json.dump(res, open(OUT, "w", encoding="utf-8"), ensure_ascii=False)
print("完成", now, "做多", len(res.get("long", {}).get("results", [])), "做空", len(res.get("short", {}).get("results", [])))
