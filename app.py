#!/usr/bin/env python3
"""技術線型 + 模擬交易：本機後端
用法：
    pip install yfinance pandas
    python app.py            # 預設 http://127.0.0.1:8765
"""
import json
import os
import re
import ssl
import sys
import threading
import time
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import pandas as pd
import yfinance as yf

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8765

# tf -> (yfinance interval, period)   Yahoo 對分K有天數上限
TF = {
    "W": ("1wk", "10y"),
    "D": ("1d", "5y"),
    "60": ("60m", "700d"),
    "30": ("30m", "59d"),
    "15": ("15m", "59d"),
    "5": ("5m", "59d"),
}

_cache = {}
_lock = threading.Lock()
CACHE_SECONDS = 60


def candidates(sym: str):
    s = sym.strip().upper()
    if not s:
        return []
    if "." not in s and s[0].isdigit():  # 台股代號：先試上市 .TW 再試上櫃 .TWO
        return [s + ".TW", s + ".TWO"]
    return [s]


def fetch(sym: str, tf: str, fresh: bool = False):
    interval, period = TF[tf]
    last_err = None
    for cand in candidates(sym):
        key = (cand, tf)
        with _lock:
            hit = _cache.get(key)
        if hit and not fresh and time.time() - hit[0] < CACHE_SECONDS:
            return hit[1]
        try:
            df = yf.Ticker(cand).history(period=period, interval=interval, auto_adjust=False)
        except Exception as e:  # noqa
            last_err = e
            continue
        if df is None or df.empty:
            continue
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        idx = df.index
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)  # 保留交易所當地時間（台股即台北時間）
        if tf in ("D", "W"):
            idx = idx.normalize()
        ts = ((idx - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)).astype("int64")
        rows = {}
        for t, o, h, l, c, v in zip(ts, df["Open"], df["High"], df["Low"], df["Close"], df["Volume"]):
            rows[int(t)] = {
                "time": int(t),
                "open": round(float(o), 2),
                "high": round(float(h), 2),
                "low": round(float(l), 2),
                "close": round(float(c), 2),
                "volume": int(v),
            }
        bars = [rows[k] for k in sorted(rows)]
        result = {"symbol": cand, "tf": tf, "bars": bars}
        with _lock:
            _cache[key] = (time.time(), result)
        return result
    raise ValueError(f"查無資料：{sym}" + (f"（{last_err}）" if last_err else ""))


# ======================= 選股掃描 =======================
WATCHLIST = os.path.join(HERE, "watchlist.txt")
def new_job():
    return {"running": False, "done": 0, "total": 0, "results": [], "error": "", "finished": 0.0,
            "fetched": 0, "asof": "", "note": "", "req_asof": "", "universe": "", "ver": 8}


JOBS = {"long": new_job(), "short": new_job()}   # 做多、做空各自獨立的掃描工作
BATCH = 80


UNIVERSE_CACHE = os.path.join(HERE, "universe_cache.json")


def _http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        return urllib.request.urlopen(req, timeout=timeout).read()
    except Exception as e:  # noqa
        if "SSL" in str(e).upper() or "CERTIFICATE" in str(e).upper():
            # 部分電腦缺少憑證，改用不驗證憑證重試（僅用於讀取公開的股票代號清單）
            return urllib.request.urlopen(req, timeout=timeout, context=ssl._create_unverified_context()).read()
        raise


_ISIN_CACHE = {}


def _isin_html(mode):
    """證交所『國際證券辨識號碼』公開頁：mode=2 上市、mode=4 上櫃（Big5 編碼的 HTML）。10 分鐘內重複使用"""
    hit = _ISIN_CACHE.get(mode)
    if hit and time.time() - hit[0] < 600:
        return hit[1]
    html = _http_get(f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}").decode("cp950", errors="ignore")
    _ISIN_CACHE[mode] = (time.time(), html)
    return html


def _kind_of(code):
    """stock = 一般個股（4 碼、1~9 開頭，排除 91 開頭的 TDR）；etf = 00 開頭的受益憑證（含 00679B 這類債券 ETF）"""
    if re.fullmatch(r"(?!91)[1-9]\d{3}", code):   # 91xx 是台灣存託憑證（TDR），不算一般個股
        return "stock"
    if re.fullmatch(r"00\d{2,4}[A-Z]?", code):
        return "etf"
    return None


def _fetch_isin(mode, suffix, want="stock"):
    out = []
    html = _isin_html(mode)
    for code, name in re.findall(r"<td[^>]*>\s*([0-9A-Z]{4,6})[\u3000\s]+([^<]+?)\s*</td>", html):
        if _kind_of(code) == want:
            out.append((code + suffix, name.strip()))
    return out


def _fetch_openapi(want="stock"):
    out = []
    sources = [
        ("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", "Code", "Name", ".TW"),
        ("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
         "SecuritiesCompanyCode", "CompanyName", ".TWO"),
    ]
    for url, ck, nk, suf in sources:
        data = json.loads(_http_get(url).decode("utf-8"))
        for r in data:
            c = str(r.get(ck, "")).strip()
            if _kind_of(c) == want:
                out.append((c + suf, str(r.get(nk, "")).strip()))
    return out


ETF_CACHE = os.path.join(HERE, "universe_etf_cache.json")
MARKETS = {"all": ((2, ".TW"), (4, ".TWO")), "twse": ((2, ".TW"),), "tpex": ((4, ".TWO"),),
           "etf": ((2, ".TW"), (4, ".TWO"))}
MIN_COUNT = {"all": 500, "twse": 400, "tpex": 200, "etf": 50}
MARKET_NAME = {"all": "上市櫃", "twse": "上市", "tpex": "上櫃", "etf": "ETF"}


def _cache_path(kind):
    return ETF_CACHE if kind == "etf" else UNIVERSE_CACHE


def _suffix(sym):
    return sym[sym.index("."):] if "." in sym else ""


def load_universe(kind: str):
    """回傳 ([(yahoo代號, 名稱)], 說明文字)。
    kind: all=上市+上櫃個股, twse=只有上市(.TW), tpex=只有上櫃(.TWO), etf=上市櫃 ETF, watch=watchlist.txt"""
    note = ""
    if kind in MARKETS:
        modes = MARKETS[kind]
        want = "etf" if kind == "etf" else "stock"
        allowed = {suf for _, suf in modes}
        need = MIN_COUNT[kind]
        items, errs = [], []
        for mode, suf in modes:
            try:
                items += _fetch_isin(mode, suf, want)
            except Exception as e:  # noqa
                errs.append(f"ISIN({mode})：{e}")
        if len(items) < need:
            try:
                items = [x for x in _fetch_openapi(want) if _suffix(x[0]) in allowed]
            except Exception as e:  # noqa
                errs.append(f"OpenAPI：{e}")
        items = list(dict(items).items())
        if len(items) >= need:
            if kind in ("all", "etf"):
                try:
                    with open(_cache_path(kind), "w", encoding="utf-8") as f:
                        json.dump(items, f, ensure_ascii=False)
                except Exception:  # noqa
                    pass
            print(f"取得{MARKET_NAME[kind]}股票清單：{len(items)} 檔")
            return items, ""
        for e in errs:
            print("取得股票清單失敗 →", e)
        if os.path.exists(_cache_path(kind)):
            try:
                with open(_cache_path(kind), encoding="utf-8") as f:
                    cached = [tuple(x) for x in json.load(f) if _suffix(x[0]) in allowed]
                if len(cached) >= need:
                    return cached, f"無法連線更新股票清單，改用上次儲存的{MARKET_NAME[kind]}清單（{len(cached)} 檔）"
            except Exception:  # noqa
                pass
        note = f"無法取得{MARKET_NAME[kind]}股票清單，已改用 watchlist.txt（原因請見終端機）"
    items = []
    if os.path.exists(WATCHLIST):
        with open(WATCHLIST, encoding="utf-8") as f:
            for line in f:
                line = line.split("#")[0].strip()
                if not line:
                    continue
                parts = line.split()
                code, name = parts[0].upper(), (parts[1] if len(parts) > 1 else "")
                if "." not in code and code[0].isdigit():
                    code += ".TW"
                items.append((code, name))
    return items, note


def truncate(d, asof):
    """回放掃描：只保留 asof（含）以前的日K，模擬「當天收盤後」的掃描結果"""
    if not asof:
        return d
    idx = d.index.tz_localize(None) if getattr(d.index, "tz", None) is not None else d.index
    return d[idx.normalize() <= pd.Timestamp(asof)]


NAMES = {}
_no_name = set()
_names_lock = threading.Lock()
_names_stamp = 0.0


def ensure_names():
    """載入「代號 → 股票名稱」對照表（優先讀本機快取，沒有才連線取得；失敗 5 分鐘內不重試）"""
    global _names_stamp
    with _names_lock:
        now = time.time()
        if len(NAMES) >= 500 and now - _names_stamp < 86400:
            return
        if _names_stamp and now - _names_stamp < 300:
            return
        _names_stamp = now
        items = []
        try:
            if os.path.exists(UNIVERSE_CACHE):
                with open(UNIVERSE_CACHE, encoding="utf-8") as f:
                    items = [tuple(x) for x in json.load(f)]
        except Exception:  # noqa
            items = []
        if len(items) < 500:
            try:
                items, _ = load_universe("all")
            except Exception as e:  # noqa
                print("載入股票名稱失敗：", e)
                items = []
        etf_items = []
        try:
            if os.path.exists(ETF_CACHE):
                with open(ETF_CACHE, encoding="utf-8") as f:
                    etf_items = [tuple(x) for x in json.load(f)]
            if len(etf_items) < 50:
                etf_items, _ = load_universe("etf")
        except Exception as e:  # noqa
            print("載入 ETF 名稱失敗：", e)
            etf_items = []
        try:
            items = list(items) + list(etf_items) + load_universe("watch")[0]
        except Exception:  # noqa
            pass
        for code, name in items:
            if name:
                NAMES[code] = name


def get_name(sym):
    ensure_names()
    n = NAMES.get(sym, "")
    if not n and sym and not sym[0].isdigit() and sym not in _no_name:  # 美股、指數等：向 Yahoo 查簡稱
        try:
            info = yf.Ticker(sym).info or {}
            n = info.get("shortName") or info.get("longName") or ""
        except Exception:  # noqa
            n = ""
        if n:
            NAMES[sym] = n
        else:
            _no_name.add(sym)
    return n


def download_batch(tickers, period="1y"):
    df = yf.download(tickers, period=period, interval="1d", auto_adjust=False,
                     group_by="ticker", threads=True, progress=False)
    out = {}
    for t in tickers:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                if t not in set(df.columns.get_level_values(0)):
                    continue
                d = df[t]
            else:
                d = df
            d = d.dropna(subset=["Open", "High", "Low", "Close"])
            if "Volume" in d.columns:
                d = d[d["Volume"] > 0]
            if len(d):
                out[t] = d
        except Exception:  # noqa
            pass
    return out


def check_series(d, flags, lookback, side="long", direction="up", mas=None):
    """日K、週K共用的四個條件。flags=(f1,f2,f3,f4)；d 最後一列 = 最新（含進行中）的K棒。
    做多：1 紅K　2 現價 > 前兩根K棒開盤與收盤（四個價位最高者）　3 均線排列：mas 依序的 MA 逐一 >（預設 5>13>21>55）
    做空：1 綠K　2 現價 < 前兩根K棒開盤與收盤（四個價位最低者）　3 均線排列：mas 依序的 MA 逐一 <（預設 5<13<21）
    mas 是自訂的均線週期清單，只填三個就只比較三個。
    4 最近 lookback 根內，5MA 與 13MA 發生交叉（direction='up' 由下往上穿越、'down' 由上往下跌破）
    回傳 (是否符合, 資訊dict)"""
    f1, f2, f3, f4 = flags
    long_ = side == "long"
    mas = [int(m) for m in (mas or ([5, 13, 21, 55] if long_ else [5, 13, 21])) if int(m) > 0]
    f3 = f3 and len(mas) >= 2          # 少於兩個週期就無從比較，視為不啟用
    need = 3
    if f3:
        need = max(need, max(mas) + 1)
    if f4:
        need = max(need, 14 + lookback)
    if len(d) < need:
        return False, {}
    c, o = d["Close"], d["Open"]
    ma = {n: c.rolling(n).mean() for n in set([5, 13] + mas)}
    info = {"cross_ago": None}
    if f1:
        red_or_green = c.iloc[-1] > o.iloc[-1] if long_ else c.iloc[-1] < o.iloc[-1]
        if not red_or_green:
            return False, info
    if f2:
        prev = [o.iloc[-2], c.iloc[-2], o.iloc[-3], c.iloc[-3]]
        if not (c.iloc[-1] > max(prev) if long_ else c.iloc[-1] < min(prev)):
            return False, info
    if f3:
        vals = [ma[n].iloc[-1] for n in mas]
        ok = all(x > y for x, y in zip(vals, vals[1:])) if long_ else all(x < y for x, y in zip(vals, vals[1:]))  # 有 NaN 時為 False
        if not ok:
            return False, info
    if f4:
        up = long_ or direction != "down"
        hit = None
        for k in range(lookback):
            i = -1 - k
            a0, b0, a1, b1 = ma[5].iloc[i], ma[13].iloc[i], ma[5].iloc[i - 1], ma[13].iloc[i - 1]
            crossed = (a0 > b0 and a1 <= b1) if up else (a0 < b0 and a1 >= b1)
            if crossed:
                hit = k
                break
        if hit is None:
            return False, info
        info["cross_ago"] = hit
    return True, info


def evaluate(d, p):
    """先判斷日K條件，通過後再由日K合成週K判斷週K條件。p["side"] = long / short"""
    side = p.get("side", "long")
    # 5. 成交量大於 N 張（Yahoo 的成交量單位是股，1 張 = 1000 股；取最新一根日K，盤中為當下累計量）
    if p.get("c5") and not d["Volume"].iloc[-1] / 1000 > p.get("minvol", 500):
        return None
    ok, info = check_series(d, (p["c1"], p["c2"], p["c3"], p["c4"]), p["lookback"], side, p.get("dir", "up"), p.get("ma"))
    if not ok:
        return None
    out = {"cross_days_ago": info["cross_ago"] if p["c4"] else None, "cross_weeks_ago": None}
    if p["w1"] or p["w2"] or p["w3"] or p["w4"]:
        w = d.resample("W-FRI").agg({"Open": "first", "High": "max", "Low": "min",
                                     "Close": "last", "Volume": "sum"}).dropna()
        wok, winfo = check_series(w, (p["w1"], p["w2"], p["w3"], p["w4"]), p["wlookback"], side, p.get("wdir", "up"), p.get("wma"))
        if not wok:
            return None
        out["cross_weeks_ago"] = winfo["cross_ago"] if p["w4"] else None
    c = d["Close"]
    prev = float(c.iloc[-2])
    out.update({
        "date": str(d.index[-1].date()),
        "close": round(float(c.iloc[-1]), 2),
        "chg": round((float(c.iloc[-1]) / prev - 1) * 100, 2) if prev else 0,
        "volume": int(d["Volume"].iloc[-1]),
    })
    return out


def run_scan(p):
    job = JOBS[p["side"]]
    job.update(running=True, done=0, total=0, results=[], error="", fetched=0, asof="", note="", req_asof=p.get("asof", ""), universe=p["universe"])
    try:
        weekly = p["w1"] or p["w2"] or p["w3"] or p["w4"]
        period = "3y" if weekly else ("2y" if p.get("asof") else "1y")
        uni, note = load_universe(p["universe"])
        job["note"] = note
        if not uni:
            raise ValueError("股票清單是空的（請檢查網路，或在 watchlist.txt 填入代號）")
        names = dict(uni)
        tickers = [t for t, _ in uni]
        job["total"] = len(tickers)
        results = []
        for i in range(0, len(tickers), BATCH):
            chunk = tickers[i:i + BATCH]
            try:
                data = download_batch(chunk, period)
            except Exception as e:  # noqa
                print("批次下載失敗：", e)
                data = {}
            if not data:  # 整批都抓不到：可能被限流，改逐檔抓一次
                for t in chunk:
                    try:
                        d1 = yf.Ticker(t).history(period=period, interval="1d", auto_adjust=False)
                        d1 = d1.dropna(subset=["Open", "High", "Low", "Close"])
                        if "Volume" in d1.columns:
                            d1 = d1[d1["Volume"] > 0]
                        if len(d1):
                            data[t] = d1
                    except Exception as e:  # noqa
                        print("逐檔下載失敗：", t, e)
            # 自選股裡沒標上市/上櫃的，.TW 抓不到就改試 .TWO
            missing = [t for t in chunk if t not in data and t.endswith(".TW") and p["universe"] == "watch"]
            if missing:
                alt = [t[:-3] + ".TWO" for t in missing]
                try:
                    for t2, d2 in download_batch(alt, period).items():
                        data[t2] = d2
                        names[t2] = names.get(t2[:-4] + ".TW", "")
                except Exception:  # noqa
                    pass
            for t, d in data.items():
                d = truncate(d, p.get("asof"))
                if not len(d):
                    continue
                try:
                    r = evaluate(d, p)
                except Exception:  # noqa
                    r = None
                if r:
                    r.update(symbol=t, name=names.get(t, ""))
                    results.append(r)
            job["fetched"] += len(data)
            for d in data.values():
                d = truncate(d, p.get("asof"))
                if len(d):
                    day = str(d.index[-1].date())
                    if day > job["asof"]:
                        job["asof"] = day
            print(f"進度 {min(i + BATCH, len(tickers))}/{len(tickers)}　本批取得資料 {len(data)} 檔　目前符合 {len(results)} 檔")
            job["done"] = min(i + BATCH, len(tickers))
            job["results"] = sorted(results, key=lambda x: -x["chg"])
        if not job["fetched"]:
            job["error"] = "沒有抓到任何股價資料（可能是網路、被 Yahoo 限流，或套件版本問題），請查看終端機的訊息"
    except Exception as e:  # noqa
        job["error"] = str(e)
    finally:
        job["running"] = False
        job["finished"] = time.time()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8")
        if u.path == "/api/ohlc":
            q = parse_qs(u.query)
            sym = (q.get("symbol") or [""])[0]
            tf = (q.get("tf") or ["D"])[0]
            if tf not in TF:
                return self._send(400, json.dumps({"error": "不支援的週期"}))
            try:
                res = dict(fetch(sym, tf, (q.get("fresh") or ["0"])[0] == "1"))
                res["name"] = get_name(res["symbol"])
                return self._send(200, json.dumps(res, ensure_ascii=False))
            except Exception as e:  # noqa
                return self._send(404, json.dumps({"error": str(e)}, ensure_ascii=False))
        if u.path == "/api/scan/start":
            q = parse_qs(u.query)
            g = lambda k, d: (q.get(k) or [d])[0]  # noqa
            side = g("side", "long") if g("side", "long") in JOBS else "long"
            job = JOBS[side]
            if job["running"]:
                return self._send(200, json.dumps({"ok": True, "already": True}))
            def plist(k):  # "5,13,21,55" -> [5, 13, 21, 55]；空白或無效的略過
                out = []
                for x in g(k, "").split(","):
                    x = x.strip()
                    if x.isdigit() and 0 < int(x) <= 500:
                        out.append(int(x))
                return out[:6] or None
            p = {"ma": plist("ma"), "wma": plist("wma"), "side": side, "dir": "down" if g("dir", "up") == "down" else "up",
                 "wdir": "down" if g("wdir", "up") == "down" else "up", "universe": g("universe", "watch"), "lookback": max(1, int(g("lookback", "5"))),
                 "c1": g("c1", "1") == "1", "c2": g("c2", "1") == "1",
                 "c3": g("c3", "1") == "1", "c4": g("c4", "1") == "1",
                 "c5": g("c5", "0") == "1", "minvol": max(0.0, float(g("minvol", "500"))),
                 "w1": g("w1", "0") == "1", "w2": g("w2", "0") == "1",
                 "w3": g("w3", "0") == "1", "w4": g("w4", "0") == "1",
                 "wlookback": max(1, int(g("wlookback", "3"))),
                 "asof": g("asof", "") if re.fullmatch(r"\d{4}-\d{2}-\d{2}", g("asof", "")) else ""}
            job.update(running=True, done=0, total=0, results=[], error="", fetched=0, asof="", note="", req_asof=p["asof"], universe=p["universe"])  # 先標記執行中
            threading.Thread(target=run_scan, args=(p,), daemon=True).start()
            return self._send(200, json.dumps({"ok": True}))
        if u.path == "/api/scan/status":
            sd = (parse_qs(u.query).get("side") or ["long"])[0]
            return self._send(200, json.dumps(JOBS.get(sd, JOBS["long"]), ensure_ascii=False))
        self._send(404, json.dumps({"error": "not found"}))


if __name__ == "__main__":
    threading.Thread(target=ensure_names, daemon=True).start()   # 預先載入股票名稱
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    url = f"http://127.0.0.1:{PORT}"
    print("啟動中：", url, "（Ctrl+C 結束）")
    threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
