#!/usr/bin/env python3
"""Slow, literal reference for rules.md. Usage: ref.py DATA_DIR OUT_DIR (writes user_daily.csv, campaign_daily.csv)."""
import csv
import glob
import json
import os
import sys
from collections import defaultdict

DAY = 86_400_000
DATA, OUT = sys.argv[1], sys.argv[2]


def split_line(line):
    """bytes line (no \n) -> list of bytes values (None for empty) or None if malformed."""
    if line == b"":
        return None
    out = []
    for f in line.split(b","):
        if f.startswith(b'"'):
            if len(f) < 2 or not f.endswith(b'"') or b'"' in f[1:-1]:
                return None
            f = f[1:-1]
        elif b'"' in f:
            return None
        out.append(f if f != b"" else None)
    return out


def read_rows(pattern, nfields):
    rows = []
    for path in sorted(glob.glob(pattern)):
        data = open(path, "rb").read()
        lines = data.split(b"\n")
        if data.endswith(b"\n"):
            lines = lines[:-1]
        for line in lines[1:]:
            r = split_line(line)
            if r is None or len(r) != nfields:
                continue
            rows.append(r)
    return rows


def isdig(b):
    return len(b) > 0 and all(48 <= c <= 57 for c in b)


def days_from_civil(y, m, d):
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def parse_ts(b):
    if b is None:
        return None
    if isdig(b):
        return int(b)
    if len(b) not in (20, 25):
        return None
    s = b
    if not (s[4:5] == b"-" and s[7:8] == b"-" and s[10:11] == b"T" and s[13:14] == b":" and s[16:17] == b":"):
        return None
    parts = [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19]]
    if not all(isdig(p) for p in parts):
        return None
    y, mo, d, h, mi, se = (int(p) for p in parts)
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    mdays = [31, 29 if leap(y) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1]
    if not (1 <= d <= mdays):
        return None
    off = 0
    if len(s) == 20:
        if s[19:20] != b"Z":
            return None
    else:
        sign = s[19:20]
        if sign not in (b"+", b"-") or s[22:23] != b":" or not isdig(s[20:22]) or not isdig(s[23:25]):
            return None
        oh, om = int(s[20:22]), int(s[23:25])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000 * (1 if sign == b"+" else -1)
    ms = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000
    return ms - off


def iso_ms(s):
    return parse_ts(s.replace("+00:00", "Z").encode())


man = json.load(open(os.path.join(DATA, "manifest.json")))
WS, WE = iso_ms(man["window_start"]), iso_ms(man["window_end"])
CWS = WS - 7 * DAY

products = {}
for r in read_rows(os.path.join(DATA, "products.csv"), 2):
    products[r[0]] = r[1]
campaigns = {}
for r in read_rows(os.path.join(DATA, "campaigns.csv"), 2):
    campaigns[r[0]] = r[1]

# links
parent = {}


def find(x):
    parent.setdefault(x, x)
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


for r in read_rows(os.path.join(DATA, "links", "*.csv"), 2):
    a, b = r
    if a is None or b is None:
        continue
    ra, rb = find(a), find(b)
    if ra != rb:
        if rb < ra:
            ra, rb = rb, ra
        parent[rb] = ra  # root is always the smallest id of the component


def person(u):
    return find(u) if u in parent else u


# events
best = {}
for r in read_rows(os.path.join(DATA, "events", "*.csv"), 7):
    eid, uid, ts, ing, et, pid, price = r
    if eid is None:
        continue
    ingv = parse_ts(ing)
    cur = best.get(eid)
    if cur is None or ingv < cur[0]:
        best[eid] = (ingv, r)

events = defaultdict(list)  # person -> list of (ts, price or 0, category)
for eid, (ingv, r) in best.items():
    _, uid, ts, ing, et, pid, price = r
    if uid is None or pid is None:
        continue
    t = parse_ts(ts)
    if t is None or not (WS <= t < WE):
        continue
    if et not in (b"view", b"cart", b"purchase"):
        continue
    pc = 0
    if et == b"purchase":
        if price is None or not isdig(price):
            continue
        pc = int(price)
        if not (1 <= pc <= 10_000_000):
            continue
    if ingv - t > DAY:
        continue
    cat = products.get(pid, b"unknown")
    events[person(uid)].append((t, pc, cat))

clicks = defaultdict(list)  # person -> list of (ts, click_id, campaign)
seen = set()
for r in read_rows(os.path.join(DATA, "clicks", "*.csv"), 4):
    cid, uid, camp, ts = r
    if cid is None or cid in seen:
        continue
    seen.add(cid)
    if uid is None or camp not in campaigns:
        continue
    t = parse_ts(ts)
    if t is None or not (CWS <= t < WE):
        continue
    clicks[person(uid)].append((t, cid, camp))

user_daily = {}
camp_daily = defaultdict(lambda: [0, 0, 0, 0])
for p, evs in events.items():
    evs.sort(key=lambda e: e[0])
    sessions = []
    prev = None
    for e in evs:
        if prev is None or e[0] - prev > 1_800_000:
            sessions.append([])
        sessions[-1].append(e)
        prev = e[0]
    cl = clicks.get(p, [])
    for s in sessions:
        start = s[0][0]
        day = start // DAY
        rev = sum(e[1] for e in s)
        purch = any(e[1] > 0 for e in s)
        q = [c for c in cl if c[0] <= start and start - c[0] <= 7 * DAY]
        if q:
            mt = max(c[0] for c in q)
            credit = min(c[1] for c in q if c[0] == mt)
            camp = [c[2] for c in q if c[0] == mt and c[1] == credit][0]
        else:
            camp = b"organic"
        cd = camp_daily[(camp, day)]
        cd[0] += 1
        cd[1] += int(purch)
        cd[2] += rev
        for c in {c[2] for c in q}:
            camp_daily[(c, day)][3] += 1
        ud = user_daily.setdefault((p, day), [0, 0, 0, defaultdict(int)])
        ud[0] += 1
        ud[1] += len(s)
        ud[2] += rev
        for e in s:
            if e[1] > 0:
                ud[3][e[2]] += e[1]

os.makedirs(OUT, exist_ok=True)


def datestr(day):
    import datetime
    return (datetime.date(1970, 1, 1) + datetime.timedelta(days=day)).isoformat()


def dec(b):
    return b.decode("utf-8", "surrogateescape")


with open(os.path.join(OUT, "user_daily.csv"), "w", newline="", encoding="utf-8", errors="surrogateescape") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
    for (p, day), (ns, ne, rev, cats) in user_daily.items():
        top = ""
        if cats:
            m = max(cats.values())
            top = dec(min(c for c, v in cats.items() if v == m))
        w.writerow([dec(p), datestr(day), ns, ne, rev, top])

with open(os.path.join(OUT, "campaign_daily.csv"), "w", newline="", encoding="utf-8", errors="surrogateescape") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
    for (c, day), v in camp_daily.items():
        ch = b"organic" if c == b"organic" else campaigns[c]
        w.writerow([dec(c), dec(ch), datestr(day), *v])
