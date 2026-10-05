#!/usr/bin/env python3
"""
bugcrowd_scope_monitor.py

Tracks scope of every public Bugcrowd engagement (bug_bounty + vdp) and sends a Telegram
alert when something changes:

  NEW IN SCOPE            target added to an in-scope group
  MOVED INTO SCOPE        target was out of scope (or in no group), now in scope
  REMOVED FROM SCOPE      in-scope target disappeared
  NOW OUT OF SCOPE        in-scope target moved to an out-of-scope group
  NEW OUT-OF-SCOPE        informational
  DESCRIPTION CHANGED     target description or scope-group description text changed
  REWARD CHANGES          P1-P4 payout bands of a scope group changed
  GROUPS ADDED/REMOVED

State: state/bugcrowd_scope_state.json (committed back by the workflow).

Safety rules:
  - first run = silent baseline (one summary message)
  - programs not seen before are baselined silently (set ANNOUNCE_NEW_PROGRAMS=1 to get
    their in-scope list instead)
  - fetch failure, or scope that suddenly comes back empty -> previous state is kept
  - Telegram failure -> that program's state is not advanced, so the alert is retried
  - exits non-zero if every program failed (so the Actions run goes red)
  - if a run sends zero real alerts, a silent heartbeat fires at most once every
    HEARTBEAT_INTERVAL_SECS, so you know the monitor is alive without being spammed

Stdlib only; uses curl for Bugcrowd requests.
"""
import hashlib
import html
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = "https://bugcrowd.com"
LIST_URL = f"{BASE}/engagements-us.json"
CATEGORIES = ("bug_bounty", "vdp")
PAGE_LIMIT = 24
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36")
STATE_FILE = os.environ.get("STATE_FILE", "state/bugcrowd_scope_state.json")
SCHEMA = 2
WORKERS = 4
MAX_PER_SECTION = 25
TG_CHUNK = 3800
ANNOUNCE_NEW_PROGRAMS = os.environ.get("ANNOUNCE_NEW_PROGRAMS", "0") == "1"
HEARTBEAT_INTERVAL_SECS = int(os.environ.get("HEARTBEAT_INTERVAL_SECS", str(6 * 3600)))
REWARD_KEYS = [f"p{i}{m}Cents" for i in range(1, 5) for m in ("Min", "Max")]


# --------------------------------------------------------------------------- fetching
def curl_get(url, accept="application/json, text/html"):
    try:
        r = subprocess.run(
            ["curl", "-sL", "--retry", "3", "--max-time", "30", "-A", UA,
             "-H", f"Accept: {accept}", url],
            capture_output=True, timeout=120)
    except subprocess.TimeoutExpired:
        return ""
    return r.stdout.decode("utf-8", "replace") if r.returncode == 0 else ""


def fetch_all_engagements():
    seen = {}
    for cat in CATEGORIES:
        page = 1
        while True:
            url = (f"{LIST_URL}?category={cat}&page={page}"
                   "&sort_by=promoted&sort_direction=desc")
            try:
                data = json.loads(curl_get(url, "*/*"))
            except ValueError:
                sys.exit(f"ERROR: unparseable list response for {cat} page {page}")
            recs = data.get("engagements") if isinstance(data, dict) else None
            if not isinstance(recs, list):
                sys.exit(f"ERROR: unexpected list response for {cat} page {page}")
            for r in recs:
                seen.setdefault(r["briefUrl"], r)
            if len(recs) < PAGE_LIMIT:
                break
            page += 1
    return list(seen.values())


def _norm_hash(s):
    return hashlib.sha1(" ".join((s or "").split()).encode()).hexdigest()[:12]


def parse_scope(doc):
    """Bugcrowd brief version document -> snapshot, or None if the shape is unexpected."""
    groups_raw = (doc.get("data") or {}).get("scope") if isinstance(doc, dict) else None
    if not isinstance(groups_raw, list):
        return None

    groups, targets = {}, {}
    # out-of-scope groups first so that, on a key collision, the in-scope entry wins
    for g in sorted(groups_raw, key=lambda g: g.get("inScope") is not False):
        gname = g.get("name") or "unnamed"
        in_scope = g.get("inScope") is not False
        rr = g.get("rewardRange")
        reward = None
        if isinstance(rr, dict) and any(rr.get(k) is not None for k in REWARD_KEYS):
            reward = {k: rr.get(k) for k in REWARD_KEYS}
        groups[gname] = {"in": in_scope, "reward": reward, "desc": _norm_hash(g.get("description"))}

        for t in g.get("targets") or []:
            name = t.get("name") or t.get("uri") or ""
            cat = t.get("category") or "unknown"
            targets[f"{cat}|{name}"] = {
                "in": in_scope, "group": gname, "name": name, "cat": cat,
                "uri": t.get("uri") or "", "desc": _norm_hash(t.get("description")),
            }
    return {"groups": groups, "targets": targets}


def fetch_scope(brief_url):
    """Returns a snapshot or None. Never raises."""
    try:
        page = curl_get(BASE + brief_url, "text/html")
        m = re.search(r'data-api-endpoints="([^"]+)"', page)
        if not m:
            return None
        endpoints = json.loads(html.unescape(m.group(1)))
        doc_path = endpoints["engagementBriefApi"]["getBriefVersionDocument"]
        if not doc_path.startswith("http"):
            doc_path = BASE + doc_path
        # the bare path returns {"errors": ...}; the .json form returns the document
        for url in (doc_path + ".json", doc_path):
            try:
                snap = parse_scope(json.loads(curl_get(url)))
            except ValueError:
                snap = None
            if snap is not None:
                time.sleep(0.2)
                return snap
    except Exception as e:  # noqa: BLE001 - one bad program must not kill the run
        print(f"WARN: scope fetch error for {brief_url}: {e}", file=sys.stderr)
    return None


# --------------------------------------------------------------------------- diffing
def fmt_reward(r):
    if not r:
        return "none"
    parts = []
    for i in range(1, 5):
        lo, hi = r.get(f"p{i}MinCents"), r.get(f"p{i}MaxCents")
        if hi is not None:
            lo_s = f"${lo // 100:,}" if lo is not None else "?"
            parts.append(f"P{i} {lo_s}-${hi // 100:,}")
    return " | ".join(parts) or "none"


def _t(t):
    s = f"• {t['name']} ({t['cat']}) [{t['group']}]"
    if t["uri"] and t["uri"] != t["name"]:
        s += f"\n    {t['uri']}"
    return s


def diff(old, new):
    ev = {k: [] for k in ("new_in", "moved_in", "removed", "moved_out", "new_out",
                          "desc", "reward", "groups_added", "groups_removed", "group_desc")}
    ot, nt = old["targets"], new["targets"]

    for k, t in nt.items():
        o = ot.get(k)
        if t["in"]:
            if o is None:
                ev["new_in"].append(_t(t))
            elif not o["in"]:
                ev["moved_in"].append(_t(t))
            elif o["desc"] != t["desc"]:
                ev["desc"].append(f"• {t['name']} ({t['cat']})")
        else:
            if o is None:
                ev["new_out"].append(_t(t))
            elif o["in"]:
                ev["moved_out"].append(_t(t))
    for k, o in ot.items():
        if k not in nt and o["in"]:
            ev["removed"].append(_t(o))

    og, ng = old["groups"], new["groups"]
    for n, g in ng.items():
        o = og.get(n)
        if o is None:
            ev["groups_added"].append(f"• {n} ({'in' if g['in'] else 'out of'} scope)")
            continue
        if g["in"] and o["reward"] != g["reward"]:
            ev["reward"].append(f"• {n}\n    {fmt_reward(o['reward'])}\n    → {fmt_reward(g['reward'])}")
        if o["desc"] != g["desc"]:
            ev["group_desc"].append(f"• {n}")
    for n in og:
        if n not in ng:
            ev["groups_removed"].append(f"• {n}")
    return ev


SECTIONS = [
    ("new_in", "🆕 NEW IN SCOPE"),
    ("moved_in", "➡️ MOVED INTO SCOPE"),
    ("removed", "❌ REMOVED FROM SCOPE"),
    ("moved_out", "🚫 NOW OUT OF SCOPE"),
    ("reward", "💰 REWARD CHANGES"),
    ("desc", "✏️ TARGET DESCRIPTION CHANGED"),
    ("group_desc", "✏️ SCOPE GROUP DESCRIPTION CHANGED"),
    ("groups_added", "📁 SCOPE GROUPS ADDED"),
    ("groups_removed", "📁 SCOPE GROUPS REMOVED"),
    ("new_out", "ℹ️ New out-of-scope entries"),
]


def format_message(name, link, ev):
    head = "🆕" if (ev["new_in"] or ev["moved_in"]) else "🔄"
    lines = [f"{head} Bugcrowd scope change", name, ""]
    for key, title in SECTIONS:
        items = ev[key]
        if not items:
            continue
        lines.append(f"{title} ({len(items)})")
        lines.extend(items[:MAX_PER_SECTION])
        if len(items) > MAX_PER_SECTION:
            lines.append(f"…and {len(items) - MAX_PER_SECTION} more")
        lines.append("")
    lines.append(link)
    return "\n".join(lines)


def format_new_program(name, link, snap):
    ins = [_t(t) for t in snap["targets"].values() if t["in"]]
    lines = ["🎯 New Bugcrowd program - in-scope items", name, "", f"IN SCOPE ({len(ins)})"]
    lines.extend(ins[:MAX_PER_SECTION])
    if len(ins) > MAX_PER_SECTION:
        lines.append(f"…and {len(ins) - MAX_PER_SECTION} more")
    lines += ["", link]
    return "\n".join(lines)


# --------------------------------------------------------------------------- telegram / state
def _chunks(text):
    cur = ""
    for line in text.split("\n"):
        if cur and len(cur) + len(line) + 1 > TG_CHUNK:
            yield cur
            cur = ""
        cur = f"{cur}\n{line}" if cur else line
    if cur:
        yield cur


def send_telegram(text, silent=False):
    token, chat = os.environ["TELEGRAM_BOT_TOKEN"], os.environ["TELEGRAM_CHAT_ID"]
    ok = True
    for chunk in _chunks(text):
        data = urllib.parse.urlencode({
            "chat_id": chat, "text": chunk, "disable_web_page_preview": "true",
            "disable_notification": "true" if silent else "false"}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=data)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                ok = ok and r.status == 200
        except Exception as e:  # noqa: BLE001
            print(f"WARN: telegram send failed: {e}", file=sys.stderr)
            ok = False
        time.sleep(1)
    return ok


def load_state():
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
        if st.get("version") == SCHEMA and isinstance(st.get("programs"), dict):
            return st["programs"], st.get("last_heartbeat", 0)
    except (OSError, ValueError):
        pass
    return {}, 0


def save_state(programs, last_heartbeat):
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    tmp = STATE_FILE + ".new"
    with open(tmp, "w") as f:
        json.dump({"version": SCHEMA, "programs": programs, "last_heartbeat": last_heartbeat},
                   f, sort_keys=True, indent=1)
    os.replace(tmp, STATE_FILE)


# --------------------------------------------------------------------------- main
def main():
    for var in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if not os.environ.get(var):
            sys.exit(f"Set {var} env var")

    engagements = fetch_all_engagements()
    old, last_heartbeat = load_state()
    first_run = not old

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        snaps = list(ex.map(lambda e: fetch_scope(e["briefUrl"]), engagements))

    new_state, fails, alerts = {}, 0, 0
    for eng, snap in zip(engagements, snaps):
        key, name = eng["briefUrl"], eng["name"]
        link = BASE + key
        prev = old.get(key)

        if snap is None or (prev and not snap["targets"] and prev["targets"]):
            print(f"WARN: no usable scope for {name} ({key}); keeping previous state", file=sys.stderr)
            fails += 1
            if prev:
                new_state[key] = prev
            continue

        snap["name"] = name
        if prev:
            ev = diff(prev, snap)
            if any(ev.values()):
                if send_telegram(format_message(name, link, ev)):
                    alerts += 1
                else:
                    snap = prev  # retry next run
        elif not first_run and ANNOUNCE_NEW_PROGRAMS:
            if not send_telegram(format_new_program(name, link, snap)):
                continue  # leave unseen so it is announced again next run
            alerts += 1
        new_state[key] = snap

    if not new_state:
        sys.exit("ERROR: nothing collected (scope extraction probably broken); state not overwritten")

    if first_run:
        send_telegram(f"✅ Bugcrowd scope monitor started - baseline of "
                      f"{len(new_state)}/{len(engagements)} programs saved")
        last_heartbeat = time.time()
    elif alerts == 0:
        now = time.time()
        if now - last_heartbeat >= HEARTBEAT_INTERVAL_SECS:
            send_telegram(f"✅ Bugcrowd scope check OK: no changes across "
                          f"{len(new_state)} programs ({fails} fetch problem(s))", silent=True)
            last_heartbeat = now

    save_state(new_state, last_heartbeat)
    print(f"Done: {len(engagements)} programs, {alerts} alert(s), {fails} fetch problem(s)")
    if engagements and fails == len(engagements):
        sys.exit(1)


if __name__ == "__main__":
    main()
