#!/usr/bin/env python3
"""
intigriti_scope_monitor.py

Tracks the scope of every Intigriti program visible to your API token (private invites
included) and sends a Telegram alert when something changes:

  NEW IN SCOPE            asset added (any tier except "Out Of Scope")
  MOVED INTO SCOPE        asset was "Out Of Scope", now in scope
  REMOVED FROM SCOPE      in-scope asset disappeared
  NOW OUT OF SCOPE        in-scope asset re-tiered to "Out Of Scope"
  TIER CHANGES            e.g. Tier 3 -> Tier 1 (payout band changed)
  ASSET DESCRIPTION CHANGED
  RULES CHANGED           rules-of-engagement text, testing requirements, safe harbour,
                          automated-tooling flag
  PROGRAM CHANGES         bounty range, confidentiality level
  NEW OUT-OF-SCOPE        informational

State: state/intigriti_scope_state.json (committed back by the workflow).

Safety rules:
  - first run = silent baseline (one summary message)
  - programs not seen before are baselined silently (ANNOUNCE_NEW_PROGRAMS=1 sends their
    in-scope list instead)
  - failed fetch / non-200 / scope suddenly empty -> previous state is kept
  - Telegram failure -> that program's state is not advanced (alert retried next run)
  - exits non-zero if every program failed
  - if a run sends zero real alerts, a silent heartbeat fires at most once every
    HEARTBEAT_INTERVAL_SECS, so you know the monitor is alive without being spammed

Stdlib + curl only. The API token is passed to curl on stdin, not on the command line.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

API_BASE = os.environ.get("INTIGRITI_API_BASE", "https://api.intigriti.com/external/researcher/v1")
TELEGRAM_API = os.environ.get("TELEGRAM_API", "https://api.telegram.org")
STATE_FILE = os.environ.get("STATE_FILE", "state/intigriti_scope_state.json")
SCHEMA = 1
LIST_LIMIT = 100
WORKERS = int(os.environ.get("WORKERS", "3"))
MAX_PER_SECTION = 25
TG_CHUNK = 3800
ANNOUNCE_NEW_PROGRAMS = os.environ.get("ANNOUNCE_NEW_PROGRAMS", "0") == "1"
HEARTBEAT_INTERVAL_SECS = int(os.environ.get("HEARTBEAT_INTERVAL_SECS", str(6 * 3600)))
OUT_OF_SCOPE_RE = re.compile(r"out[\s_-]*of[\s_-]*scope", re.I)


# --------------------------------------------------------------------------- fetching
def api_get(path):
    """GET JSON from the Intigriti API; None on any failure."""
    token = os.environ["INTIGRITI_API_TOKEN"]
    try:
        r = subprocess.run(
            ["curl", "-s", "--retry", "3", "--max-time", "30",
             "-w", "\n%{http_code}", "-H", "@-", f"{API_BASE}{path}"],
            input=f"Authorization: Bearer {token}\n".encode(),
            capture_output=True, timeout=180)
    except subprocess.TimeoutExpired:
        return None
    if r.returncode != 0:
        return None
    body, _, code = r.stdout.decode("utf-8", "replace").rpartition("\n")
    if code != "200":
        print(f"WARN: HTTP {code} for {path}", file=sys.stderr)
        return None
    try:
        return json.loads(body)
    except ValueError:
        return None


def fetch_all_programs():
    records, offset = [], 0
    while True:
        page = api_get(f"/programs?limit={LIST_LIMIT}&offset={offset}")
        recs = page.get("records") if isinstance(page, dict) else None
        if not isinstance(recs, list):
            sys.exit("ERROR: unexpected response from the programs list endpoint")
        records.extend(recs)
        if len(recs) < LIST_LIMIT:
            return records
        offset += LIST_LIMIT


def _h(x):
    """Stable short hash of any JSON-ish value, whitespace-normalised."""
    if x is None:
        x = ""
    if not isinstance(x, str):
        x = json.dumps(x, sort_keys=True)
    return hashlib.sha1(" ".join(x.split()).encode()).hexdigest()[:12]


def _val(x):
    """Intigriti enums look like {"id": 1, "value": "Tier 1"}."""
    return (x.get("value") if isinstance(x, dict) else x) or ""


def parse_detail(detail, rec):
    """Program detail + list record -> snapshot, or None if the shape is unexpected."""
    content = (detail.get("domains") or {}).get("content") if isinstance(detail, dict) else None
    if not isinstance(content, list):
        return None

    assets = {}
    out_flags = [(a, bool(OUT_OF_SCOPE_RE.search(_val(a.get("tier"))))) for a in content]
    # out-of-scope first, so an in-scope duplicate of the same endpoint wins
    for a, is_out in sorted(out_flags, key=lambda p: not p[1]):
        typ = _val(a.get("type")) or "unknown"
        endpoint = a.get("endpoint") or ""
        assets[f"{typ}|{endpoint}"] = {
            "type": typ, "endpoint": endpoint, "tier": _val(a.get("tier")) or "unknown",
            "in": not is_out, "desc": _h(a.get("description")),
        }

    roe = ((detail.get("rulesOfEngagement") or {}).get("content")) or {}
    testing = roe.get("testingRequirements")
    tooling = testing.get("automatedTooling") if isinstance(testing, dict) else None
    rules = {
        "desc": _h(roe.get("description")),
        "testing": _h(testing),
        "safe_harbour": roe.get("safeHarbour"),
        "tooling": _val(tooling) if tooling is not None else None,
    }

    lo, hi = rec.get("minBounty") or {}, rec.get("maxBounty") or {}
    return {
        "assets": assets,
        "rules": rules,
        "program": {
            "bounty": f"{lo.get('value', '?')} - {hi.get('value', '?')} {hi.get('currency', '')}".strip(),
            "confidentiality": _val(detail.get("confidentialityLevel")),
        },
    }


def fetch_snapshot(rec):
    """Never raises; returns a snapshot or None."""
    try:
        detail = api_get(f"/programs/{rec['id']}")
        time.sleep(0.2)
        return parse_detail(detail, rec) if detail is not None else None
    except Exception as e:  # noqa: BLE001 - one bad program must not kill the run
        print(f"WARN: error fetching {rec.get('name')}: {e}", file=sys.stderr)
        return None


# --------------------------------------------------------------------------- diffing
def _a(a):
    return f"• {a['endpoint']}  ({a['type']}, {a['tier']})"


def diff(old, new):
    ev = {k: [] for k in ("new_in", "moved_in", "removed", "moved_out", "tier", "desc",
                          "rules", "program", "new_out")}
    oa, na = old["assets"], new["assets"]

    for k, a in na.items():
        o = oa.get(k)
        if a["in"]:
            if o is None:
                ev["new_in"].append(_a(a))
            elif not o["in"]:
                ev["moved_in"].append(_a(a))
            else:
                if o["tier"] != a["tier"]:
                    ev["tier"].append(f"• {a['endpoint']}  ({a['type']}): {o['tier']} → {a['tier']}")
                if o["desc"] != a["desc"]:
                    ev["desc"].append(f"• {a['endpoint']}  ({a['type']})")
        else:
            if o is None:
                ev["new_out"].append(_a(a))
            elif o["in"]:
                ev["moved_out"].append(_a(a))
    for k, o in oa.items():
        if k not in na and o["in"]:
            ev["removed"].append(_a(o))

    orr, nrr = old["rules"], new["rules"]
    if orr["desc"] != nrr["desc"]:
        ev["rules"].append("• Rules-of-engagement description changed")
    if orr["testing"] != nrr["testing"]:
        ev["rules"].append("• Testing requirements changed")
    if orr["safe_harbour"] != nrr["safe_harbour"]:
        ev["rules"].append(f"• Safe harbour: {orr['safe_harbour']} → {nrr['safe_harbour']}")
    if orr["tooling"] != nrr["tooling"]:
        ev["rules"].append(f"• Automated tooling: {orr['tooling']} → {nrr['tooling']}")

    op, np_ = old["program"], new["program"]
    if op["bounty"] != np_["bounty"]:
        ev["program"].append(f"• Bounty range: {op['bounty']} → {np_['bounty']}")
    if op["confidentiality"] != np_["confidentiality"]:
        ev["program"].append(f"• Confidentiality: {op['confidentiality']} → {np_['confidentiality']}")
    return ev


SECTIONS = [
    ("new_in", "🆕 NEW IN SCOPE"),
    ("moved_in", "➡️ MOVED INTO SCOPE"),
    ("tier", "📊 TIER CHANGES"),
    ("removed", "❌ REMOVED FROM SCOPE"),
    ("moved_out", "🚫 NOW OUT OF SCOPE"),
    ("program", "💰 PROGRAM CHANGES"),
    ("rules", "📜 RULES CHANGED"),
    ("desc", "✏️ ASSET DESCRIPTION CHANGED"),
    ("new_out", "ℹ️ New out-of-scope entries"),
]


def format_message(name, link, ev):
    head = "🆕" if (ev["new_in"] or ev["moved_in"]) else "🔄"
    lines = [f"{head} Intigriti scope change", name, ""]
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
    ins = [_a(a) for a in snap["assets"].values() if a["in"]]
    lines = ["🎯 New Intigriti program - in-scope assets", name, "", f"IN SCOPE ({len(ins)})"]
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
        req = urllib.request.Request(f"{TELEGRAM_API}/bot{token}/sendMessage", data=data)
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
    for var in ("INTIGRITI_API_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if not os.environ.get(var):
            sys.exit(f"Set {var} env var")

    programs = fetch_all_programs()
    old, last_heartbeat = load_state()
    first_run = not old

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        snaps = list(ex.map(fetch_snapshot, programs))

    new_state, fails, alerts = {}, 0, 0
    for rec, snap in zip(programs, snaps):
        pid, name = str(rec["id"]), rec.get("name", "?")
        link = (rec.get("webLinks") or {}).get("detail", "")
        prev = old.get(pid)

        if snap is None or (prev and not snap["assets"] and prev["assets"]):
            print(f"WARN: no usable scope for {name} ({pid}); keeping previous state", file=sys.stderr)
            fails += 1
            if prev:
                new_state[pid] = prev
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
        new_state[pid] = snap

    if not new_state:
        sys.exit("ERROR: nothing collected; state not overwritten")

    if first_run:
        send_telegram(f"✅ Intigriti scope monitor started - baseline of "
                      f"{len(new_state)}/{len(programs)} programs saved")
        last_heartbeat = time.time()
    elif alerts == 0:
        now = time.time()
        if now - last_heartbeat >= HEARTBEAT_INTERVAL_SECS:
            send_telegram(f"✅ Intigriti scope check OK: no changes across "
                          f"{len(new_state)} programs ({fails} fetch problem(s))", silent=True)
            last_heartbeat = now

    save_state(new_state, last_heartbeat)
    print(f"Done: {len(programs)} programs, {alerts} alert(s), {fails} fetch problem(s)")
    if programs and fails == len(programs):
        sys.exit(1)


if __name__ == "__main__":
    main()
