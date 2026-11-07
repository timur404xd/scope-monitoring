#!/usr/bin/env python3
"""
bugcrowd_scope_monitor.py

Tracks scope of every public Bugcrowd engagement (bug_bounty + vdp) and sends an alert to
Discord and/or Telegram when something changes:

  NEW IN SCOPE            target added to an in-scope group
  MOVED INTO SCOPE        target was out of scope (or in no group), now in scope
  REMOVED FROM SCOPE      in-scope target disappeared
  NOW OUT OF SCOPE        in-scope target moved to an out-of-scope group
  NEW OUT-OF-SCOPE        informational
  DESCRIPTION CHANGED     target description or scope-group description text changed
  REWARD CHANGES          P1-P4 payout bands of a scope group changed
  GROUPS ADDED/REMOVED

Where alerts go (configure one or both):
  Discord   DISCORD_WEBHOOK_URL            webhook of the channel for scope changes; each
                                           change is an embed titled with the program name
                                           (linking to the brief), green when something
                                           entered scope, orange for other changes
            DISCORD_HEARTBEAT_WEBHOOK_URL  optional separate channel for the heartbeat and
                                           startup notice (defaults to the main webhook)
            DISCORD_USERNAME               optional display name (default "Bugcrowd Scope")
  Telegram  TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID

State: state/bugcrowd_scope_state.json (committed back by the workflow).

Safety rules:
  - first run = silent baseline (one summary message)
  - programs not seen before are baselined silently (set ANNOUNCE_NEW_PROGRAMS=1 to get
    their in-scope list instead)
  - fetch failure, or scope that suddenly comes back empty -> previous state is kept
  - a program missing from the listing (flaky unauthenticated pagination) keeps its
    previous state for MISSING_GRACE_SECS, so it is not "new" when it comes back
  - every program ever listed is remembered in "known", including ones whose scope
    fetch failed during the baseline, so a later successful fetch is not "new" either
  - a failed send (Discord or Telegram) -> that program's state is not advanced, so the
    alert is retried
  - exits non-zero if every program failed (so the Actions run goes red)
  - alerts go to Discord and Telegram (whichever are configured); a silent heartbeat
    goes to Discord only, at most once every HEARTBEAT_INTERVAL_SECS (default 6 hours)

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
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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
HEARTBEAT_TZ = ZoneInfo(os.environ.get("HEARTBEAT_TZ", "Africa/Cairo"))
HEARTBEAT_INTERVAL_SECS = int(os.environ.get("HEARTBEAT_INTERVAL_SECS", str(6 * 3600)))
MISSING_GRACE_SECS = int(os.environ.get("MISSING_GRACE_SECS", str(7 * 86400)))
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
DISCORD_HEARTBEAT_WEBHOOK_URL = (os.environ.get("DISCORD_HEARTBEAT_WEBHOOK_URL", "").strip()
                                 or DISCORD_WEBHOOK_URL)
DISCORD_USERNAME = os.environ.get("DISCORD_USERNAME", "Bugcrowd Scope")
DISCORD_EMBED_CHUNK = 4000     # embed description limit is 4096
DISCORD_CONTENT_CHUNK = 1900   # plain message limit is 2000
DISCORD_GREEN, DISCORD_ORANGE, DISCORD_BLUE = 0x2ECC71, 0xE67E22, 0x5865F2
DISCORD_FLAG_SUPPRESS_EMBEDS = 1 << 2
DISCORD_FLAG_SILENT = 1 << 12  # posts without a push/sound notification
_DISCORD_MD_RE = re.compile(r"([\\*_~`|>\[\]])")
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


def _md(s):
    """Escape Discord markdown, so wildcard targets like *.example.com don't turn italic."""
    # lines that are just a URL stay as-is: Discord auto-links them, and escapes would break the link
    return "\n".join(line if line.strip().startswith(("http://", "https://"))
                     else _DISCORD_MD_RE.sub(r"\\\1", line) for line in s.split("\n"))


def discord_change_body(ev):
    lines = []
    for key, title in SECTIONS:
        items = ev[key]
        if not items:
            continue
        lines.append(f"**{title} ({len(items)})**")
        lines.extend(_md(i) for i in items[:MAX_PER_SECTION])
        if len(items) > MAX_PER_SECTION:
            lines.append(f"…and {len(items) - MAX_PER_SECTION} more")
        lines.append("")
    return "\n".join(lines).strip()


def discord_new_program_body(snap):
    ins = [_t(t) for t in snap["targets"].values() if t["in"]]
    lines = [f"**IN SCOPE ({len(ins)})**"] + [_md(i) for i in ins[:MAX_PER_SECTION]]
    if len(ins) > MAX_PER_SECTION:
        lines.append(f"…and {len(ins) - MAX_PER_SECTION} more")
    return "\n".join(lines)


# --------------------------------------------------------------------------- notifications / state
def _chunks(text, limit=TG_CHUNK):
    cur = ""
    for line in text.split("\n"):
        if cur and len(cur) + len(line) + 1 > limit:
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


def _telegram_on():
    return bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))


def _discord_post(webhook, payload):
    """POST one message to a Discord webhook; waits out rate limits. True on success."""
    url = webhook + ("&" if "?" in webhook else "?") + "wait=true"
    data = json.dumps(payload).encode()
    for _ in range(4):
        req = urllib.request.Request(url, data=data, headers={
            "Content-Type": "application/json",
            # Discord's edge rejects the default Python-urllib user agent
            "User-Agent": "bugcrowd-scope-monitor (https://github.com, 1.0)"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return 200 <= r.status < 300
        except urllib.error.HTTPError as e:
            body = e.read()
            if e.code == 429:
                try:
                    wait = float(json.loads(body.decode()).get("retry_after", 2))
                except (ValueError, AttributeError):
                    wait = 2.0
                time.sleep(min(wait, 30) + 0.5)
                continue
            print(f"WARN: discord send failed: HTTP {e.code} {body[:300]!r}", file=sys.stderr)
            return False
        except Exception as e:  # noqa: BLE001
            print(f"WARN: discord send failed: {e}", file=sys.stderr)
            return False
    print("WARN: discord send failed: still rate-limited after retries", file=sys.stderr)
    return False


def send_discord_embed(title, link, body, color, footer):
    parts = list(_chunks(body, DISCORD_EMBED_CHUNK)) or [""]
    ok = True
    for n, desc in enumerate(parts, 1):
        embed = {"title": title[:256], "description": desc, "color": color,
                 "footer": {"text": footer + (f" · part {n}/{len(parts)}" if len(parts) > 1 else "")},
                 "timestamp": datetime.now(timezone.utc).isoformat()}
        if link.startswith(("http://", "https://")):
            embed["url"] = link
        ok = _discord_post(DISCORD_WEBHOOK_URL, {"username": DISCORD_USERNAME, "embeds": [embed],
                                                 "allowed_mentions": {"parse": []}}) and ok
        time.sleep(1)
    return ok


def send_change(name, link, ev):
    """Scope-change alert to every configured destination; True only if all succeeded."""
    ok = True
    if DISCORD_WEBHOOK_URL:
        entered = ev["new_in"] or ev["moved_in"]
        ok = send_discord_embed(f"{'🆕' if entered else '🔄'} {name}", link, discord_change_body(ev),
                                DISCORD_GREEN if entered else DISCORD_ORANGE,
                                "Bugcrowd scope change") and ok
    if _telegram_on():
        ok = send_telegram(format_message(name, link, ev)) and ok
    return ok


def send_new_program(name, link, snap):
    ok = True
    if DISCORD_WEBHOOK_URL:
        ok = send_discord_embed(f"🎯 {name}", link, discord_new_program_body(snap), DISCORD_BLUE,
                                "New Bugcrowd program") and ok
    if _telegram_on():
        ok = send_telegram(format_new_program(name, link, snap)) and ok
    return ok


def send_status(text, silent=False):
    """Startup notice: Discord (the heartbeat channel, if one is set) + Telegram."""
    ok = True
    if DISCORD_HEARTBEAT_WEBHOOK_URL:
        flags = DISCORD_FLAG_SUPPRESS_EMBEDS | (DISCORD_FLAG_SILENT if silent else 0)
        for chunk in _chunks(text, DISCORD_CONTENT_CHUNK):
            ok = _discord_post(DISCORD_HEARTBEAT_WEBHOOK_URL,
                               {"username": DISCORD_USERNAME, "content": chunk, "flags": flags,
                                "allowed_mentions": {"parse": []}}) and ok
            time.sleep(1)
    if _telegram_on():
        ok = send_telegram(text, silent=silent) and ok
    return ok


def heartbeat_due(last_heartbeat, now):
    """At most once every HEARTBEAT_INTERVAL_SECS."""
    return now - last_heartbeat >= HEARTBEAT_INTERVAL_SECS


def send_heartbeat(text):
    """Heartbeat: Discord only, silent. True when Discord accepted it."""
    if not DISCORD_HEARTBEAT_WEBHOOK_URL:
        print("INFO: no Discord webhook set, heartbeat skipped (heartbeats are Discord-only)")
        return False
    return _discord_post(DISCORD_HEARTBEAT_WEBHOOK_URL, {
        "username": DISCORD_USERNAME, "content": text[:1990],
        "flags": DISCORD_FLAG_SUPPRESS_EMBEDS | DISCORD_FLAG_SILENT,
        "allowed_mentions": {"parse": []}})


def load_state():
    try:
        with open(STATE_FILE) as f:
            st = json.load(f)
        if st.get("version") == SCHEMA and isinstance(st.get("programs"), dict):
            programs = st["programs"]
            # older state files have no "known" list; seed it from the saved programs
            known = set(st.get("known") or programs.keys())
            return programs, st.get("last_heartbeat", 0), known
    except (OSError, ValueError):
        pass
    return {}, 0, set()


def save_state(programs, last_heartbeat, known):
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    tmp = STATE_FILE + ".new"
    with open(tmp, "w") as f:
        json.dump({"version": SCHEMA, "programs": programs, "last_heartbeat": last_heartbeat,
                   "known": sorted(known)},
                  f, sort_keys=True, indent=1)
    os.replace(tmp, STATE_FILE)


def _strip_missing(snap):
    return {k: v for k, v in snap.items() if k != "missing_since"}


# --------------------------------------------------------------------------- main
def main():
    if not DISCORD_WEBHOOK_URL and not _telegram_on():
        sys.exit("Set DISCORD_WEBHOOK_URL and/or TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID")

    engagements = fetch_all_engagements()
    old, last_heartbeat, known = load_state()
    first_run = not old and not known
    now = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        snaps = list(ex.map(lambda e: fetch_scope(e["briefUrl"]), engagements))

    new_state, fails, alerts = {}, 0, 0
    for eng, snap in zip(engagements, snaps):
        key, name = eng["briefUrl"], eng["name"]
        link = BASE + key
        prev = old.get(key)
        is_known = key in known

        if snap is None or (prev and not snap["targets"] and prev["targets"]):
            print(f"WARN: no usable scope for {name} ({key}); keeping previous state", file=sys.stderr)
            fails += 1
            if prev:
                new_state[key] = _strip_missing(prev)
            if first_run:
                known.add(key)  # it existed at baseline; never announce it as new later
            continue

        snap["name"] = name
        if prev:
            ev = diff(prev, snap)
            if any(ev.values()):
                if send_change(name, link, ev):
                    alerts += 1
                else:
                    snap = _strip_missing(prev)  # retry next run
        elif not first_run and not is_known and ANNOUNCE_NEW_PROGRAMS:
            if not send_new_program(name, link, snap):
                continue  # leave unseen so it is announced again next run
            alerts += 1
        # known but no saved snapshot (fetch failed before) -> silent baseline, no alert
        new_state[key] = snap
        known.add(key)

    # programs that dropped out of the listing this run: keep them for a grace period,
    # so a flaky page does not make them look "new" when they reappear
    listed = {e["briefUrl"] for e in engagements}
    carried = 0
    for key, prev in old.items():
        if key in listed:
            continue
        since = prev.get("missing_since") or now
        if now - since < MISSING_GRACE_SECS:
            new_state[key] = {**prev, "missing_since": since}
            carried += 1
        else:
            known.discard(key)  # really gone; announce it if it ever relaunches
            print(f"INFO: dropping {prev.get('name', key)} (missing from listing too long)",
                  file=sys.stderr)

    if not new_state:
        sys.exit("ERROR: nothing collected (scope extraction probably broken); state not overwritten")

    if first_run:
        send_status(f"✅ Bugcrowd scope monitor started - baseline of "
                      f"{len(new_state)}/{len(engagements)} programs saved")
        last_heartbeat = now  # first heartbeat 6 hours after the baseline
    elif heartbeat_due(last_heartbeat, now):
        when = datetime.fromtimestamp(now, HEARTBEAT_TZ).strftime("%H:%M %Z")
        if send_heartbeat(f"✅ Bugcrowd scope check at {when}: monitor alive, "
                         f"{len(new_state)} programs tracked, {alerts} alert(s) this run, "
                         f"{fails} fetch problem(s), {carried} missing from listing"):
            last_heartbeat = now

    save_state(new_state, last_heartbeat, known)
    print(f"Done: {len(engagements)} programs, {alerts} alert(s), {fails} fetch problem(s), "
          f"{carried} carried over")
    if engagements and fails == len(engagements):
        sys.exit(1)


if __name__ == "__main__":
    main()
