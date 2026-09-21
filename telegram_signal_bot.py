#!/usr/bin/env python3
"""
Otherside Capital -- Journal DB -> Telegram signal bot (final, built against
your real Supabase schema: public.options_trades)
=============================================================================

What this does
---------------
Polls options_trades and posts three kinds of updates to your free channel:

  1. OPEN         -- a brand new trade you've just logged.
  2. STOP UPDATED -- your stop-loss or trailing stop on an open trade moved
     to a new stock price.
  3. CLOSED       -- that trade's outcome, including P&L, win or loss.

The goal is full transparency: every open call the channel sees gets its
stop adjustments and its final outcome posted too, not just the win.

It also refreshes digest.json every run -- a rolling 7-day recap of closed
trades (win rate, total P&L, per-trade notes) with no P&L/thesis analysis
attached. Since this repo is public, that file is readable over plain
HTTPS with no DB credentials needed, which is what the weekly X-content
task reads from to draft a recap thread and graphic.

Why this needs more than "watch for new rows"
------------------------------------------------
Checked this against your actual trading-journal app's source (app/page.js).
Findings:
  - Every options trade is INSERTed with status = "open" the moment you log
    it. There's no "planned" status anywhere in the app, so watching for
    brand-new rows via created_at reliably catches every open trade exactly
    once.
  - Moving a stop (via your trailing-stop tool, which writes to the
    trailing_stop_price column) and closing a trade (status flips
    "open" -> "closed", exit fields get filled in) are BOTH done as an
    in-place UPDATE on the same row -- no new row is created either time.
    Your options_trades table has no updated_at column, so a plain "new
    rows since last check" poll can't see either of these on its own (this
    is exactly the bug you hit where a closed test trade never posted).

The fix: this bot keeps a small record in state.json of every trade it's
currently "watching" (posted as OPEN, not yet posted as CLOSED) --
tracked_open_ids -- plus the last known stop values for each one
(stop_watch). Every poll:
  1. Looks for brand-new rows (created_at > watermark) -> posts each as
     OPEN, starts watching it, and records its starting stop values.
  2. Re-fetches just the ids being watched (normally 0-5 trades, never the
     whole table) and compares:
       - status == closed?              -> post CLOSED (with P&L), stop
                                            watching it.
       - sl_stock_price or
         trailing_stop_price changed?    -> post STOP UPDATED, update the
                                            recorded value so it isn't
                                            re-announced next poll.
That re-check is the only reliable way to notice an in-place edit on a
table that doesn't record when a row was last modified.

One deliberate limitation: trades that were already open in your journal
*before* you first start this bot are never added to the watch list (the
first run only records a starting watermark, it doesn't back-post
history). So older open positions won't get stop or close updates posted
either -- consistent, since the channel was never told they existed as an
open trade in the first place. Every trade logged after the bot starts
gets the full open -> stop adjustments -> close story.

Requirements
------------
    pip install psycopg2-binary requests

Getting a bot token and chat ID
--------------------------------
1. In Telegram, message @BotFather -> /newbot -> follow the prompts ->
   it gives you a token that looks like 123456789:AAExxxxxxxxxxxxxxxxxxxx
2. Add that bot as an admin of your channel (Channel -> Administrators ->
   Add Admin -> search for your bot's @username, give it "Post Messages").
3. To find your channel's chat ID: post any message in the channel, then
   visit https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates in a browser
   -- the channel's id shows up as a negative number like -1001234567890.

Running it continuously
------------------------
This script polls in an infinite loop. For a simple always-on setup:
  - Cheapest: a $5-6/month VPS (DigitalOcean, Linode, Hetzner) running
    this under `systemd` or inside a `screen`/`tmux` session.
  - Free option: a scheduled GitHub Actions workflow that runs the
    "check once and exit" mode (RUN_ONCE = True below) every 10-15
    minutes instead of looping forever.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

# Supabase direct connection string. Get this from your project's "Connect"
# button -> Direct connection tab. Read from an env var so the password
# never ends up hardcoded or committed anywhere.
#   export JOURNAL_DB_URL="postgresql://postgres:YOUR-PASSWORD@db.xxxx.supabase.co:5432/postgres"
DB_CONNECTION_STRING = os.environ.get("JOURNAL_DB_URL", "")

TABLE_NAME = "options_trades"
ID_COLUMN = "id"

# The two columns that represent "where's the stop right now." sl_stock_price
# is the stop you set at entry (editable later too); trailing_stop_price is
# what your trailing-stop tool writes when you move it as the trade runs.
STOP_COLUMNS = {
    "sl_stock_price": "Stop loss",
    "trailing_stop_price": "Trailing stop",
}

# Telegram bot credentials -- set as environment variables, don't hardcode.
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
FREE_CHANNEL_CHAT_ID = os.environ.get("TELEGRAM_FREE_CHAT_ID", "")

# How often to check for new rows, in seconds. Only used when RUN_ONCE=False.
POLL_INTERVAL_SECONDS = 90

# Set True to run as a one-shot check (e.g. from a GitHub Actions cron job)
# instead of an always-on loop.
RUN_ONCE = True

STATE_FILE = Path(__file__).parent / "state.json"

# Rolling 7-day recap of closed trades, refreshed every run. This is what
# lets other tools (e.g. a weekly X-content task) get last week's trade
# outcomes without ever needing your DB password -- since this repo is
# public, they can just read this file over plain HTTPS.
DIGEST_FILE = Path(__file__).parent / "digest.json"
DIGEST_WINDOW_DAYS = 7


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text())
    else:
        state = {}
    # setdefault so older state.json files (from before stop/close tracking
    # was added) still load fine.
    state.setdefault("last_created_at", None)
    state.setdefault("tracked_open_ids", [])
    state.setdefault("stop_watch", {})
    return state


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, default=str))


def _norm_num(value):
    """Coerce a DB numeric (often a Decimal) to a plain float, or None.
    Keeping these as plain floats in state.json (rather than Decimal/str)
    means the next run's fresh DB value compares cleanly against the saved
    one -- no spurious 'stop changed' firing just from a type mismatch."""
    if value is None:
        return None
    return float(value)


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def fetch_newly_opened(cur, last_created_at):
    """Return brand-new options_trades rows since last_created_at, oldest
    first, plus the new watermark to save. On the very first run (no
    watermark yet), returns nothing but records the current max created_at
    so we don't dump the whole trade history into the channel."""
    if last_created_at is None:
        cur.execute(f"SELECT MAX(created_at) AS max_created_at FROM {TABLE_NAME}")
        row = cur.fetchone()
        return [], (row["max_created_at"] if row else None)

    cur.execute(
        f"""
        SELECT * FROM {TABLE_NAME}
        WHERE created_at > %s
        ORDER BY created_at ASC
        """,
        (last_created_at,),
    )
    rows = cur.fetchall()
    new_watermark = rows[-1]["created_at"] if rows else last_created_at
    return rows, new_watermark


def fetch_tracked_rows(cur, tracked_open_ids):
    """Re-fetch the current state of every trade we're watching (posted as
    OPEN, not yet posted as CLOSED) so we can notice stop moves and closes.
    Cast id to text so the comparison works regardless of whether psycopg2
    handed us uuid.UUID objects or plain strings when we first tracked
    them."""
    if not tracked_open_ids:
        return []
    cur.execute(
        f"""
        SELECT * FROM {TABLE_NAME}
        WHERE {ID_COLUMN}::text = ANY(%s)
        """,
        (list(tracked_open_ids),),
    )
    return cur.fetchall()


def fetch_weekly_digest(cur):
    """Pull every trade closed in the last DIGEST_WINDOW_DAYS days, for the
    weekly X-content task to build a recap thread/graphic from. Read-only,
    doesn't affect the OPEN/STOP/CLOSE posting logic above at all."""
    cur.execute(
        f"""
        SELECT * FROM {TABLE_NAME}
        WHERE lower(status) = 'closed'
          AND exit_date >= (now() - interval '{DIGEST_WINDOW_DAYS} days')
        ORDER BY exit_date DESC
        """
    )
    return cur.fetchall()


def build_digest_payload(rows) -> dict:
    pnls = [float(r["pnl"]) for r in rows if r.get("pnl") is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_days": DIGEST_WINDOW_DAYS,
        "trade_count": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(pnls) * 100, 1) if pnls else None,
        "total_pnl": round(sum(pnls), 2) if pnls else None,
        "trades": [
            {
                "ticker": r.get("ticker"),
                "position_side": r.get("position_side"),
                "option_type": r.get("option_type"),
                "strike_price": _norm_num(r.get("strike_price")),
                "expiry_date": str(r["expiry_date"]) if r.get("expiry_date") else None,
                "entry_date": str(r["entry_date"]) if r.get("entry_date") else None,
                "exit_date": str(r["exit_date"]) if r.get("exit_date") else None,
                "entry_stock_price": _norm_num(r.get("entry_stock_price")),
                "premium": _norm_num(r.get("premium")),
                "contracts": _norm_num(r.get("contracts")),
                "pnl": _norm_num(r.get("pnl")),
                "notes": r.get("notes"),
                "journal_notes": r.get("journal_notes"),
            }
            for r in rows
        ],
    }


def save_digest(payload: dict) -> None:
    DIGEST_FILE.write_text(json.dumps(payload, indent=2, default=str))


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def _fmt(value, suffix=""):
    if value is None:
        return "n/a"
    return f"{value}{suffix}"


def format_open_message(row) -> str:
    direction = f"{(row.get('position_side') or '').title()} {(row.get('option_type') or '').title()}".strip()
    return (
        f"✅ OPEN — live position\n"
        f"\U0001F4CA {row.get('ticker', 'n/a')} — {direction}\n\n"
        f"Strike {_fmt(row.get('strike_price'))} | Exp {_fmt(row.get('expiry_date'))}\n"
        f"Contracts: {_fmt(row.get('contracts'))}  |  Premium: {_fmt(row.get('premium'))}\n"
        f"Underlying @ entry: {_fmt(row.get('entry_stock_price'))}\n\n"
        f"Stop (stock): {_fmt(row.get('sl_stock_price'))}  |  Target (stock): {_fmt(row.get('tp_stock_price'))}\n"
        f"Δ {_fmt(row.get('delta'))}  Γ {_fmt(row.get('gamma'))}  "
        f"Θ {_fmt(row.get('theta'))}  V {_fmt(row.get('vega'))}\n\n"
        f"{row.get('notes') or ''}\n\n"
        f"⚠️ Educational commentary, not personalized investment advice. "
        f"Full breakdown on YouTube weekly."
    )


def format_stop_update_message(row, field: str, old_value, new_value) -> str:
    label = STOP_COLUMNS.get(field, "Stop")
    old_str = f"${old_value:,.2f}" if old_value is not None else "not set"
    new_str = f"${new_value:,.2f}"
    return (
        f"\U0001F53A STOP UPDATED\n"
        f"\U0001F4CA {row.get('ticker', 'n/a')}\n\n"
        f"{label}: {old_str} → {new_str}\n\n"
        f"⚠️ Educational commentary, not personalized investment advice. "
        f"Full breakdown on YouTube weekly."
    )


def format_close_message(row) -> str:
    direction = f"{(row.get('position_side') or '').title()} {(row.get('option_type') or '').title()}".strip()
    pnl = row.get("pnl")

    if pnl is None:
        badge = "\U0001F512 CLOSED"
        pnl_line = "P&L: n/a"
    else:
        pnl_val = float(pnl)
        if pnl_val > 0:
            badge = "\U0001F7E2 CLOSED — WIN"
        elif pnl_val < 0:
            badge = "\U0001F534 CLOSED — LOSS"
        else:
            badge = "⚪ CLOSED — BREAKEVEN"
        sign = "+" if pnl_val > 0 else ""
        pnl_line = f"P&L: {sign}${pnl_val:,.2f}"

    notes = row.get("journal_notes") or row.get("notes") or ""

    return (
        f"{badge}\n"
        f"\U0001F4CA {row.get('ticker', 'n/a')} — {direction}\n\n"
        f"Strike {_fmt(row.get('strike_price'))} | Exp {_fmt(row.get('expiry_date'))}\n"
        f"Entry: {_fmt(row.get('entry_date'))}  |  Exit: {_fmt(row.get('exit_date'))}\n\n"
        f"{pnl_line}\n\n"
        f"{notes}\n\n"
        f"⚠️ Educational commentary, not personalized investment advice. "
        f"Full breakdown on YouTube weekly."
    )


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def post_to_telegram(text: str) -> None:
    if not BOT_TOKEN or not FREE_CHANNEL_CHAT_ID:
        raise RuntimeError(
            "Set TELEGRAM_BOT_TOKEN and TELEGRAM_FREE_CHAT_ID environment variables before running."
        )
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(url, data={"chat_id": FREE_CHANNEL_CHAT_ID, "text": text}, timeout=15)
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Main poll cycle
# ---------------------------------------------------------------------------

def run_once(state: dict) -> dict:
    import psycopg2
    import psycopg2.extras

    last_created_at = state.get("last_created_at")
    tracked_open_ids = list(state.get("tracked_open_ids", []))
    stop_watch = dict(state.get("stop_watch", {}))

    conn = psycopg2.connect(DB_CONNECTION_STRING)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Brand-new trades -> post OPEN, start watching them.
            new_rows, new_watermark = fetch_newly_opened(cur, last_created_at)
            for row in new_rows:
                status = (row.get("status") or "").strip().lower()
                if status != "open":
                    # Not currently possible per your app (every insert is
                    # status='open') -- a safety net, not expected behaviour.
                    continue
                post_to_telegram(format_open_message(row))
                print(f"[{datetime.now(timezone.utc).isoformat()}] posted OPEN id={row.get('id')} ({row.get('ticker')})")
                rid = str(row[ID_COLUMN])
                tracked_open_ids.append(rid)
                stop_watch[rid] = {col: _norm_num(row.get(col)) for col in STOP_COLUMNS}

            # 2. Re-check every trade being watched for stop moves / closes.
            tracked_rows = fetch_tracked_rows(cur, tracked_open_ids)
            still_open_ids = set(tracked_open_ids)

            for row in tracked_rows:
                rid = str(row[ID_COLUMN])
                status = (row.get("status") or "").strip().lower()

                if status == "closed":
                    post_to_telegram(format_close_message(row))
                    print(f"[{datetime.now(timezone.utc).isoformat()}] posted CLOSE id={row.get('id')} ({row.get('ticker')}) pnl={row.get('pnl')}")
                    still_open_ids.discard(rid)
                    stop_watch.pop(rid, None)
                    continue

                # Still open -- compare stop columns against what we last saw.
                prev_stops = stop_watch.get(rid, {})
                new_stops = {}
                for col in STOP_COLUMNS:
                    old_val = prev_stops.get(col)
                    new_val = _norm_num(row.get(col))
                    if new_val is not None and new_val != old_val:
                        post_to_telegram(format_stop_update_message(row, col, old_val, new_val))
                        print(f"[{datetime.now(timezone.utc).isoformat()}] posted STOP UPDATE id={row.get('id')} ({row.get('ticker')}) {col}: {old_val} -> {new_val}")
                    new_stops[col] = new_val
                stop_watch[rid] = new_stops

            tracked_open_ids = [i for i in tracked_open_ids if i in still_open_ids]

            # 3. Refresh the public weekly digest (read-only, no posting).
            digest_rows = fetch_weekly_digest(cur)
            save_digest(build_digest_payload(digest_rows))
    finally:
        conn.close()

    if (
        new_watermark != last_created_at
        or tracked_open_ids != state.get("tracked_open_ids", [])
        or stop_watch != state.get("stop_watch", {})
    ):
        state["last_created_at"] = new_watermark
        state["tracked_open_ids"] = tracked_open_ids
        state["stop_watch"] = stop_watch
        save_state(state)

    return state


def main():
    if not DB_CONNECTION_STRING:
        raise RuntimeError("Set the JOURNAL_DB_URL environment variable to your Supabase connection string.")
    state = load_state()
    if RUN_ONCE:
        run_once(state)
        return
    print("Starting signal bot loop. Ctrl+C to stop.")
    while True:
        try:
            state = run_once(state)
        except Exception as exc:  # noqa: BLE001 -- keep the loop alive on transient errors
            print(f"[error] {exc}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
