#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  source_server_id TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  checksum TEXT
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slave_id TEXT NOT NULL,
  event_seq INTEGER NOT NULL,
  timestamp INTEGER,
  received_at INTEGER NOT NULL,
  kind TEXT,
  state TEXT,
  label TEXT,
  group_name TEXT,
  machine_type TEXT,
  confidence TEXT,
  raw_json TEXT NOT NULL,
  UNIQUE(slave_id, event_seq)
);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_slave_seq ON events(slave_id, event_seq);
"""


INDEX_HTML = r"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GTNH Machine Monitor</title>
  <link rel="stylesheet" href="/style.css">
</head>
<body>
  <header>
    <div>
      <h1>GTNH Machine Monitor</h1>
      <p id="freshness">Chargement...</p>
    </div>
    <div class="controls">
      <label>Période
        <select id="range">
          <option value="3600">1h</option>
          <option value="86400" selected>24h</option>
          <option value="604800">7j</option>
        </select>
      </label>
      <button id="refresh">Actualiser</button>
    </div>
  </header>

  <main>
    <section class="summary" id="summary"></section>
    <section class="panel">
      <div class="panel-title">
        <h2>Machines</h2>
        <input id="filter" type="search" placeholder="Filtrer label/groupe/type">
      </div>
      <table>
        <thead>
          <tr>
            <th data-sort="label">Machine</th>
            <th data-sort="group">Groupe</th>
            <th data-sort="state">État</th>
            <th data-sort="utilization">Utilisation</th>
            <th data-sort="activeTime">Actif</th>
            <th data-sort="blockedTime">Bloqué</th>
            <th data-sort="cycles">Cycles</th>
            <th data-sort="avgCycle">Cycle moyen</th>
            <th data-sort="lastSeen">Vu</th>
            <th data-sort="confidence">Confiance</th>
          </tr>
        </thead>
        <tbody id="machines"></tbody>
      </table>
    </section>
  </main>

  <script src="/app.js"></script>
</body>
</html>
"""


STYLE_CSS = r"""
:root {
  color-scheme: light dark;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #101418;
  color: #edf2f7;
}

body {
  margin: 0;
  min-width: 320px;
}

header {
  display: flex;
  justify-content: space-between;
  gap: 24px;
  align-items: center;
  padding: 22px 28px;
  border-bottom: 1px solid #26313a;
  background: #151b21;
}

h1, h2, p { margin: 0; }
h1 { font-size: 22px; font-weight: 700; }
h2 { font-size: 17px; }
p { color: #9fb0bf; margin-top: 4px; }

main {
  padding: 24px 28px;
}

.controls {
  display: flex;
  align-items: center;
  gap: 12px;
}

select, input, button {
  background: #202a33;
  border: 1px solid #344451;
  color: #edf2f7;
  border-radius: 6px;
  padding: 8px 10px;
}

button { cursor: pointer; }

.summary {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
  margin-bottom: 20px;
}

.metric {
  border: 1px solid #27333d;
  border-radius: 8px;
  padding: 14px;
  background: #151b21;
}

.metric strong {
  display: block;
  font-size: 24px;
  margin-bottom: 4px;
}

.metric span { color: #9fb0bf; }

.panel {
  border: 1px solid #27333d;
  border-radius: 8px;
  background: #151b21;
  overflow: hidden;
}

.panel-title {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  padding: 16px;
  border-bottom: 1px solid #27333d;
}

table {
  border-collapse: collapse;
  width: 100%;
}

th, td {
  padding: 10px 12px;
  border-bottom: 1px solid #27333d;
  text-align: left;
  font-size: 14px;
}

th {
  color: #9fb0bf;
  font-weight: 600;
  cursor: pointer;
  user-select: none;
}

tbody tr:hover { background: #1b232b; }

.badge {
  display: inline-block;
  min-width: 66px;
  border-radius: 999px;
  padding: 3px 8px;
  text-align: center;
  font-size: 12px;
  font-weight: 700;
}

.active { background: #14532d; color: #bbf7d0; }
.idle { background: #334155; color: #e2e8f0; }
.blocked { background: #7f1d1d; color: #fecaca; }
.unknown { background: #3f3f46; color: #e4e4e7; }
.offline { background: #4a2d12; color: #fed7aa; }

@media (max-width: 900px) {
  header, .panel-title { align-items: flex-start; flex-direction: column; }
  table { min-width: 860px; }
  .panel { overflow-x: auto; }
}
"""


APP_JS = r"""
const state = { events: [], status: {}, sortKey: 'utilization', sortDir: -1 };

const fmtDuration = seconds => {
  if (!Number.isFinite(seconds) || seconds < 0) return '-';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
};

const fmtAge = timestamp => {
  if (!timestamp) return '-';
  return fmtDuration(Date.now() / 1000 - timestamp);
};

async function loadData() {
  const rangeSeconds = Number(document.getElementById('range').value);
  const since = Math.floor(Date.now() / 1000) - rangeSeconds;
  const [eventsRes, statusRes] = await Promise.all([
    fetch(`/api/events?since=${since}&limit=100000`),
    fetch('/api/status'),
  ]);
  state.events = await eventsRes.json();
  state.status = await statusRes.json();
  render();
}

function computeMetrics(events, rangeSeconds) {
  const now = Date.now() / 1000;
  const windowStart = now - rangeSeconds;
  const bySlave = new Map();

  for (const ev of events) {
    const sid = ev.slave_id || 'unknown';
    if (!bySlave.has(sid)) bySlave.set(sid, []);
    bySlave.get(sid).push(ev);
  }

  const rows = [];
  for (const [slaveId, list] of bySlave.entries()) {
    list.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0) || (a.event_seq || 0) - (b.event_seq || 0));
    const latest = list[list.length - 1] || {};
    const durations = { active: 0, idle: 0, blocked: 0, unknown: 0 };
    let cycles = 0;
    let activeStart = null;
    const cycleDurations = [];

    for (let i = 0; i < list.length; i++) {
      const cur = list[i];
      const next = list[i + 1];
      const start = Math.max(cur.timestamp || windowStart, windowStart);
      const end = Math.min((next && next.timestamp) || now, now);
      const duration = Math.max(0, end - start);
      const curState = cur.state || 'unknown';
      durations[curState] = (durations[curState] || 0) + duration;

      if (curState === 'active' && activeStart === null) activeStart = start;
      if (curState !== 'active' && activeStart !== null) {
        cycles += 1;
        cycleDurations.push(Math.max(0, start - activeStart));
        activeStart = null;
      }
    }

    const lastSeen = latest.timestamp || 0;
    const offline = now - lastSeen > Math.max(180, Number(state.status.expected_upload_interval_s || 60) * 3);
    const activeTime = durations.active || 0;
    const blockedTime = durations.blocked || 0;
    const utilization = rangeSeconds > 0 ? activeTime / rangeSeconds : 0;
    const avgCycle = cycleDurations.length ? cycleDurations.reduce((a, b) => a + b, 0) / cycleDurations.length : 0;

    rows.push({
      slaveId,
      label: latest.label || slaveId,
      group: latest.group || latest.group_name || 'default',
      machineType: latest.machine_type || 'unknown',
      state: offline ? 'offline' : (latest.state || 'unknown'),
      utilization,
      activeTime,
      blockedTime,
      cycles,
      avgCycle,
      lastSeen,
      confidence: latest.confidence || 'low',
    });
  }

  return rows;
}

function render() {
  const rangeSeconds = Number(document.getElementById('range').value);
  const rows = computeMetrics(state.events, rangeSeconds);
  const query = document.getElementById('filter').value.toLowerCase();
  const filtered = rows.filter(row => `${row.label} ${row.group} ${row.machineType}`.toLowerCase().includes(query));
  filtered.sort((a, b) => {
    const av = a[state.sortKey];
    const bv = b[state.sortKey];
    if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * state.sortDir;
    return String(av).localeCompare(String(bv)) * state.sortDir;
  });

  const online = rows.filter(row => row.state !== 'offline').length;
  const blocked = rows.filter(row => row.state === 'blocked').length;
  const active = rows.filter(row => row.state === 'active').length;
  const totalActive = rows.reduce((sum, row) => sum + row.activeTime, 0);
  document.getElementById('summary').innerHTML = `
    <div class="metric"><strong>${rows.length}</strong><span>machines connues</span></div>
    <div class="metric"><strong>${online}</strong><span>online</span></div>
    <div class="metric"><strong>${active}</strong><span>actives maintenant</span></div>
    <div class="metric"><strong>${blocked}</strong><span>bloquées</span></div>
    <div class="metric"><strong>${fmtDuration(totalActive)}</strong><span>temps actif cumulé</span></div>
  `;

  const lastIngest = state.status.last_ingest_at || 0;
  document.getElementById('freshness').textContent = lastIngest
    ? `Dernière sync il y a ${fmtAge(lastIngest)} · ${state.status.event_count || 0} événements bruts`
    : 'Aucune donnée reçue';

  document.getElementById('machines').innerHTML = filtered.map(row => `
    <tr>
      <td><strong>${escapeHtml(row.label)}</strong><br><small>${escapeHtml(row.machineType)}</small></td>
      <td>${escapeHtml(row.group)}</td>
      <td><span class="badge ${row.state}">${row.state}</span></td>
      <td>${Math.round(row.utilization * 100)}%</td>
      <td>${fmtDuration(row.activeTime)}</td>
      <td>${fmtDuration(row.blockedTime)}</td>
      <td>${row.cycles}</td>
      <td>${row.avgCycle ? fmtDuration(row.avgCycle) : '-'}</td>
      <td>${fmtAge(row.lastSeen)}</td>
      <td>${escapeHtml(row.confidence)}</td>
    </tr>
  `).join('');
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[char]));
}

document.getElementById('refresh').addEventListener('click', loadData);
document.getElementById('range').addEventListener('change', loadData);
document.getElementById('filter').addEventListener('input', render);
document.querySelectorAll('th[data-sort]').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.dataset.sort;
    if (state.sortKey === key) state.sortDir *= -1;
    else { state.sortKey = key; state.sortDir = -1; }
    render();
  });
});

loadData();
setInterval(loadData, 30000);
"""


class EventStore:
    def __init__(self, db_path: Path, data_dir: Path | None = None) -> None:
        self.db_path = db_path
        self.data_dir = data_dir
        self.lock = threading.Lock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.data_dir:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def ingest_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        batch_id = str(batch.get("batch_id") or "")
        source_server_id = str(batch.get("source_server_id") or "")
        events = batch.get("events")
        if not batch_id or not source_server_id or not isinstance(events, list):
            raise ValueError("batch_id, source_server_id and events are required")

        received_at = int(time.time())
        accepted_events = 0
        with self.lock, self.connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO batches(batch_id, source_server_id, received_at, checksum) VALUES (?, ?, ?, ?)",
                (batch_id, source_server_id, received_at, batch.get("checksum")),
            )
            for event in events:
                if not isinstance(event, dict):
                    continue
                slave_id = str(event.get("slave_id") or "")
                event_seq = event.get("event_seq")
                if not slave_id or not isinstance(event_seq, int):
                    continue
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO events(
                      slave_id, event_seq, timestamp, received_at, kind, state, label, group_name,
                      machine_type, confidence, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        slave_id,
                        event_seq,
                        event.get("timestamp"),
                        received_at,
                        event.get("kind"),
                        event.get("state"),
                        event.get("label"),
                        event.get("group") or event.get("group_name"),
                        event.get("machine_type"),
                        event.get("confidence"),
                        json.dumps(event, separators=(",", ":"), ensure_ascii=False),
                    ),
                )
                accepted_events += cur.rowcount
            conn.commit()

        if self.data_dir and accepted_events:
            self._append_data_file(batch, received_at)

        return {
            "accepted": True,
            "batch_id": batch_id,
            "acked_event_count": accepted_events,
            "server_time": received_at,
        }

    def _append_data_file(self, batch: dict[str, Any], received_at: int) -> None:
        day = time.strftime("%Y-%m-%d", time.gmtime(received_at))
        path = self.data_dir / f"events-{day}.ndjson"
        with path.open("a", encoding="utf-8") as handle:
            for event in batch.get("events", []):
                record = {
                    "received_at": received_at,
                    "source_server_id": batch.get("source_server_id"),
                    "batch_id": batch.get("batch_id"),
                    "event": event,
                }
                handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False))
                handle.write("\n")

    def list_events(self, since: int | None, limit: int) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 200000))
        params: list[Any] = []
        where = ""
        if since is not None:
            where = "WHERE COALESCE(timestamp, received_at) >= ?"
            params.append(since)
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT raw_json FROM events {where} ORDER BY COALESCE(timestamp, received_at), slave_id, event_seq LIMIT ?",
                params,
            ).fetchall()
        return [json.loads(row["raw_json"]) for row in rows]

    def status(self, expected_upload_interval_s: int) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS event_count, MAX(received_at) AS last_ingest_at FROM events"
            ).fetchone()
            batch_row = conn.execute("SELECT COUNT(*) AS batch_count FROM batches").fetchone()
        return {
            "event_count": row["event_count"] or 0,
            "batch_count": batch_row["batch_count"] or 0,
            "last_ingest_at": row["last_ingest_at"],
            "expected_upload_interval_s": expected_upload_interval_s,
        }


class MonitorHandler(BaseHTTPRequestHandler):
    server_version = "GTNHMachineMonitor/0.1"

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, text: str, content_type: str) -> None:
        data = text.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_text(INDEX_HTML, "text/html; charset=utf-8")
        elif parsed.path == "/style.css":
            self._send_text(STYLE_CSS, "text/css; charset=utf-8")
        elif parsed.path == "/app.js":
            self._send_text(APP_JS, "application/javascript; charset=utf-8")
        elif parsed.path == "/health":
            self._send_json({"ok": True, "time": int(time.time())})
        elif parsed.path == "/api/status":
            self._send_json(self.server.store.status(self.server.expected_upload_interval_s))  # type: ignore[attr-defined]
        elif parsed.path == "/api/events":
            qs = parse_qs(parsed.query)
            since = int(qs["since"][0]) if qs.get("since") else None
            limit = int(qs["limit"][0]) if qs.get("limit") else 50000
            self._send_json(self.server.store.list_events(since, limit))  # type: ignore[attr-defined]
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/ingest":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            result = self.server.store.ingest_batch(payload)  # type: ignore[attr-defined]
        except Exception as exc:  # keep endpoint simple for OC clients
            self._send_json({"accepted": False, "error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        self._send_json(result)

    def log_message(self, fmt: str, *args: Any) -> None:
        print("%s - %s" % (self.address_string(), fmt % args))


class MonitorServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        store: EventStore,
        expected_upload_interval_s: int,
    ) -> None:
        super().__init__(server_address, MonitorHandler)
        self.store = store
        self.expected_upload_interval_s = expected_upload_interval_s


def main() -> int:
    parser = argparse.ArgumentParser(description="GTNH OpenComputers machine monitor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", type=Path, default=Path("data/monitor.sqlite3"))
    parser.add_argument("--data-dir", type=Path, default=None, help="optional NDJSON export directory, e.g. inside a private repo")
    parser.add_argument("--expected-upload-interval", type=int, default=60)
    args = parser.parse_args()

    store = EventStore(args.db, args.data_dir)
    server = MonitorServer((args.host, args.port), store, args.expected_upload_interval)
    print(f"Serving dashboard on http://{args.host}:{args.port}")
    print("Ingest endpoint: /ingest")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
