from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pc.monitor import EventStore


class EventStoreTest(unittest.TestCase):
    def test_ingest_deduplicates_events_by_slave_and_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = EventStore(Path(tmp) / "monitor.sqlite3")
            batch = {
                "source_server_id": "central-a",
                "batch_id": "batch-1",
                "events": [
                    {
                        "slave_id": "slave-a",
                        "event_seq": 1,
                        "timestamp": 100,
                        "kind": "transition",
                        "state": "active",
                        "label": "EBF 1",
                        "group": "ebf",
                        "machine_type": "gt_machine",
                        "confidence": "high",
                    }
                ],
            }

            first = store.ingest_batch(batch)
            second = store.ingest_batch(batch)

            self.assertTrue(first["accepted"])
            self.assertEqual(first["acked_event_count"], 1)
            self.assertEqual(second["acked_event_count"], 0)
            self.assertEqual(len(store.list_events(None, 100)), 1)

    def test_list_events_filters_by_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = EventStore(Path(tmp) / "monitor.sqlite3")
            store.ingest_batch(
                {
                    "source_server_id": "central-a",
                    "batch_id": "batch-1",
                    "events": [
                        {"slave_id": "slave-a", "event_seq": 1, "timestamp": 100, "state": "idle"},
                        {"slave_id": "slave-a", "event_seq": 2, "timestamp": 200, "state": "active"},
                    ],
                }
            )

            events = store.list_events(since=150, limit=100)

            self.assertEqual([event["event_seq"] for event in events], [2])

    def test_optional_ndjson_export_writes_raw_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "export"
            store = EventStore(Path(tmp) / "monitor.sqlite3", data_dir)
            store.ingest_batch(
                {
                    "source_server_id": "central-a",
                    "batch_id": "batch-1",
                    "events": [{"slave_id": "slave-a", "event_seq": 1, "timestamp": 100}],
                }
            )

            exported = list(data_dir.glob("events-*.ndjson"))

            self.assertEqual(len(exported), 1)
            self.assertIn('"batch_id":"batch-1"', exported[0].read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
