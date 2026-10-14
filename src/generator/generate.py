"""Plant telemetry generator with two sinks: Lakeflow Connect Zerobus (live gateways) and JSON files (historian export).

Routing with --sink both (the default):
  * Plant North and Plant South gateways push records through Zerobus Ingest into a managed Delta table.
  * Plant East has a legacy historian that exports JSON-lines files to a UC volume every --files-interval-s seconds.
--sink zerobus or --sink files sends every plant to that one sink.

--bad-record-rate injects known defects so data quality can be tested end to end. Every injected defect is counted
in a manifest (JSON in the volume's _manifests folder) that the DQ gate reconciles against the quarantine table.
All data is synthetic.
"""
import argparse
import json
import logging
import os
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

# Serverless Python tasks have no __file__; the bundle passes --src-path explicitly.
_src = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--src-path=")), None)
sys.path.append(_src or os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from telemetry.config import SENSORS  # noqa: E402
from telemetry.physics import Fleet, build_station_master  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("generator")
logging.getLogger("zerobus").setLevel(logging.WARNING)

FIRMWARE = {"zerobus": "gw-fw-3.2.1", "files": "hist-exp-1.4"}
FILE_PLANTS = {"PLT-E"}  # legacy historian export
# Each defect must trip one rule in src/pipeline/telemetry_pipeline.sql (view telemetry_checked):
#   hard (dropped from silver, kept in quarantine): null_station -> station_id_present, out_of_range ->
#   vibration_rms_in_range, type_mismatch / extra_field -> no_rescued_data (Auto Loader rescued data),
#   truncated_line -> station_id_present (unparseable line). Soft (flagged in silver): future_ts -> not_from_future,
#   unknown_firmware -> known_firmware. duplicate -> removed by the silver de-duplication.
HARD_DEFECTS = ("null_station", "out_of_range", "type_mismatch", "extra_field", "truncated_line")
SOFT_DEFECTS = ("future_ts", "unknown_firmware")
SINK_DEFECTS = {
    "zerobus": ["null_station", "out_of_range", "future_ts", "unknown_firmware", "duplicate"],
    "files": ["null_station", "out_of_range", "type_mismatch", "extra_field", "truncated_line",
              "future_ts", "unknown_firmware", "duplicate"],
}


def route(plant_id: str, sink: str) -> str:
    if sink != "both":
        return sink
    return "files" if plant_id in FILE_PLANTS else "zerobus"


def make_record(s: dict, readings: dict, i: int, t: float, sink: str, run_tag: str) -> dict:
    rec = {"event_id": f"{s['station_id']}-{int(t)}", "gateway_id": s["gateway_id"], "plant_id": s["plant_id"],
           "line_id": s["line_id"], "station_id": s["station_id"], "seq": int(t), "firmware": FIRMWARE[sink],
           "run_tag": run_tag}
    rec["ts"] = (int(t * 1_000_000) if sink == "zerobus"
                 else datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="milliseconds"))
    rec.update({k: float(readings[k][i]) for k in SENSORS})
    return rec


def inject(rec: dict, kind: str, sink: str) -> dict | str:
    """Apply one defect. Returns the record, or a raw string for defects that break the file format."""
    rec = dict(rec)
    if kind == "null_station":
        rec["station_id"] = None
    elif kind == "out_of_range":
        rec["vibration_rms"] = 999.0  # sensor saturation / unit error
    elif kind == "future_ts":  # gateway clock 30 min ahead
        rec["ts"] = (rec["ts"] + 30 * 60 * 1_000_000 if sink == "zerobus"
                     else datetime.fromisoformat(rec["ts"]).timestamp() + 1800)
        if sink == "files":
            rec["ts"] = datetime.fromtimestamp(rec["ts"], timezone.utc).isoformat(timespec="milliseconds")
    elif kind == "unknown_firmware":
        rec["firmware"] = "gw-fw-0.9.0-beta"
    elif kind == "type_mismatch":
        rec["bearing_temp_c"] = f"{rec['bearing_temp_c']}C"  # unit suffix from a misconfigured exporter
    elif kind == "extra_field":
        rec["humidity_pct"] = 41.5  # schema drift: a new sensor appears without notice
    elif kind == "truncated_line":
        return json.dumps(rec)[: 40]  # half-written line from an interrupted export
    return rec


class ZerobusSink:
    """One Zerobus stream per gateway, recreated on failure (at-least-once; silver de-duplicates)."""

    def __init__(self, endpoint: str, table: str, scope: str):
        from databricks.sdk import WorkspaceClient
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk
        w = WorkspaceClient()
        self.cid = w.dbutils.secrets.get(scope, "zerobus_client_id")
        self.secret = w.dbutils.secrets.get(scope, "zerobus_client_secret")
        self.sdk, self.props = ZerobusSdk(endpoint, w.config.host), TableProperties(table)
        self.opts = StreamConfigurationOptions(record_type=RecordType.JSON)
        self.streams, self.last_offset = {}, {}

    def send(self, gw: str, records: list[dict]):
        for attempt in range(5):
            try:
                if gw not in self.streams:
                    self.streams[gw] = self.sdk.create_stream(self.cid, self.secret, self.props, self.opts)
                    log.info("opened Zerobus stream for %s", gw)
                for r in records:
                    self.last_offset[gw] = self.streams[gw].ingest_record_offset(r)
                return
            except Exception as e:
                log.warning("stream %s error (attempt %d): %s", gw, attempt + 1, e)
                try:
                    self.streams.pop(gw).close()
                except Exception:
                    pass
                time.sleep(min(2 ** attempt, 10))
        raise RuntimeError(f"giving up on gateway {gw}")

    def wait_durable(self):
        for gw, off in self.last_offset.items():
            self.streams[gw].wait_for_offset(off)

    def close(self):
        for s in self.streams.values():
            s.flush()
            s.close()


class FileSink:
    """JSON-lines files per gateway. Written to a hidden temp name, then renamed, so Auto Loader never sees half a file."""

    def __init__(self, landing: str, run_tag: str):
        self.landing, self.run_tag, self.n = landing, run_tag, 0
        os.makedirs(landing, exist_ok=True)
        self.buf = defaultdict(list)

    def add(self, gw: str, line: str):
        self.buf[gw].append(line)

    def flush(self) -> int:
        files = 0
        for gw, lines in self.buf.items():
            self.n += 1
            name = f"{gw}_{self.run_tag}_{self.n:05d}.json"
            tmp = os.path.join(self.landing, f".{name}.tmp")
            with open(tmp, "w") as f:
                f.write("\n".join(lines) + "\n")
            os.rename(tmp, os.path.join(self.landing, name))
            files += 1
        self.buf.clear()
        return files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sink", choices=["both", "zerobus", "files"], default="both")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--zerobus-table", default="telemetry_zerobus_raw")
    ap.add_argument("--zerobus-endpoint", default="")
    ap.add_argument("--secret-scope", default="pdm-demo")
    ap.add_argument("--volume", default="landing")
    ap.add_argument("--duration-min", type=float, default=3)
    ap.add_argument("--files-interval-s", type=int, default=10)
    ap.add_argument("--bad-record-rate", type=float, default=0.02)
    ap.add_argument("--run-tag", default="")
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--src-path", default="")
    a, _ = ap.parse_known_args()

    run_tag = a.run_tag or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    vol = f"/Volumes/{a.catalog}/{a.schema}/{a.volume}"
    rng = random.Random(a.seed)
    stations = build_station_master()
    sinks_used = {route(s["plant_id"], a.sink) for s in stations}
    zb = ZerobusSink(a.zerobus_endpoint, f"{a.catalog}.{a.schema}.{a.zerobus_table}", a.secret_scope) \
        if "zerobus" in sinks_used else None
    fs = FileSink(f"{vol}/telemetry", run_tag) if "files" in sinks_used else None
    t_start = time.time()
    fleet = Fleet(stations, seed=a.seed, t0=t_start)
    sent, defects, files = Counter(), defaultdict(Counter), 0
    log.info("starting run_tag=%s sink=%s sinks=%s stations=%d duration=%.1f min bad_rate=%.3f",
             run_tag, a.sink, sorted(sinks_used), len(stations), a.duration_min, a.bad_record_rate)
    tick, last = 0, t_start
    try:
        while time.time() - t_start < a.duration_min * 60:
            t = float(int(time.time()))
            readings, _ = fleet.step(t)
            zb_batches = defaultdict(list)
            for i, s in enumerate(stations):
                sink = route(s["plant_id"], a.sink)
                rec = make_record(s, readings, i, t, sink, run_tag)
                out = [rec]
                if rng.random() < a.bad_record_rate:
                    kind = rng.choice(SINK_DEFECTS[sink])
                    defects[sink][kind] += 1
                    out = [rec, dict(rec)] if kind == "duplicate" else [inject(rec, kind, sink)]
                for r in out:
                    sent[sink] += 1
                    if sink == "zerobus":
                        zb_batches[s["gateway_id"]].append(r)
                    else:
                        fs.add(s["gateway_id"], r if isinstance(r, str) else json.dumps(r))
            for gw, recs in zb_batches.items():
                zb.send(gw, recs)
            if fs and tick % a.files_interval_s == a.files_interval_s - 1:
                files += fs.flush()
            if zb and tick % 10 == 0:
                zb.wait_durable()
            tick += 1
            if time.time() - last >= 30:
                log.info("STATS sent=%s files=%d defects=%s", dict(sent), files, {k: dict(v) for k, v in defects.items()})
                last = time.time()
            time.sleep(max(0.0, t + 1 - time.time()))
    finally:
        if fs:
            files += fs.flush()
        if zb:
            zb.wait_durable()
            zb.close()
    manifest = {
        "run_tag": run_tag, "sink": a.sink, "finished_at": datetime.now(timezone.utc).isoformat(),
        "records_sent": dict(sent), "files_written": files,
        "defects": {k: dict(v) for k, v in defects.items()},
        "expected_quarantine": {k: sum(n for d, n in v.items() if d in HARD_DEFECTS) for k, v in defects.items()},
        "expected_soft_violations": {k: sum(n for d, n in v.items() if d in SOFT_DEFECTS) for k, v in defects.items()},
        "expected_duplicates": {k: v.get("duplicate", 0) for k, v in defects.items()},
    }
    os.makedirs(f"{vol}/_manifests", exist_ok=True)
    with open(f"{vol}/_manifests/{run_tag}.json", "w") as f:
        json.dump(manifest, f, indent=2)
    log.info("MANIFEST %s", json.dumps(manifest))


if __name__ == "__main__":
    main()
