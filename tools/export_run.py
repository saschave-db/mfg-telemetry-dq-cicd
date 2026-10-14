"""Export a Databricks job run as text evidence.

For every notebook task: an .ipynb with cell outputs and a readable .md.
For the run: a JSON summary (ids, URLs, states, durations).

Usage: python tools/export_run.py <job_run_id> <out_dir> [--profile P]
"""
import argparse
import base64
import html as htmllib
import json
import re
import subprocess
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

MAX_ROWS = 40


def cli(args, profile):
    out = subprocess.run(["databricks", *args, "--profile", profile, "-o", "json"],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat() if ms else None


def render_result(item) -> str:
    kind = item.get("type")
    data = item.get("data")
    if kind == "table":
        cols = [c["name"] for c in item.get("schema", [])]
        rows = data or []
        lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
        for r in rows[:MAX_ROWS]:
            lines.append("| " + " | ".join("" if v is None else str(v).replace("|", "/") for v in r) + " |")
        if len(rows) > MAX_ROWS:
            lines.append(f"... {len(rows) - MAX_ROWS} more rows")
        return "\n".join(lines)
    if kind == "html":
        text = re.sub(r"<[^>]+>", " ", str(data))
        return re.sub(r"\s+\n", "\n", htmllib.unescape(text)).strip()
    if kind == "error":
        return "ERROR: " + str(item.get("summary") or data)[:4000]
    if isinstance(data, str):
        return data
    return json.dumps(data)[:4000] if data else ""


NOISE = ("pyspark.sql.connect.logging", "application/vnd.jupyter.widget-view+json", "color_warning(",
         "Add type hints to the `predict` method")


def clean(text: str) -> str:
    """Drop runtime log noise (Spark Connect session warnings, progress widgets)."""
    return "\n".join(l for l in text.splitlines() if not any(n in l for n in NOISE)).strip()


def notebook_from_export(html: str):
    b64 = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html).group(1)
    return json.loads(urllib.parse.unquote(base64.b64decode(b64).decode()))


def cells(model):
    for c in sorted(model["commands"], key=lambda c: c.get("position", 0)):
        src = c.get("command", "")
        res = c.get("results") or {}
        outputs = []
        if res.get("type") == "listResults":
            outputs = [render_result(i) for i in res.get("data") or []]
        elif res:
            outputs = [render_result(res)]
        if c.get("error"):
            outputs.append("ERROR: " + str(c.get("errorSummary") or c["error"])[:4000])
        yield src, [clean(o) for o in outputs if clean(o)]


def to_ipynb(model, header):
    nb_cells = [{"cell_type": "markdown", "metadata": {}, "source": header}]
    for src, outs in cells(model):
        if src.lstrip().startswith("%md"):
            nb_cells.append({"cell_type": "markdown", "metadata": {},
                             "source": re.sub(r"^\s*%md\s*", "", src)})
            continue
        nb_cells.append({
            "cell_type": "code", "metadata": {}, "execution_count": None, "source": src,
            "outputs": [{"output_type": "stream", "name": "stdout", "text": o + "\n"} for o in outs],
        })
    return {"nbformat": 4, "nbformat_minor": 5, "cells": nb_cells,
            "metadata": {"language_info": {"name": "python"}}}


def to_md(model, header):
    parts = [header]
    for src, outs in cells(model):
        if src.lstrip().startswith("%md"):
            parts.append(re.sub(r"^\s*%md\s*", "", src))
            continue
        parts.append("```python\n" + src.strip() + "\n```")
        for o in outs:
            parts.append("Output:\n\n```text\n" + o.strip() + "\n```" if not o.startswith("|") else "Output:\n\n" + o)
    return "\n\n".join(parts) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id")
    ap.add_argument("out_dir")
    ap.add_argument("--profile", default="DEFAULT")
    a = ap.parse_args()
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    run = cli(["jobs", "get-run", a.run_id], a.profile)
    summary = {
        "job_id": run.get("job_id"), "run_id": run.get("run_id"), "run_name": run.get("run_name"),
        "run_page_url": run.get("run_page_url"),
        "state": run.get("state"), "status": run.get("status"),
        "start_time": ts(run.get("start_time")), "end_time": ts(run.get("end_time")),
        "tasks": [],
    }
    # Repair runs add later attempts of the same task: process in start order so the latest attempt wins.
    for t in sorted(run.get("tasks", []), key=lambda t: t.get("start_time", 0)):
        if t.get("state", {}).get("result_state") == "DISABLED":
            continue  # not selected in a partial run (run-now with "only")
        summary["tasks"].append({
            "task_key": t["task_key"], "run_id": t["run_id"], "state": t.get("state"),
            "start_time": ts(t.get("start_time")), "end_time": ts(t.get("end_time")),
            "duration_s": round((t.get("end_time", 0) - t.get("start_time", 0)) / 1000, 1),
        })
        if "spark_python_task" in t:
            o = cli(["jobs", "get-run-output", str(t["run_id"])], a.profile)
            log = (o.get("logs") or "") + ("\n" + o["error"] + "\n" + o.get("error_trace", "") if o.get("error") else "")
            name = Path(t["spark_python_task"]["python_file"]).stem
            (out / f"{name}.log").write_text(
                f"# stdout of task {t['task_key']} (task run {t['run_id']}) of job run {a.run_id}\n"
                f"# {run.get('run_page_url')}\n# truncated by Databricks: {o.get('logs_truncated', False)}\n\n" + log)
            print("wrote", out / f"{name}.log")
        if "notebook_task" not in t:
            continue
        exp = cli(["jobs", "export-run", str(t["run_id"]), "--views-to-export", "CODE"], a.profile)
        model = notebook_from_export(exp["views"][0]["content"])
        name = Path(t["notebook_task"]["notebook_path"]).stem
        header = (f"# Executed notebook: {name}\n\n"
                  f"Exported from Databricks job run `{a.run_id}` (task `{t['task_key']}`, task run `{t['run_id']}`).\n\n"
                  f"Result: **{t.get('state', {}).get('result_state')}** · "
                  f"start {ts(t.get('start_time'))} · end {ts(t.get('end_time'))}\n\n"
                  f"Run URL: {run.get('run_page_url')}\n")
        (out / f"{name}.ipynb").write_text(json.dumps(to_ipynb(model, header), indent=1))
        (out / f"{name}.md").write_text(to_md(model, header))
        print("wrote", out / f"{name}.ipynb")
    (out / f"run_{a.run_id}.json").write_text(json.dumps(summary, indent=2))
    print("wrote", out / f"run_{a.run_id}.json")


if __name__ == "__main__":
    main()
