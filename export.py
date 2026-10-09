"""Readable blind HTML and editable CSV; private full records stay outside the blind folder."""

import csv
import html
import json
import time
import uuid
from state import dumps

FIELDS = [
    "form",
    "trigger_condition",
    "first_divergence",
    "propagation",
    "observable_consequence",
    "oracle_type",
    "behavior_abstraction",
    "label_status",
]
UNSUPPORTED = "taco-mutation-light pipeline不支持"


def observation(raw):
    # No stderr, stack, source locations, container paths or internal execution IDs.
    exception = raw.get("exception_class")
    if exception not in (
        None,
        "ZeroDivisionError",
        "ValueError",
        "TypeError",
        "IndexError",
        "KeyError",
        "RuntimeError",
        "AssertionError",
        "SystemExit",
        "NameError",
        "AttributeError",
        "RecursionError",
        "OverflowError",
    ):
        exception = "program_exception"
    return {
        "status": raw["status"],
        "output": raw.get("output"),
        "exception_class": exception,
        "verdict": raw["verdict"],
    }


def public(sample):
    witness = sample["witness"]
    return {
        "test_fixture": bool(sample.get("task_provenance", {}).get("test_fixture")),
        "sample_id": sample["sample_id"],
        "spec": sample["spec"],
        "original_code": sample["original_code"],
        "input": witness["input"],
        "input_source": "official",
        "expected": witness["expected"],
        "answer_source": witness["answer_source"],
        "comparison": sample["execution_contract"]["comparison"],
        "entry": sample["execution_contract"]["entry"],
        "original_observation": observation(witness["original"]),
        "mutant_observation": observation(witness["mutant"]),
        "tested_count": sample["tested_count"],
        "official_count": sample["official_count"],
        "raw_official_count": sample["raw_official_count"],
        "form": "盲标阶段隐藏",
        "trigger_condition": UNSUPPORTED,
        "first_divergence": UNSUPPORTED,
        "propagation": UNSUPPORTED,
        "observable_consequence": [
            {
                "channel": c["channel"],
                "original_status": c["original_status"],
                "mutant_status": c["mutant_status"],
                "original": c["original"],
                "mutant": c["mutant"],
                "exception_class": observation(witness["mutant"])["exception_class"],
            }
            for c in sample["observable_consequence"]
        ],
        "oracle_type": sample["oracle_type"],
        "behavior_abstraction": UNSUPPORTED,
        "label_status": "unknown",
    }


def cell(value):
    value = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    # Keep editable CSV safe when opened in common spreadsheet applications.
    return "'" + value if value.startswith(("=", "+", "-", "@")) else value


def export(state):
    directory = (
        state.directory
        / "exports"
        / (time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6])
    )
    blind = directory / "blind"
    blind.mkdir(parents=True)
    samples = state.samples()
    (directory / "materials.jsonl").write_text(
        "".join(dumps(s) + "\n" for s in samples)
    )
    records = [public(s) for s in samples]
    (blind / "records.jsonl").write_text("".join(dumps(s) + "\n" for s in records))
    with (blind / "annotations.csv").open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["sample_id", *FIELDS, "notes"])
        writer.writeheader()
        for row in records:
            writer.writerow({k: cell(row.get(k, "")) for k in writer.fieldnames})

    def block(title, value):
        text = (
            value
            if isinstance(value, str)
            else json.dumps(value, ensure_ascii=False, indent=2)
        )
        return "<h3>" + html.escape(title) + "</h3><pre>" + html.escape(text) + "</pre>"

    cards = []
    for r in records:
        cards.append(
            "<article><h2>"
            + html.escape(r["sample_id"])
            + "</h2>"
            + "".join(
                block(k, r[k])
                for k in (
                    "spec",
                    "original_code",
                    "entry",
                    "input",
                    "expected",
                    "comparison",
                    "original_observation",
                    "mutant_observation",
                    "observable_consequence",
                    "oracle_type",
                )
            )
            + f"<p>已执行 {r['tested_count']} / {r['official_count']} 个可用官方案例（原始字段 {r['raw_official_count']} 个）。标签状态：unknown。</p></article>"
        )
    page = (
        """<!doctype html><html lang="zh"><meta charset="utf-8"><title>TACO 试标材料</title>
<style>body{font:16px/1.6 system-ui;max-width:1000px;margin:32px auto;padding:0 20px;background:#f5f6f8;color:#17202a}article{background:white;padding:24px;margin:24px 0;border-radius:12px}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f0f2f5;padding:16px}h3{font-size:15px;color:#40536b}@media print{article{break-before:page}}</style>
<h1>TACO 官方见证试标材料</h1><p>先阅读证据，在同目录 annotations.csv 中填写。未支持的字段保留指定占位文字；请勿把具体见证推广成未经验证的触发规律。</p>
"""
        + (
            "<p><strong>这是离线验收用合成示例，不是真实 LLM 试标产出。</strong></p>"
            if any(r["test_fixture"] for r in records)
            else ""
        )
        + "".join(cards)
        + "</html>"
    )
    (blind / "index.html").write_text(page)
    return {
        "samples": len(samples),
        "blind_page": str(blind / "index.html"),
        "annotations": str(blind / "annotations.csv"),
        "private_materials": str(directory / "materials.jsonl"),
    }
