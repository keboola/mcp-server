import json
from pathlib import Path

run_dirs = sorted(Path('results').glob('run_*'), key=lambda p: p.stat().st_mtime)
if not run_dirs:
    raise SystemExit(0)
latest = run_dirs[-1]
if not (latest / 'summary.json').exists():
    raise SystemExit(0)
s = json.loads((latest / 'summary.json').read_text())
m = s['metrics']

print(f"passed={m['passed']}")
print(f"failed={m['failed']}")
print(f"total={m['total_questions']}")
print(f"pass_rate={m['overall_pass_rate']:.2f}")
print(f"duration={m['duration_seconds']:.0f}")
status = 'passed' if m['failed'] == 0 and m.get('errors', 0) == 0 and m.get('partial', 0) == 0 else 'failed'
print(f"status={status}")

# Count regressions vs previous run (downloaded into prev-results/)
regressions = 0
prev_runs = sorted(Path('prev-results').glob('run_*'), key=lambda p: p.stat().st_mtime) if Path('prev-results').exists() else []
if prev_runs:
    prev_file = prev_runs[-1] / 'results.jsonl'
    curr_file = latest / 'results.jsonl'
    if prev_file.exists() and curr_file.exists():
        prev_by_qid = {}
        for line in prev_file.read_text().splitlines():
            if line.strip():
                try:
                    pr = json.loads(line)
                except json.JSONDecodeError:
                    continue
                prev_by_qid[str(pr.get('question_id', ''))] = pr
        for line in curr_file.read_text().splitlines():
            if line.strip():
                try:
                    cr = json.loads(line)
                except json.JSONDecodeError:
                    continue
                qid = str(cr.get('question_id', ''))
                if qid in prev_by_qid:
                    if prev_by_qid[qid].get('status') == 'passed' and cr.get('status') not in ('passed', 'skipped'):
                        regressions += 1
print(f"regressions={regressions}")
