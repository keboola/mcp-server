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

# Load results.jsonl to compute partial count (m dict never contains 'partial' key)
results = []
results_file = latest / 'results.jsonl'
if results_file.exists():
    for line in results_file.read_text().splitlines():
        if line.strip():
            try:
                results.append(json.loads(line))
            except json.JSONDecodeError:
                continue
evaluated = [r for r in results if r.get('status') != 'skipped']
partial_count = sum(1 for r in evaluated if r.get('status') == 'partial')

print(f"passed={m['passed']}")
print(f"failed={m['failed']}")
print(f"total={m['total_questions']}")
print(f"pass_rate={m['overall_pass_rate']:.2f}")
print(f"duration={m['duration_seconds']:.0f}")
status = 'passed' if m['failed'] == 0 and m.get('errors', 0) == 0 and partial_count == 0 else 'failed'
print(f"status={status}")

# Questions measured as unstable against an unmodified server. A single-trial comparison on a
# question whose base pass rate is well under 100% produces phantom regressions at that rate, so
# these are counted separately instead of failing the gate. Reported, never silently dropped.
flaky_path = Path(__file__).parent.parent / 'kaibench-flaky-questions.txt'
flaky_qids = set()
if flaky_path.exists():
    for raw in flaky_path.read_text().splitlines():
        entry = raw.split('#', 1)[0].strip()
        if entry:
            flaky_qids.add(entry)

# Count regressions vs previous run (downloaded into prev-results/)
# `baseline_run` stays empty when no comparison happened, so callers can tell "0 regressions"
# apart from "never compared" — the two look identical otherwise.
regressions = 0
regressed_qids = []
flaky_regressions = 0
flaky_regressed_qids = []
baseline_run = ''
prev_runs = sorted(Path('prev-results').glob('run_*'), key=lambda p: p.stat().st_mtime) if Path('prev-results').exists() else []
if prev_runs:
    prev_file = prev_runs[-1] / 'results.jsonl'
    if prev_file.exists() and results_file.exists():
        baseline_run = prev_runs[-1].name
        prev_by_qid = {}
        for line in prev_file.read_text().splitlines():
            if line.strip():
                try:
                    pr = json.loads(line)
                except json.JSONDecodeError:
                    continue
                prev_by_qid[str(pr.get('question_id', ''))] = pr
        for r in evaluated:
            qid = str(r.get('question_id', ''))
            if qid in prev_by_qid:
                if prev_by_qid[qid].get('status') == 'passed' and r.get('status') not in ('passed', 'skipped'):
                    if qid in flaky_qids:
                        flaky_regressions += 1
                        flaky_regressed_qids.append(qid)
                    else:
                        regressions += 1
                        regressed_qids.append(qid)
print(f"regressions={regressions}")
print(f"regressed_qids={','.join(sorted(regressed_qids))}")
print(f"flaky_regressions={flaky_regressions}")
print(f"flaky_regressed_qids={','.join(sorted(flaky_regressed_qids))}")
print(f"baseline_run={baseline_run}")
