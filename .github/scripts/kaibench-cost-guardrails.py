import json
from pathlib import Path

MAX_TOKENS_PER_QUESTION = 100000
MAX_TOTAL_TOKENS = 2000000

run_dirs = sorted(Path('results').glob('run_*'), key=lambda p: p.stat().st_mtime)
if not run_dirs:
    print("::notice::No results/run_* directories found; skipping token usage checks.")
    raise SystemExit(0)
latest = run_dirs[-1]
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
total_tokens = sum(r.get('trace', {}).get('total_tokens', 0) or 0 for r in evaluated)
violations = []

if total_tokens == 0:
    print("::notice::Token usage not available — KaiClient may need updating")
else:
    if total_tokens > MAX_TOTAL_TOKENS:
        violations.append(f"Total tokens ({total_tokens:,}) exceeds limit ({MAX_TOTAL_TOKENS:,})")

    for r in evaluated:
        qid = r.get('question_id', '?')
        tokens = r.get('trace', {}).get('total_tokens', 0) or 0
        if tokens > MAX_TOKENS_PER_QUESTION:
            violations.append(f"Q{qid}: {tokens:,} tokens exceeds per-question limit ({MAX_TOKENS_PER_QUESTION:,})")

    if violations:
        for v in violations:
            print(f"::warning::{v}")
    else:
        print(f"::notice::Token usage OK — {total_tokens:,} total tokens across {len(evaluated)} questions")
