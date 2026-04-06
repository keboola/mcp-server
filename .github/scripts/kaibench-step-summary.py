import json, os
from pathlib import Path

runs = sorted(Path('results').glob('run_*'), key=lambda p: p.stat().st_mtime)
if not runs:
    raise SystemExit(0)
latest = runs[-1]
if not (latest / 'summary.json').exists():
    raise SystemExit(0)
s = json.loads((latest / 'summary.json').read_text())
m = s['metrics']
mcp_sha = os.environ.get('GITHUB_SHA', 'N/A')[:12]

# Load per-question results for tool usage stats
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

# Load previous run for regression comparison (downloaded into prev-results/)
prev_by_qid = {}
prev_summary = None
prev_runs = sorted(Path('prev-results').glob('run_*'), key=lambda p: p.stat().st_mtime) if Path('prev-results').exists() else []
if prev_runs:
    prev_run = prev_runs[-1]
    prev_file = prev_run / 'results.jsonl'
    prev_summary_file = prev_run / 'summary.json'
    if prev_file.exists():
        for line in prev_file.read_text().splitlines():
            if line.strip():
                try:
                    pr = json.loads(line)
                except json.JSONDecodeError:
                    continue
                prev_by_qid[str(pr.get('question_id', ''))] = pr
    if prev_summary_file.exists():
        prev_summary = json.loads(prev_summary_file.read_text())

# Aggregate tool stats
all_tool_calls = []
for r in evaluated:
    all_tool_calls.extend(r.get('trace', {}).get('tool_calls', []))
total_tools = len(all_tool_calls)
avg_tools = total_tools / len(evaluated) if evaluated else 0
avg_duration = sum(r.get('duration_ms', 0) for r in evaluated) / len(evaluated) / 1000 if evaluated else 0
total_tokens = sum(r.get('trace', {}).get('total_tokens', 0) or 0 for r in evaluated)
avg_tokens = total_tokens / len(evaluated) if evaluated else 0

# Tool call metrics
tool_errors = [tc for tc in all_tool_calls if tc.get('is_error')]
tool_approvals = [tc for tc in all_tool_calls if tc.get('approval_required')]
tool_denied = [tc for tc in all_tool_calls if tc.get('approval_required') and not tc.get('was_approved')]
tool_success_rate = (total_tools - len(tool_errors)) / total_tools if total_tools else 0

# Tool name frequency
from collections import Counter
tool_freq = Counter(tc.get('tool_name', 'unknown') for tc in all_tool_calls)

# Stream health
stream_errors = [r for r in evaluated if r.get('trace', {}).get('stream_error')]
early_terminations = [r for r in evaluated if r.get('trace', {}).get('stream_terminated_early')]
orphaned_questions = [r for r in evaluated if r.get('trace', {}).get('orphaned_tool_calls')]

print('## KaiBench Evaluation Results')
print()
print(f'MCP server commit: `{mcp_sha}`')
print()
print('| Metric | Value |')
print('|--------|-------|')
print(f'| Run ID | `{s["run_id"]}` |')
print(f'| Duration | {m["duration_seconds"]:.0f}s |')
print(f'| **Total** | **{m["total_questions"]}** |')
print(f'| Passed | {m["passed"]} |')
print(f'| Failed | {m["failed"]} |')
print(f'| Skipped | {m.get("skipped", 0)} |')
print(f'| Errors | {m.get("errors", 0)} |')
print(f'| **Pass Rate** | **{m["overall_pass_rate"]:.1%}** |')
avg_score = m.get('average_score')
if avg_score is not None:
    print(f'| Avg Score | {avg_score:.2f} |')
partial_count = sum(1 for r in evaluated if r.get('status') == 'partial')
if partial_count:
    print(f'| Partial | {partial_count} |')
print(f'| Total Tool Calls | {total_tools} |')
print(f'| Avg Tool Calls/Question | {avg_tools:.1f} |')
print(f'| Avg Duration/Question | {avg_duration:.0f}s |')
print(f'| Total Tokens | {total_tokens:,} |')
print(f'| Avg Tokens/Question | {avg_tokens:,.0f} |')

# Tool call metrics section
if all_tool_calls:
    print()
    print('### Tool Call Metrics')
    print()
    print('| Metric | Value |')
    print('|--------|-------|')
    print(f'| Tool Success Rate | {tool_success_rate:.1%} ({total_tools - len(tool_errors)}/{total_tools}) |')
    if tool_errors:
        print(f'| Tool Errors | {len(tool_errors)} |')
    if tool_approvals:
        print(f'| Approvals Required | {len(tool_approvals)} |')
    if tool_denied:
        print(f'| Approvals Denied | {len(tool_denied)} |')
    print()
    print('<details><summary>Tool usage breakdown</summary>')
    print()
    print('| Tool | Calls | Errors |')
    print('|------|-------|--------|')
    error_by_tool = Counter(tc.get('tool_name', 'unknown') for tc in tool_errors)
    for tool_name, count in tool_freq.most_common(20):
        errs = error_by_tool.get(tool_name, 0)
        err_str = str(errs) if errs else '-'
        print(f'| `{tool_name}` | {count} | {err_str} |')
    print()
    print('</details>')

# Stream health section
if stream_errors or early_terminations or orphaned_questions:
    print()
    print('### Stream Health')
    print()
    if stream_errors:
        print(f':warning: **{len(stream_errors)} question(s) had stream errors**')
        for r in stream_errors:
            trace = r.get('trace', {})
            qid = r.get('question_id', '?')
            err = trace.get('stream_error', 'unknown')
            code = trace.get('stream_error_code', '')
            code_str = f' (code: {code})' if code else ''
            print(f'- Q{qid}: {err}{code_str}')
        print()
    if orphaned_questions:
        print(f':warning: **{len(orphaned_questions)} question(s) had orphaned tool calls** (started but no output)')
        for r in orphaned_questions:
            trace = r.get('trace', {})
            qid = r.get('question_id', '?')
            orphaned = trace.get('orphaned_tool_calls', [])
            print(f'- Q{qid}: {len(orphaned)} orphaned call(s)')
        print()
    if early_terminations:
        for r in early_terminations:
            if r not in stream_errors and r not in orphaned_questions:
                print(f':warning: **1 question had early stream termination** (Q{r.get("question_id","?")})')
                print()

# Regression comparison
if prev_by_qid or prev_summary is not None:
    pm = prev_summary['metrics'] if prev_summary is not None else {}
    prev_rate = pm.get('overall_pass_rate', 0)
    curr_rate = m['overall_pass_rate']
    delta = curr_rate - prev_rate
    arrow = ':arrow_up:' if delta > 0 else ':arrow_down:' if delta < 0 else ':left_right_arrow:'
    print()
    print('### Regression Comparison')
    print()
    print(f'Previous run: `{prev_runs[-1].name}`')
    print()
    print('| Metric | Previous | Current | Delta |')
    print('|--------|----------|---------|-------|')
    print(f'| Overall Pass Rate | {prev_rate:.1%} | {curr_rate:.1%} | {arrow} {delta:+.1%} |')
    prev_passed = pm.get('passed', 0)
    curr_passed = m['passed']
    print(f'| Passed | {prev_passed} | {curr_passed} | {curr_passed - prev_passed:+d} |')

    # Per-type comparison
    prev_by_type = {t['question_type']: t for t in (prev_summary or {}).get('by_question_type', [])}
    for t in s.get('by_question_type', []):
        qt = t['question_type']
        if qt in prev_by_type:
            pt = prev_by_type[qt]
            pr = pt.get('pass_rate', 0)
            cr = t.get('pass_rate', 0)
            td = cr - pr
            ta = ':arrow_up:' if td > 0 else ':arrow_down:' if td < 0 else ':left_right_arrow:'
            print(f'| {qt} | {pr:.1%} | {cr:.1%} | {ta} {td:+.1%} |')

    # Per-question regressions and improvements
    regressions = []
    improvements = []
    if prev_by_qid:
        for r in evaluated:
            qid = str(r.get('question_id', ''))
            if qid in prev_by_qid:
                prev_st = prev_by_qid[qid].get('status', '?')
                curr_st = r.get('status', '?')
                if prev_st == 'passed' and curr_st not in ('passed', 'skipped'):
                    regressions.append((qid, r.get('question_type', ''), prev_st, curr_st))
                elif prev_st != 'passed' and curr_st == 'passed':
                    improvements.append((qid, r.get('question_type', ''), prev_st, curr_st))

    if regressions:
        print()
        print(f':rotating_light: **{len(regressions)} Regression(s)**')
        print()
        print('| Q | Type | Previous | Current |')
        print('|---|------|----------|---------|')
        for qid, qt, ps, cs in regressions:
            print(f'| {qid} | {qt} | {ps} | {cs} |')

    if improvements:
        print()
        print(f':tada: **{len(improvements)} Improvement(s)**')
        print()
        print('| Q | Type | Previous | Current |')
        print('|---|------|----------|---------|')
        for qid, qt, ps, cs in improvements:
            print(f'| {qid} | {qt} | {ps} | {cs} |')

# Errors & failures detail
errors_and_failures = [r for r in evaluated if r.get('status') in ('error', 'failed', 'partial')]
if errors_and_failures:
    print()
    print('### Errors & Failures Detail')
    print()
    by_type = {}
    for r in errors_and_failures:
        qt = r.get('question_type', 'Unknown')
        by_type.setdefault(qt, []).append(r)
    for qt, items in sorted(by_type.items()):
        print(f'**{qt}**')
        print()
        for r in items:
            qid = r.get('question_id', '?')
            status = r.get('status', '?')
            emoji = {'failed': ':x:', 'error': ':warning:', 'partial': ':large_orange_diamond:'}.get(status, status)
            print(f'<details><summary>{emoji} Q{qid} ({status})</summary>')
            print()
            if r.get('error_message'):
                print(f'**Error:** `{r["error_message"][:200]}`')
                print()
            expected = str(r.get('expected_answer') or '-')[:200]
            extracted = str(r.get('extracted_answer') or '-')[:200]
            print(f'**Expected:** {expected}')
            print()
            print(f'**Extracted:** {extracted}')
            notes = r.get('verification', {}).get('notes', '')
            if notes:
                print()
                print(f'**Notes:** {notes[:300]}')
            score = r.get('verification', {}).get('score', r.get('score'))
            if score is not None:
                print()
                print(f'**Score:** {score}')
            print()
            print('</details>')
        print()

# Per-type breakdown
for t in s.get('by_question_type', []):
    print()
    print(f'### {t["question_type"]}')
    print(f'{t["passed_count"]}/{t["total_count"]} passed ({t.get("pass_rate", 0):.1%})')
    if t.get('skipped_count', 0):
        print(f'_{t["skipped_count"]} skipped_')

# Per-question table (only evaluated questions)
if evaluated:
    print()
    print('### Per-Question Results')
    print()
    print('| Q | Type | Status | Tools | Tokens | Duration | Expected | Extracted | Notes |')
    print('|---|------|--------|-------|--------|----------|----------|-----------|-------|')
    def sort_key(x):
        qid = str(x.get('question_id', '0'))
        try:
            return (0, int(qid), '')
        except ValueError:
            return (1, 0, qid)
    def _cell(s: str) -> str:
        return s.replace('|', r'\|').replace('\n', ' ').replace('\r', ' ')
    for r in sorted(evaluated, key=sort_key):
        qid = r.get('question_id', '?')
        qtype = (r.get('question_type') or '')[:12]
        status = r.get('status', '?')
        emoji = {'passed': ':white_check_mark:', 'failed': ':x:', 'error': ':warning:'}.get(status, status)
        tools = len(r.get('trace', {}).get('tool_calls', []))
        tokens = r.get('trace', {}).get('total_tokens', 0) or 0
        tokens_str = f'{tokens:,}' if tokens else '-'
        dur = f'{r.get("duration_ms", 0)/1000:.0f}s'
        expected = _cell(str(r.get('expected_answer') or '-')[:25])
        extracted = _cell(str(r.get('extracted_answer') or '-')[:25])
        notes = _cell((r.get('verification', {}).get('notes') or '')[:40])
        health = ''
        trace = r.get('trace', {})
        if trace.get('stream_error'):
            health = ' :boom:'
        elif trace.get('orphaned_tool_calls'):
            health = ' :grey_question:'
        print(f'| {qid} | {qtype} | {emoji}{health} | {tools} | {tokens_str} | {dur} | {expected} | {extracted} | {notes} |')

# MCP Tool Validation detail section
mcp_results = [r for r in evaluated if r.get('question_type') == 'MCP Tool Validation']
if mcp_results:
    print()
    print('### MCP Tool Validation Detail')
    print()
    for r in sorted(mcp_results, key=lambda x: (
        int(str(x.get('question_id', '')).split('-')[-1])
        if str(x.get('question_id', '')).split('-')[-1].isdigit()
        else str(x.get('question_id', ''))
    )):
        phase = r.get('question_id', '?')
        phase_status = r.get('status', '?')
        phase_emoji = {'passed': ':white_check_mark:', 'failed': ':x:', 'error': ':warning:', 'partial': ':large_orange_diamond:'}.get(phase_status, phase_status)
        extracted = r.get('extracted_answer')
        if isinstance(extracted, dict) and extracted:
            pass_ct = sum(1 for t in extracted.values() if t.get('status') == 'PASS')
            fail_ct = sum(1 for t in extracted.values() if t.get('status') == 'FAIL')
            warn_ct = sum(1 for t in extracted.values() if t.get('status') == 'WARN')
            skip_ct = sum(1 for t in extracted.values() if t.get('status') == 'SKIP')
            total_ct = len(extracted)
            print(f'<details><summary>{phase_emoji} <b>{phase}</b> — {pass_ct}/{total_ct} passed'
                  + (f', {warn_ct} warn' if warn_ct else '')
                  + (f', {fail_ct} fail' if fail_ct else '')
                  + (f', {skip_ct} skip' if skip_ct else '')
                  + '</summary>')
            print()
            print('| Test | Tool | Status | Trace | Description |')
            print('|------|------|--------|-------|-------------|')
            for tid in sorted(extracted.keys(), key=lambda x: int(x.split('-')[1]) if '-' in x and x.split('-')[1].isdigit() else 0):
                t = extracted[tid]
                st = t.get('status', '?')
                st_emoji = {'PASS': ':white_check_mark:', 'FAIL': ':x:', 'WARN': ':warning:', 'SKIP': ':fast_forward:'}.get(st, st)
                traced = ':white_check_mark:' if t.get('trace_verified') else ':x:'
                desc = _cell((t.get('description') or '')[:60])
                print(f'| {tid} | `{t.get("tool_name", "?")}` | {st_emoji} | {traced} | {desc} |')
            print()
            print('</details>')
        else:
            notes = (r.get('verification', {}).get('notes') or r.get('error_message') or '')[:80]
            print(f'- {phase_emoji} **{phase}**: {notes}')
