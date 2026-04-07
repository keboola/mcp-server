import json
from pathlib import Path
from collections import Counter

runs = sorted(Path('results').glob('run_*'), key=lambda p: p.stat().st_mtime)
if not runs:
    print("No runs found in results/")
    exit(0)
latest = runs[-1]
results_file = latest / 'results.jsonl'
if not results_file.exists():
    print("No results.jsonl found")
    exit(0)

results = []
for _l in results_file.read_text().splitlines():
    if _l.strip():
        try:
            results.append(json.loads(_l))
        except json.JSONDecodeError:
            continue
evaluated = [r for r in results if r.get('status') != 'skipped']
summary_file = latest / 'summary.json'
if not summary_file.exists():
    print("No summary.json found (run may have been interrupted)")
    exit(0)
summary = json.loads(summary_file.read_text())
m = summary['metrics']

# Load previous run (downloaded into prev-results/)
prev_by_qid = {}
prev_summary = None
prev_runs = sorted(Path('prev-results').glob('run_*'), key=lambda p: p.stat().st_mtime) if Path('prev-results').exists() else []
if prev_runs:
    prev_run = prev_runs[-1]
    prev_file = prev_run / 'results.jsonl'
    prev_sum_file = prev_run / 'summary.json'
    if prev_file.exists():
        for line in prev_file.read_text().splitlines():
            if line.strip():
                try:
                    pr = json.loads(line)
                except json.JSONDecodeError:
                    continue
                prev_by_qid[str(pr.get('question_id', ''))] = pr
    if prev_sum_file.exists():
        prev_summary = json.loads(prev_sum_file.read_text())

W = 72  # display width

# ── Overall Summary ──
print('=' * W)
print(f'  KAIBENCH VERBOSE RESULTS — {latest.name}')
print('=' * W)
print()

passed = m['passed']
failed = m['failed']
errors = m.get('errors', 0)
partial = sum(1 for r in evaluated if r.get('status') == 'partial')
total = m['total_questions']
skipped = m.get('skipped', 0)
avg_score = m.get('average_score', 0)
dur = m.get('duration_seconds', 0)

# Bar chart for pass/partial/fail/error
bar_w = 40
if total > 0:
    p_w = round(passed / total * bar_w)
    pt_w = round(partial / total * bar_w)
    f_w = round(failed / total * bar_w)
    e_w = bar_w - p_w - pt_w - f_w
    bar = '\u2588' * p_w + '\u2593' * pt_w + '\u2591' * f_w + '\u00b7' * max(0, e_w)
else:
    bar = '\u00b7' * bar_w
print(f'  [{bar}] {passed}/{total} passed ({m["overall_pass_rate"]:.1%})')
legend_parts = [f'\u2588 passed={passed}']
if partial: legend_parts.append(f'\u2593 partial={partial}')
if failed: legend_parts.append(f'\u2591 failed={failed}')
if errors or skipped:
    dot_parts = []
    if errors: dot_parts.append(f'errors={errors}')
    if skipped: dot_parts.append(f'skipped={skipped}')
    legend_parts.append('\u00b7 ' + ', '.join(dot_parts))
print(f'  {" | ".join(legend_parts)}')
print(f'  Avg Score: {avg_score:.2f} | Duration: {dur:.0f}s')
print()

# ── Aggregate Tool & Token Stats ──
all_tool_calls = []
for r in evaluated:
    all_tool_calls.extend(r.get('trace', {}).get('tool_calls', []))
total_tools = len(all_tool_calls)
total_tokens = sum(r.get('trace', {}).get('total_tokens', 0) or 0 for r in evaluated)
durations = [r.get('duration_ms', 0) / 1000 for r in evaluated]
tool_counts = [len(r.get('trace', {}).get('tool_calls', [])) for r in evaluated]
token_counts = [r.get('trace', {}).get('total_tokens', 0) or 0 for r in evaluated]
n = len(evaluated) or 1

tool_errors = [tc for tc in all_tool_calls if tc.get('is_error')]
tool_approvals = [tc for tc in all_tool_calls if tc.get('approval_required')]
tool_denied = [tc for tc in all_tool_calls if tc.get('approval_required') and not tc.get('was_approved')]

print('-' * W)
print('  AGGREGATE METRICS')
print('-' * W)
print(f'  {"Tool Calls":<24} total={total_tools:<8} avg={total_tools/n:<8.1f} '
      f'min={min(tool_counts) if tool_counts else 0:<6} max={max(tool_counts) if tool_counts else 0}')
print(f'  {"Tokens":<24} total={total_tokens:<8,} avg={total_tokens/n:<8,.0f} '
      f'min={min(token_counts) if token_counts else 0:<6,} max={max(token_counts) if token_counts else 0:,}')
print(f'  {"Duration (s)":<24} total={sum(durations):<8.0f} avg={sum(durations)/n:<8.0f} '
      f'min={min(durations) if durations else 0:<6.0f} max={max(durations) if durations else 0:.0f}')
if total_tools:
    err_rate = len(tool_errors) / total_tools
    print(f'  {"Tool Success Rate":<24} {(1 - err_rate):.1%} ({total_tools - len(tool_errors)}/{total_tools})'
          + (f'  |  errors={len(tool_errors)}' if tool_errors else ''))
if tool_approvals:
    print(f'  {"Approvals":<24} required={len(tool_approvals)}  denied={len(tool_denied)}')
print()

# ── Tool Usage Breakdown (top 15) ──
if all_tool_calls:
    tool_freq = Counter(tc.get('tool_name', '?') for tc in all_tool_calls)
    error_by_tool = Counter(tc.get('tool_name', '?') for tc in tool_errors)
    print('-' * W)
    print('  TOOL USAGE (top 15)')
    print('-' * W)
    print(f'  {"Tool":<36} {"Calls":>6} {"Errors":>7} {"Err%":>6}')
    print(f'  {"─" * 36} {"─" * 6} {"─" * 7} {"─" * 6}')
    for name, count in tool_freq.most_common(15):
        errs = error_by_tool.get(name, 0)
        err_pct = f'{errs/count:.0%}' if errs else '-'
        print(f'  {name:<36} {count:>6} {errs:>7} {err_pct:>6}')
    print()

# ── Stream Health ──
stream_errors = [r for r in evaluated if r.get('trace', {}).get('stream_error')]
early_terms = [r for r in evaluated if r.get('trace', {}).get('stream_terminated_early')]
orphaned = [r for r in evaluated if r.get('trace', {}).get('orphaned_tool_calls')]
if stream_errors or early_terms or orphaned:
    print('-' * W)
    print('  STREAM HEALTH ISSUES')
    print('-' * W)
    for r in stream_errors:
        trace = r.get('trace', {})
        qid = r.get('question_id', '?')
        err = trace.get('stream_error', '?')
        code = trace.get('stream_error_code', '')
        code_s = f' (code: {code})' if code else ''
        print(f'  [STREAM_ERR] Q{qid}: {err}{code_s}')
    for r in orphaned:
        qid = r.get('question_id', '?')
        orph = r.get('trace', {}).get('orphaned_tool_calls', [])
        print(f'  [ORPHANED]   Q{qid}: {len(orph)} orphaned tool call(s)')
    for r in early_terms:
        if r not in stream_errors and r not in orphaned:
            print(f'  [EARLY_TERM] Q{r.get("question_id", "?")}: stream terminated early')
    print()

# ── By Question Type (with partial, avg score, tool/token stats) ──
print('-' * W)
print('  BY QUESTION TYPE')
print('-' * W)
by_type = {}
for r in evaluated:
    qt = r.get('question_type', 'Unknown')
    by_type.setdefault(qt, []).append(r)
for qt in sorted(by_type):
    items = by_type[qt]
    t_passed = sum(1 for r in items if r.get('status') == 'passed')
    t_failed = sum(1 for r in items if r.get('status') == 'failed')
    t_partial = sum(1 for r in items if r.get('status') == 'partial')
    t_error = sum(1 for r in items if r.get('status') == 'error')
    t_total = len(items)
    t_rate = t_passed / t_total if t_total else 0
    t_scores = [r.get('verification', {}).get('score', r.get('score')) for r in items]
    t_scores = [s for s in t_scores if s is not None and isinstance(s, (int, float))]
    t_avg_score = sum(t_scores) / len(t_scores) if t_scores else 0
    t_tools = sum(len(r.get('trace', {}).get('tool_calls', [])) for r in items)
    t_tokens = sum(r.get('trace', {}).get('total_tokens', 0) or 0 for r in items)
    t_dur = sum(r.get('duration_ms', 0) for r in items) / 1000
    print()
    bw = 20
    pw = round(t_passed / t_total * bw) if t_total else 0
    ptw = round(t_partial / t_total * bw) if t_total else 0
    fw = bw - pw - ptw
    type_bar = '\u2588' * pw + '\u2593' * ptw + '\u2591' * fw
    print(f'  {qt}')
    print(f'  [{type_bar}] {t_passed}/{t_total} passed ({t_rate:.0%})'
          + (f' + {t_partial} partial' if t_partial else '')
          + (f' + {t_error} errors' if t_error else ''))
    print(f'  avg_score={t_avg_score:.2f}  tools={t_tools}  '
          f'tokens={t_tokens:,}  duration={t_dur:.0f}s')

print()

# ── Regression Comparison ──
regressions = []
improvements = []
if prev_by_qid or prev_summary:
    for r in evaluated:
        qid = str(r.get('question_id', ''))
        if qid in prev_by_qid:
            ps = prev_by_qid[qid].get('status', '?')
            cs = r.get('status', '?')
            if ps == 'passed' and cs != 'passed':
                regressions.append((qid, r.get('question_type', ''), ps, cs))
            elif ps != 'passed' and cs == 'passed':
                improvements.append((qid, r.get('question_type', ''), ps, cs))
    if prev_summary or regressions or improvements:
        print('-' * W)
        print('  REGRESSION COMPARISON')
        print('-' * W)
    if prev_summary:
        pm = prev_summary['metrics']
        prev_rate = pm.get('overall_pass_rate', 0)
        curr_rate = m['overall_pass_rate']
        delta = curr_rate - prev_rate
        arrow = '\u25b2' if delta > 0 else '\u25bc' if delta < 0 else '='
        print(f'  Pass rate: {prev_rate:.1%} -> {curr_rate:.1%} ({arrow} {delta:+.1%})')
    if regressions:
        print(f'  REGRESSIONS ({len(regressions)}):')
        for qid, qt, ps, cs in regressions:
            print(f'    Q{qid} ({qt}): {ps} -> {cs}')
    if improvements:
        print(f'  IMPROVEMENTS ({len(improvements)}):')
        for qid, qt, ps, cs in improvements:
            print(f'    Q{qid} ({qt}): {ps} -> {cs}')
    if regressions or improvements:
        print()

# ── Per-Question Detail ──
print('-' * W)
print('  PER-QUESTION DETAIL')
print('-' * W)

def sort_key(x):
    qid = str(x.get('question_id', '0'))
    try:
        return (0, int(qid), '')
    except ValueError:
        last = qid.split('-')[-1]
        if last.isdigit():
            return (0, int(last), '')
        return (1, 0, qid)

for r in sorted(evaluated, key=sort_key):
    qid = str(r.get('question_id', '?'))
    status = r.get('status', '?')
    qtype = r.get('question_type', '')
    score = r.get('verification', {}).get('score', r.get('score', '-'))
    trace = r.get('trace', {})
    tc_list = trace.get('tool_calls', [])
    tools = len(tc_list)
    tokens = trace.get('total_tokens', 0) or 0
    input_tok = trace.get('input_tokens', 0) or 0
    output_tok = trace.get('output_tokens', 0) or 0
    dur_s = r.get('duration_ms', 0) / 1000

    icon = {'passed': '\u2714', 'failed': '\u2718', 'error': '\u26a0', 'partial': '\u25d2'}.get(status, '?')

    # Regression indicator
    reg = ''
    if qid in prev_by_qid:
        ps = prev_by_qid[qid].get('status', '?')
        if ps == 'passed' and status != 'passed':
            reg = ' <<< REGRESSED'
        elif ps != 'passed' and status == 'passed':
            reg = ' >>> IMPROVED'

    print()
    print(f'  {icon} Q{qid} [{status.upper()}] — {qtype}{reg}')
    print(f'    score={score}  tools={tools}  tokens={tokens:,}'
          + (f' (in={input_tok:,} out={output_tok:,})' if input_tok or output_tok else '')
          + f'  duration={dur_s:.0f}s')

    # Stream health
    if trace.get('stream_error'):
        err = trace.get('stream_error', '?')
        code = trace.get('stream_error_code', '')
        print(f'    STREAM ERROR: {err}' + (f' (code: {code})' if code else ''))
    if trace.get('orphaned_tool_calls'):
        print(f'    ORPHANED TOOL CALLS: {len(trace["orphaned_tool_calls"])}')

    # Tool call summary for this question
    if tc_list:
        tc_freq = Counter(tc.get('tool_name', '?') for tc in tc_list)
        tc_errs = [tc for tc in tc_list if tc.get('is_error')]
        tool_summary = ', '.join(f'{n}x{c}' if c > 1 else n for n, c in tc_freq.most_common(8))
        if len(tc_freq) > 8:
            tool_summary += f' (+{len(tc_freq) - 8} more)'
        print(f'    tools: {tool_summary}')
        if tc_errs:
            err_names = Counter(tc.get('tool_name', '?') for tc in tc_errs)
            print(f'    tool errors: {", ".join(f"{n}({c})" for n, c in err_names.most_common())}')

    # Answer detail for non-passed
    if status == 'error':
        err = r.get('error_message', '')
        if err:
            print(f'    ERROR: {err[:300]}')
    elif status in ('failed', 'partial'):
        expected = str(r.get('expected_answer', ''))[:150]
        extracted = str(r.get('extracted_answer', ''))[:150]
        notes = r.get('verification', {}).get('notes', '')[:200]
        print(f'    expected:  {expected}')
        print(f'    extracted: {extracted}')
        if notes:
            print(f'    notes:     {notes}')

    # MCP Tool Validation sub-tests
    if qtype == 'MCP Tool Validation':
        extracted = r.get('extracted_answer')
        if isinstance(extracted, dict) and extracted:
            sub_pass = sum(1 for t in extracted.values() if t.get('status') == 'PASS')
            sub_fail = sum(1 for t in extracted.values() if t.get('status') == 'FAIL')
            sub_warn = sum(1 for t in extracted.values() if t.get('status') == 'WARN')
            sub_skip = sum(1 for t in extracted.values() if t.get('status') == 'SKIP')
            print(f'    sub-tests: {sub_pass} pass, {sub_fail} fail, '
                  f'{sub_warn} warn, {sub_skip} skip / {len(extracted)} total')
            for tid in sorted(extracted.keys(), key=lambda x: (0, int(x.split('-')[1])) if '-' in x and x.split('-')[1].isdigit() else (1, x)):
                t = extracted[tid]
                st = t.get('status', '?')
                if st != 'PASS':
                    desc = (t.get('description') or '')[:80]
                    print(f'      [{st}] {tid} ({t.get("tool_name", "?")}) — {desc}')

print()
print('=' * W)
