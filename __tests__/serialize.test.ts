import { describe, expect, it } from 'vitest';

import { filterToonNulls, toonSerialize, toonSerializeCompact } from '@/serialize';

describe('filterToonNulls', () => {
  it('drops null/undefined keys from a plain object', () => {
    expect(filterToonNulls({ a: 1, b: null, c: undefined, d: 'x' })).toEqual({ a: 1, d: 'x' });
  });

  it('drops null keys from a single-item object list', () => {
    expect(filterToonNulls([{ a: 1, b: null }])).toEqual([{ a: 1 }]);
  });

  it('keeps union of value-bearing keys across a multi-item list, aligned with null', () => {
    const input = [
      { id: 1, name: 'a', extra: null },
      { id: 2, name: null, extra: 'y' },
    ];
    // `name` and `extra` each have a value in some row, so both columns survive,
    // with null where a given row lacks the value.
    expect(filterToonNulls(input)).toEqual([
      { id: 1, name: 'a', extra: null },
      { id: 2, name: null, extra: 'y' },
    ]);
  });

  it('recurses into nested objects', () => {
    expect(filterToonNulls({ a: { b: null, c: 2 } })).toEqual({ a: { c: 2 } });
  });
});

describe('toon serializers', () => {
  it('encodes an object list as an aligned TOON table', () => {
    const text = toonSerialize([
      { id: 'a', name: 'x' },
      { id: 'b', name: 'y' },
    ]);
    expect(text).toContain('[2]{id,name}');
    expect(text).toContain('a,x');
  });

  it('compact form omits all-null columns', () => {
    const text = toonSerializeCompact({ a: 1, b: null });
    expect(text).toContain('a: 1');
    expect(text).not.toContain('b:');
  });
});
