import { describe, expect, it } from 'vitest';

import { getRouteQueryString } from './routeQuery';

describe('getRouteQueryString', () => {
  it('returns undefined when route is missing', () => {
    expect(getRouteQueryString(undefined, 'db')).toBeUndefined();
  });

  it('returns undefined when route query is missing', () => {
    expect(getRouteQueryString({}, 'db')).toBeUndefined();
  });

  it('returns undefined when query value is not a string', () => {
    expect(getRouteQueryString({ query: { db: ['test'] } }, 'db')).toBeUndefined();
  });

  it('returns the query value when it is a string', () => {
    expect(getRouteQueryString({ query: { db: 'test' } }, 'db')).toBe('test');
  });
});
