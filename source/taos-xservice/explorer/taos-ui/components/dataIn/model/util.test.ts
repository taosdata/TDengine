import { describe, expect, it } from 'vitest';

import { recoverFromData } from './util';

describe('recoverFromData', () => {
  it('restores kinghist connect_timeout from legacy conn_timeout', () => {
    const display = { connect_timeout: '' };

    recoverFromData('kinghist', display, { conn_timeout: '45s' });

    expect(display.connect_timeout).toBe('45s');
  });

  it('prefers connect_timeout over legacy conn_timeout when both exist', () => {
    const display = { connect_timeout: '' };

    recoverFromData('kinghist', display, {
      conn_timeout: '45s',
      connect_timeout: '30s'
    });

    expect(display.connect_timeout).toBe('30s');
  });
});
