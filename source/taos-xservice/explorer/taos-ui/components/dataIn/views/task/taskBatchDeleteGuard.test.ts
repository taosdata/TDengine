import { describe, expect, it } from 'vitest';

import { filterBatchDeletableIds, isTaskStatusBatchDeletable } from './taskBatchDeleteGuard';

describe('taskBatchDeleteGuard', () => {
  it('allows created tasks to be batch deleted', () => {
    expect(isTaskStatusBatchDeletable('created')).toBe(true);
  });

  it('allows failed tasks even when status text contains stray whitespace', () => {
    expect(isTaskStatusBatchDeletable('failed')).toBe(true);
    expect(isTaskStatusBatchDeletable(' failed')).toBe(true);
  });

  it('blocks running tasks from batch delete', () => {
    expect(isTaskStatusBatchDeletable('running')).toBe(false);
  });

  it('filters selected rows down to batch-deletable ids only', () => {
    expect(
      filterBatchDeletableIds([
        { id: '1', status: 'created' },
        { id: '2', status: 'failed' },
        { id: '3', status: 'running' },
        { id: '4', status: 'queued' },
      ]),
    ).toEqual(['1', '2']);
  });
});
