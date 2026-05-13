const DELETABLE_TASK_STATUSES = new Set([
  'created',
  'completed',
  'stopped',
  'failed',
  'interrupted',
  'ticked',
]);

export function isTaskStatusBatchDeletable(status?: string | null) {
  return DELETABLE_TASK_STATUSES.has((status || '').trim().toLowerCase());
}

export function filterBatchDeletableIds(tasks: Array<{ id: string; status?: string | null }>) {
  return tasks.filter(task => isTaskStatusBatchDeletable(task.status)).map(task => task.id);
}
