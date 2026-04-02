export function getTaskTargetDatabase(task: Recordable | undefined): string | undefined {
  if (!task || typeof task !== 'object') return undefined
  if (typeof task.targetDB === 'string' && task.targetDB.length > 0) return task.targetDB
  if (task.to_expand && typeof task.to_expand.subject === 'string' && task.to_expand.subject.length > 0) {
    return task.to_expand.subject
  }
  return undefined
}

const NON_BLOCKING_STATUSES = new Set(['stopped', 'completed', 'failed'])

export function isTaskStatusBlockingDatabaseDeletion(status: unknown): boolean {
  if (typeof status !== 'string') return true
  const normalized = status.toLowerCase()
  return !NON_BLOCKING_STATUSES.has(normalized)
}

export function findBlockingDatabaseTask(tasks: Recordable[] | undefined, dbName: string): Recordable | undefined {
  if (!Array.isArray(tasks) || !dbName) return undefined
  for (const t of tasks) {
    const target = getTaskTargetDatabase(t)
    if (target !== dbName) continue
    if (isTaskStatusBlockingDatabaseDeletion(t?.status)) return t
  }
  return undefined
}
