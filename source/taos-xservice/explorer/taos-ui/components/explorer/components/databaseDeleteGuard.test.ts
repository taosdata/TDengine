import { describe, it, expect } from 'vitest'
import {
  getTaskTargetDatabase,
  isTaskStatusBlockingDatabaseDeletion,
  findBlockingDatabaseTask,
} from './databaseDeleteGuard'

describe('databaseDeleteGuard', () => {
  it('reads target database from targetDB', () => {
    const task = { targetDB: 'db1', status: 'running' }
    expect(getTaskTargetDatabase(task)).toBe('db1')
  })

  it('reads target database from to_expand.subject when targetDB missing', () => {
    const task = { to_expand: { subject: 'db2' }, status: 'stopped' }
    expect(getTaskTargetDatabase(task)).toBe('db2')
  })

  it('treats only stopped, completed, failed as non-blocking', () => {
    expect(isTaskStatusBlockingDatabaseDeletion('stopped')).toBe(false)
    expect(isTaskStatusBlockingDatabaseDeletion('completed')).toBe(false)
    expect(isTaskStatusBlockingDatabaseDeletion('failed')).toBe(false)
  })

  it('treats created, queued, running, tick, stopping as blocking', () => {
    expect(isTaskStatusBlockingDatabaseDeletion('created')).toBe(true)
    expect(isTaskStatusBlockingDatabaseDeletion('queued')).toBe(true)
    expect(isTaskStatusBlockingDatabaseDeletion('running')).toBe(true)
    expect(isTaskStatusBlockingDatabaseDeletion('tick')).toBe(true)
    expect(isTaskStatusBlockingDatabaseDeletion('stopping')).toBe(true)
  })

  it('unknown or missing status blocks by default', () => {
    expect(isTaskStatusBlockingDatabaseDeletion(undefined)).toBe(true)
    expect(isTaskStatusBlockingDatabaseDeletion(null)).toBe(true)
    expect(isTaskStatusBlockingDatabaseDeletion('something-else')).toBe(true)
  })

  it('finds the first blocking task for a selected database', () => {
    const tasks = [
      { targetDB: 'db1', status: 'stopped' },
      { targetDB: 'db1', status: 'running', id: 2 },
      { targetDB: 'db1', status: 'queued', id: 3 },
    ]
    const found = findBlockingDatabaseTask(tasks, 'db1')
    expect(found).toBeDefined()
    expect((found as any).id).toBe(2)
  })

  it('finds blocking task when database is in to_expand.subject', () => {
    const tasks = [
      { to_expand: { subject: 'db1' }, status: 'running', id: 5 },
    ]
    const found = findBlockingDatabaseTask(tasks, 'db1')
    expect(found).toBeDefined()
    expect((found as any).id).toBe(5)
  })

  it('tasks targeting different database should not block', () => {
    const tasks = [
      { targetDB: 'db2', status: 'running' },
      { to_expand: { subject: 'db3' }, status: 'queued' },
    ]
    const found = findBlockingDatabaseTask(tasks, 'db1')
    expect(found).toBeUndefined()
  })
})
