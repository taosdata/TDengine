import { describe, expect, it, vi } from 'vitest';
import { createEnsureXnodeController } from './xnodeGate.helper';

describe('xnodeGate.helper', () => {
  it('runs the action immediately when xnodes already exist', async () => {
    const onMissingXnode = vi.fn();
    const action = vi.fn();
    const controller = createEnsureXnodeController({
      hasXnode: vi.fn().mockResolvedValue(true),
      onMissingXnode
    });

    await controller.ensureXnodeThen(action);

    expect(action).toHaveBeenCalledTimes(1);
    expect(onMissingXnode).not.toHaveBeenCalled();
  });

  it('prompts the user and does not run the blocked action when no xnode exists', async () => {
    const onMissingXnode = vi.fn();
    const action = vi.fn();
    const controller = createEnsureXnodeController({
      hasXnode: vi.fn().mockResolvedValue(false),
      onMissingXnode
    });

    await controller.ensureXnodeThen(action);

    expect(action).not.toHaveBeenCalled();
    expect(onMissingXnode).toHaveBeenCalledTimes(1);
  });

  it('treats xnode-check failures as blocked and falls back to the missing-xnode handler', async () => {
    const onMissingXnode = vi.fn();
    const action = vi.fn();
    const controller = createEnsureXnodeController({
      hasXnode: vi.fn().mockRejectedValue(new Error('show xnodes failed')),
      onMissingXnode
    });

    await expect(controller.ensureXnodeThen(action)).resolves.toBeUndefined();

    expect(action).not.toHaveBeenCalled();
    expect(onMissingXnode).toHaveBeenCalledTimes(1);
  });
});
