export type XnodePendingAction = () => void | Promise<void>;

export function createEnsureXnodeController(options: {
  hasXnode: () => Promise<boolean>;
  onMissingXnode: () => void | Promise<void>;
}) {
  return {
    async ensureXnodeThen(action: XnodePendingAction) {
      let hasXnode = false;

      try {
        hasXnode = await options.hasXnode();
      } catch (_error) {
        hasXnode = false;
      }

      if (hasXnode) {
        await action();
        return;
      }

      await options.onMissingXnode();
    }
  };
}
