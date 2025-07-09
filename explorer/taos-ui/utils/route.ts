import { NavigationFailureType, NavigationFailure } from 'vue-router';

export function isRouteAborted(res: NavigationFailure | void | undefined) {
  return res instanceof Error && 'type' in res && (res as NavigationFailure).type & NavigationFailureType.aborted;
}
