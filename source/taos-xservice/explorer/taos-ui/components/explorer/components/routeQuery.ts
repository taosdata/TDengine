export function getRouteQueryString(route: Recordable | undefined, key: string): string | undefined {
  const value = route?.query?.[key];
  return typeof value === 'string' && value ? value : undefined;
}
