export interface XnodeFormState {
  endpoint: string;
  user: string;
  pass: string;
  token?: string;
}

export type XnodeValidationError = 'endpoint' | 'credentials' | 'authMode' | 'user' | null;

export interface XnodeSqlResult {
  column_meta: Array<[string, ...unknown[]]>;
  data: Array<unknown[]>;
}

export interface XnodeRow {
  [key: string]: unknown;
}

const XNODE_USER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function escapeSqlString(value: string): string {
  return value.replaceAll('\\', '\\\\').replaceAll("'", "''");
}

export function validateXnodeForm(form: XnodeFormState): XnodeValidationError {
  const endpoint = form.endpoint.trim();
  const user = form.user.trim();
  const pass = form.pass.trim();
  const token = (form.token ?? '').trim();

  if (!endpoint) return 'endpoint';
  if (token && (user || pass)) return 'authMode';
  if ((user && !pass) || (!user && pass)) return 'credentials';
  if (user && !XNODE_USER_PATTERN.test(user)) return 'user';
  return null;
}

export function buildCreateXnodeSql(form: XnodeFormState): string {
  const endpoint = escapeSqlString(form.endpoint.trim());
  const user = form.user.trim();
  const pass = escapeSqlString(form.pass.trim());
  const token = escapeSqlString((form.token ?? '').trim());

  return token
    ? `create xnode '${endpoint}' token '${token}';`
    : user && pass
    ? `create xnode '${endpoint}' user ${user} pass '${pass}';`
    : `create xnode '${endpoint}';`;
}

export function buildDropXnodeSql(id: unknown): string {
  const normalizedId =
    typeof id === 'string' && /^\d+$/.test(id.trim())
      ? Number(id.trim())
      : typeof id === 'number'
        ? id
        : Number.NaN;

  if (!Number.isInteger(normalizedId) || normalizedId <= 0) {
    throw new Error('invalid xnode id');
  }
  return `drop xnode ${normalizedId};`;
}

export function normalizeXnodeRows(result: XnodeSqlResult): XnodeRow[] {
  return result.data.map(row => {
    const normalizedRow = Object.fromEntries(result.column_meta.map((item, index) => [item[0], row[index]]));
    if (normalizedRow.endpoint == null && normalizedRow.url != null) {
      normalizedRow.endpoint = normalizedRow.url;
    }
    return normalizedRow;
  });
}

export function hasAnyXnode(rows: XnodeRow[]): boolean {
  return rows.length > 0;
}
