export interface ClsConfig {
  clsEnabled: string | number;
  clsRefreshInterval: string | number;
  clsUrl: string;
  clsLicenseId: string;
  clsQuotaSlotId: string;
  clsLastSucTime: string;
  clsLastReqTime: string;
  clsLastFailReason: string;
}

export type ClsValidationError = 'clsRefreshInterval' | 'clsUrl' | 'clsLicenseId' | null;

export const DEFAULT_CLS_QUOTA_SLOT_ID = 'tsdb-1';
export const DEFAULT_CLS_URL = 'http://localhost:6072';
export const SHOW_VARIABLES_SETTLE_DELAY_MS = 700;
const CLS_REFRESH_INTERVAL_MIN = 10;
const CLS_REFRESH_INTERVAL_MAX = 86400;

export interface ClsInfoItem {
  key: keyof ClsConfig;
  value: string;
}

function normalizeString(value: unknown): string {
  return String(value ?? '').trim();
}

function readVariableName(row: Record<string, unknown>): string {
  return normalizeString(row.name ?? row.variable_name ?? row.variable ?? row[0]);
}

function readVariableValue(row: Record<string, unknown>): string {
  return normalizeString(row.value ?? row.current_value ?? row.val ?? row[1]);
}

export function createDefaultClsConfig(): ClsConfig {
  return {
    clsEnabled: '0',
    clsRefreshInterval: '',
    clsUrl: '',
    clsLicenseId: '',
    clsQuotaSlotId: '',
    clsLastSucTime: '',
    clsLastReqTime: '',
    clsLastFailReason: ''
  };
}

export function parseClsConfigFromVariables(rows: Array<Record<string, unknown>>): ClsConfig {
  const parsed = createDefaultClsConfig();

  rows.forEach(row => {
    const name = readVariableName(row);
    const value = readVariableValue(row);

    if (name === 'clsEnabled') {
      parsed.clsEnabled = value || '0';
    } else if (name === 'clsRefreshInterval') {
      parsed.clsRefreshInterval = value;
    } else if (name === 'clsUrl') {
      parsed.clsUrl = value;
    } else if (name === 'clsLicenseId') {
      parsed.clsLicenseId = value;
    } else if (name === 'clsQuotaSlotId') {
      parsed.clsQuotaSlotId = value;
    } else if (name === 'clsLastSucTime') {
      parsed.clsLastSucTime = value;
    } else if (name === 'clsLastReqTime') {
      parsed.clsLastReqTime = value;
    } else if (name === 'clsLastFailReason') {
      parsed.clsLastFailReason = value;
    }
  });

  return parsed;
}

export function shouldShowClsInfo(config: ClsConfig): boolean {
  return normalizeString(config.clsEnabled) !== '' && normalizeString(config.clsEnabled) !== '0';
}

export function isClassicActivationLocked(config: ClsConfig): boolean {
  return shouldShowClsInfo(config);
}

export function validateClsConfig(config: ClsConfig): ClsValidationError {
  if (!shouldShowClsInfo(config)) {
    return null;
  }

  const interval = Number(normalizeString(config.clsRefreshInterval));
  if (!Number.isInteger(interval) || interval < CLS_REFRESH_INTERVAL_MIN || interval > CLS_REFRESH_INTERVAL_MAX) {
    return 'clsRefreshInterval';
  }
  if (!normalizeString(config.clsUrl)) {
    return 'clsUrl';
  }
  if (!normalizeString(config.clsLicenseId)) {
    return 'clsLicenseId';
  }
  return null;
}

export function buildClsLicensePayload(config: ClsConfig) {
  return {
    cls_enabled: normalizeString(config.clsEnabled) || '0',
    cls_refresh_interval: normalizeString(config.clsRefreshInterval),
    cls_url: normalizeString(config.clsUrl),
    cls_license_id: normalizeString(config.clsLicenseId),
    cls_quota_slot_id: normalizeString(config.clsQuotaSlotId) || DEFAULT_CLS_QUOTA_SLOT_ID
  };
}

export function buildClsInfoItems(config: ClsConfig, emptyText: string): ClsInfoItem[] {
  if (!shouldShowClsInfo(config)) {
    return [];
  }

  return [
    { key: 'clsEnabled', value: String(config.clsEnabled) },
    { key: 'clsRefreshInterval', value: String(config.clsRefreshInterval || '') },
    { key: 'clsUrl', value: config.clsUrl || '' },
    { key: 'clsLicenseId', value: config.clsLicenseId || '' },
    { key: 'clsQuotaSlotId', value: config.clsQuotaSlotId || '' },
    { key: 'clsLastSucTime', value: config.clsLastSucTime || '' },
    { key: 'clsLastReqTime', value: config.clsLastReqTime || '' },
    { key: 'clsLastFailReason', value: config.clsLastFailReason || emptyText }
  ];
}

export { CLS_REFRESH_INTERVAL_MAX, CLS_REFRESH_INTERVAL_MIN };
