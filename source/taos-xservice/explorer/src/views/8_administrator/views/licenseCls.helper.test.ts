import { describe, expect, it } from 'vitest';
import {
  buildClsInfoItems,
  buildClsLicensePayload,
  type ClsConfig,
  createDefaultClsConfig,
  isClassicActivationLocked,
  parseClsConfigFromVariables,
  shouldShowClsInfo,
  validateClsConfig
} from './licenseCls.helper';

function makeClsConfig(overrides: Partial<ClsConfig> = {}): ClsConfig {
  return {
    ...createDefaultClsConfig(),
    ...overrides
  };
}

describe('licenseCls.helper', () => {
  it('parses cls variables from show variables rows', () => {
    expect(
      parseClsConfigFromVariables([
        { name: 'supportVnodes', value: '100' },
        { name: 'clsEnabled', value: '1' },
        { name: 'clsRefreshInterval', value: '15' },
        { name: 'clsUrl', value: 'http://192.168.2.158:6072' },
        { name: 'clsLicenseId', value: 'lic-7f858400-a21e-406b-8874-2cc98207ced0' },
        { name: 'clsQuotaSlotId', value: 'tsdb-9' },
        { name: 'clsLastSucTime', value: '2026-05-09 10:00:00.000' },
        { name: 'clsLastReqTime', value: '2026-05-09 10:01:00.000' },
        { name: 'clsLastFailReason', value: 'connect timeout' }
      ])
    ).toEqual({
      clsEnabled: '1',
      clsRefreshInterval: '15',
      clsUrl: 'http://192.168.2.158:6072',
      clsLicenseId: 'lic-7f858400-a21e-406b-8874-2cc98207ced0',
      clsQuotaSlotId: 'tsdb-9',
      clsLastSucTime: '2026-05-09 10:00:00.000',
      clsLastReqTime: '2026-05-09 10:01:00.000',
      clsLastFailReason: 'connect timeout'
    });
  });

  it('hides cls info when cls is disabled', () => {
    expect(shouldShowClsInfo(createDefaultClsConfig())).toBe(false);
    expect(shouldShowClsInfo(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '15',
      clsUrl: 'http://192.168.2.158:6072',
      clsLicenseId: 'lic-7f858400-a21e-406b-8874-2cc98207ced0'
    }))).toBe(true);
  });

  it('locks classic activation while cls is enabled', () => {
    expect(isClassicActivationLocked(createDefaultClsConfig())).toBe(false);
    expect(isClassicActivationLocked(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '15',
      clsUrl: '',
      clsLicenseId: ''
    }))).toBe(true);
  });

  it('validates cls refresh interval boundaries when cls is enabled', () => {
    expect(validateClsConfig(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '9',
      clsUrl: 'http://192.168.2.158:6072',
      clsLicenseId: 'lic-1'
    }))).toBe('clsRefreshInterval');
    expect(validateClsConfig(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '86401',
      clsUrl: 'http://192.168.2.158:6072',
      clsLicenseId: 'lic-1'
    }))).toBe('clsRefreshInterval');
    expect(validateClsConfig(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '10',
      clsUrl: 'http://192.168.2.158:6072',
      clsLicenseId: 'lic-1'
    }))).toBe(null);
    expect(validateClsConfig(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '86400',
      clsUrl: 'http://192.168.2.158:6072',
      clsLicenseId: 'lic-1'
    }))).toBe(null);
  });

  it('requires cls url and license id only when cls is enabled', () => {
    expect(validateClsConfig(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '15',
      clsUrl: '',
      clsLicenseId: 'lic-1'
    }))).toBe('clsUrl');
    expect(validateClsConfig(makeClsConfig({
      clsEnabled: '1',
      clsRefreshInterval: '15',
      clsUrl: 'http://192.168.2.158:6072',
      clsLicenseId: ''
    }))).toBe('clsLicenseId');
    expect(validateClsConfig(makeClsConfig({
      clsEnabled: '0',
      clsRefreshInterval: '',
      clsUrl: '',
      clsLicenseId: ''
    }))).toBe(null);
  });

  it('builds cls payload with trimmed values', () => {
    expect(
      buildClsLicensePayload({
        clsEnabled: ' 1 ',
        clsRefreshInterval: ' 15 ',
        clsUrl: ' http://192.168.2.158:6072 ',
        clsLicenseId: ' lic-1 ',
        clsQuotaSlotId: ' tsdb-9 ',
        clsLastSucTime: '',
        clsLastReqTime: '',
        clsLastFailReason: ''
      })
    ).toEqual({
      cls_enabled: '1',
      cls_refresh_interval: '15',
      cls_url: 'http://192.168.2.158:6072',
      cls_license_id: 'lic-1',
      cls_quota_slot_id: 'tsdb-9'
    });
  });

  it('defaults cls quota slot id in payload when omitted', () => {
    expect(
      buildClsLicensePayload({
        clsEnabled: '1',
        clsRefreshInterval: '15',
        clsUrl: 'http://192.168.2.158:6072',
        clsLicenseId: 'lic-1',
        clsQuotaSlotId: '   ',
        clsLastSucTime: '',
        clsLastReqTime: '',
        clsLastFailReason: ''
      })
    ).toEqual({
      cls_enabled: '1',
      cls_refresh_interval: '15',
      cls_url: 'http://192.168.2.158:6072',
      cls_license_id: 'lic-1',
      cls_quota_slot_id: 'tsdb-1'
    });
  });

  it('builds cls info items and falls back to localized empty text', () => {
    expect(
      buildClsInfoItems(
        {
          clsEnabled: '1',
          clsRefreshInterval: '15',
          clsUrl: 'http://192.168.2.158:6072',
          clsLicenseId: 'lic-1',
          clsQuotaSlotId: 'tsdb-9',
          clsLastSucTime: '2026-05-09 10:00:00.000',
          clsLastReqTime: '2026-05-09 10:01:00.000',
          clsLastFailReason: ''
        },
        'None'
      )
    ).toEqual([
      { key: 'clsEnabled', value: '1' },
      { key: 'clsRefreshInterval', value: '15' },
      { key: 'clsUrl', value: 'http://192.168.2.158:6072' },
      { key: 'clsLicenseId', value: 'lic-1' },
      { key: 'clsQuotaSlotId', value: 'tsdb-9' },
      { key: 'clsLastSucTime', value: '2026-05-09 10:00:00.000' },
      { key: 'clsLastReqTime', value: '2026-05-09 10:01:00.000' },
      { key: 'clsLastFailReason', value: 'None' }
    ]);
  });
});
