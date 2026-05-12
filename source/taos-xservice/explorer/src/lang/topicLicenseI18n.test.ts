import { describe, expect, it } from 'vitest';
import en from './en/topic';
import zh from './zh/topic';

describe('license topic i18n', () => {
  it('defines empty-state translations for cls failure reason fallback', () => {
    expect(en.topic.none).toBe('None');
    expect(zh.topic.none).toBe('无');
  });

  it('keeps the zh cls refresh interval label explicit about seconds', () => {
    expect(zh.topic.clsRefreshInterval).toContain('(s)');
  });

  it('keeps cls quota slot labels prefixed consistently across locales', () => {
    expect(en.topic.clsQuotaSlotId).toBe('CLS Slot ID');
    expect(zh.topic.clsQuotaSlotId).toBe('CLS 配额 ID');
  });

  it('defines cls config section titles for both locales', () => {
    expect(en.topic.clsConfigInfo).toBe('CLS Config');
    expect(zh.topic.clsConfigInfo).toBe('CLS 配置');
  });
});
