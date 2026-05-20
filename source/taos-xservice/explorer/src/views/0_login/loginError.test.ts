import { describe, expect, it } from 'vitest';
import { getLoginErrorMessage, isCaptchaLoginError } from './loginError';

const translate = (key: string) => `translated:${key}`;

describe('login error messages', () => {
  it('translates known backend login error keys', () => {
    expect(getLoginErrorMessage('clockOutOfSync', translate, 'fallback')).toBe(
      'translated:login.clockOutOfSync'
    );
    expect(getLoginErrorMessage('invalidEncryptedPassword', translate, 'fallback')).toBe(
      'translated:login.invalidEncryptedPassword'
    );
  });

  it('keeps unknown backend messages unchanged', () => {
    expect(getLoginErrorMessage('Invalid password', translate, 'fallback')).toBe('Invalid password');
  });

  it('uses fallback when backend message is empty', () => {
    expect(getLoginErrorMessage('', translate, 'fallback')).toBe('fallback');
  });

  it('recognizes captcha errors that need captcha refresh', () => {
    expect(isCaptchaLoginError('captchaRequired')).toBe(true);
    expect(isCaptchaLoginError('captchaInputError')).toBe(true);
    expect(isCaptchaLoginError('clockOutOfSync')).toBe(false);
  });
});
