const TRANSLATED_LOGIN_ERROR_KEYS = new Set([
  'captchaRequired',
  'captchaInputError',
  'clockOutOfSync',
  'invalidEncryptedPassword'
]);

export function getLoginErrorMessage(
  desc: unknown,
  translate: (key: string) => string,
  fallback: string
): string {
  if (typeof desc !== 'string' || !desc) {
    return fallback;
  }

  if (TRANSLATED_LOGIN_ERROR_KEYS.has(desc)) {
    return translate(`login.${desc}`);
  }

  return desc;
}

export function isCaptchaLoginError(desc: unknown): boolean {
  return desc === 'captchaRequired' || desc === 'captchaInputError';
}
