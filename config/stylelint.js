export default {
  root: true,
  extends: ['stylelint-config-standard-scss', 'stylelint-config-recess-order', 'stylelint-config-recommended-vue/scss'],
  plugins: ['stylelint-scss'],
  rules: {
    'no-empty-source': null,
    'scss/at-extend-no-missing-placeholder': null,
    'selector-class-pattern': [
      '^([a-z][a-z0-9]*)(-[a-z0-9]+|__[a-z0-9]+|--[a-z0-9]+)*$',
      {
        message:
          'Expected class name to use only hyphens (-), double hyphens (--) or double underscores (__) and no camelCase, uppercase letters, or other special characters'
      }
    ],
    'selector-id-pattern': [
      '^([a-z][a-z0-9]*)(-[a-z0-9]+|__[a-z0-9]+|--[a-z0-9]+)*$',
      {
        message:
          'Expected id name to use only hyphens (-), double hyphens (--) or double underscores (__) and no camelCase, uppercase letters, or other special characters'
      }
    ]
  },
  fix: true,
  ignorePatterns: ['node_modules', 'dist', 'coverage', '*/public/*', '*/assets/*']
};
