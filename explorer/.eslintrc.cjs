const config = require('taos-ui/config/eslint.cjs');

module.exports = {
  ...config,
  rules: {
    '@typescript-eslint/no-explicit-any': 'off',
    'vue/multi-word-component-names': 'off',
    '@typescript-eslint/no-unused-vars': 'off',
  }
};
