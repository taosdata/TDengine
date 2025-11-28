/// <reference types="vitest" />

import stylelintPlugin from 'vite-plugin-stylelint';
import vue from '@vitejs/plugin-vue';
import VueJsx from '@vitejs/plugin-vue-jsx';
import EslintPlugin from 'vite-plugin-eslint';
import AutoImport from 'unplugin-auto-import/vite';
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers';
import Components from 'unplugin-vue-components/vite';
import { createSvgIconsPlugin } from 'vite-plugin-svg-icons';
import UnoCss from 'unocss/vite';
import { join } from 'path';
import dts from 'vite-plugin-dts';
import { nodePolyfills } from 'vite-plugin-node-polyfills';

export default {
  base: './',
  resolve: {
    alias: {
      '@': './'
    },
    extensions: ['.js', '.json', '.ts', '.vue', 'jsx', 'tsx']
  },
  css: {
    preprocessorOptions: {
      scss: {}
    }
  },
  plugins: [
    nodePolyfills(),
    vue(),
    VueJsx(),
    UnoCss(),
    createSvgIconsPlugin({
      symbolId: 'icon-[name]',
      iconDirs: [resolve('/assets/icons')]
    }),
    stylelintPlugin({
      // 你可以在这里添加Stylelint的配置选项
      // 例如，指定要检查的文件
      include: ['**/*.{scss.vue}']
      // stylelintPath: '.stylelintrc'
      // 或者使用`.stylelintrc`、`stylelint.config.js`等配置文件
    }),
    AutoImport({
      resolvers: [
        ElementPlusResolver({
          importStyle: 'sass'
        })
      ],
      eslintrc: {
        enabled: true, // Default `false`
        filepath: resolve('./.eslintrc-auto-import.json'), // Default `./.eslintrc-auto-import.json`
        globalsPropValue: true // Default `true`, (true | false | 'readonly' | 'readable' | 'writable' | 'writeable')
      },
      imports: ['vue', '@vueuse/core'],
      dts: resolve('./types/auto-imports.d.ts')
    }),
    Components({
      resolvers: [
        ElementPlusResolver({
          importStyle: 'sass'
        })
      ],
      // 只导入文件夹下第一层的 index.vue 文件
      dts: resolve('./types/components.d.ts')
    }),
    EslintPlugin(),
    dts({
      tsconfigPath: './tsconfig.json',
      rollupTypes: true,
      exclude: ['App.vue', 'main.ts', '*.test.ts', '*.spec.ts'],
      insertTypesEntry: true
    })
  ],
  server: {
    port: 3000,
    hmr: true
  },
  test: {
    globals: true,
    environment: 'jsdom',
    testTimeout: 10000,
    setupFiles: './vitest.setup.ts',
    server: {
      deps: {
        inline: ['element-plus', 'vue-i18n']
      }
    },
    workspace: ['assets', 'utils', 'locales', 'constants', 'components', 'hooks', 'config'],
    // 启用覆盖率报告
    coverage: {
      // 指定覆盖率报告的输出目录
      reportsDirectory: './coverage',
      // 指定哪些文件需要收集覆盖率信息
      include: ['**/*.{js,ts,vue}'],
      // 指定哪些文件排除在覆盖率统计之外
      exclude: ['**/main.ts', '**/router/*', 'i18n/*', 'constants/*', 'config/*', '**/APP.vue'],
      // 设置覆盖率阈值，如果未达到这些阈值，测试将失败
      thresholds: {
        statements: 70,
        branches: 70,
        functions: 70,
        lines: 70
      },
      // 配置覆盖率报告的格式，支持多种格式
      reporter: ['text', 'lcov']
    }
  },
  // optimizeDeps: {
  //   include: ['buffer-polyfill.js'],
  //   esbuildOptions: {
  //     plugins: [NodeGlobalsPolyfillPlugin({})]
  //   }
  // },
  build: {
    lib: {
      entry: './components/index.ts',
      name: 'components',
      fileName: (format: string) => `components.${format}.js`,
      formats: ['es']
    },
    rollupOptions: {
      external: [
        'vue',
        // '@tdengine/websocket',
        '**/*.test.ts', // 排除测试文件
        '**/*.spec.ts' // 排除测试文件
      ],
      output: {
        globals: {
          vue: 'Vue'
        },
        preserveModules: true,
        exports: 'named',
        // 分离每个组件
        entryFileNames: '[name].js',
        chunkFileNames: '[name].js',
        assetFileNames: '[name].[ext]'
      }
    },
    outDir: 'dist'
  }
};

export function resolve(path: string) {
  return join(__dirname, path);
}
