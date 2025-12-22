/// <reference types="vitest" />
import stylelintPlugin from 'vite-plugin-stylelint';
import vue from '@vitejs/plugin-vue';
import EslintPlugin from 'vite-plugin-eslint';
import { join } from 'path';
import AutoImport from 'unplugin-auto-import/vite';
import viteImagemin from '@vheemstra/vite-plugin-imagemin';
import imageminMozjpeg from 'imagemin-mozjpeg';
import imageminWebp from 'imagemin-webp';
import imageGifsicle from 'imagemin-gifsicle';
import imageminPngquant from 'imagemin-pngquant';
import imageSvgo from 'imagemin-svgo';
import { visualizer } from 'rollup-plugin-visualizer';
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers';
import Components from 'unplugin-vue-components/vite';
import { createSvgIconsPlugin } from 'vite-plugin-svg-icons';
// import viteCompression from 'vite-plugin-compression';
import UnoCss from 'unocss/vite';
import { loadEnv } from 'vite';
import { nodePolyfills } from 'vite-plugin-node-polyfills';
import VueJsx from '@vitejs/plugin-vue-jsx';
export function resolve(path) {
  return join(process.cwd(), path);
}
export function getBaseConfig(configEnv, additionalScss, deployUrl, commonIcons, viteDeploy) {
  const lifecycle = process.env.npm_lifecycle_event;
  const { VITE_SERVICE_PORT = 8080 } = loadEnv(configEnv.mode, process.cwd());
  const baseConfig = {
    base: './',
    resolve: {
      alias: {
        '@': resolve('src'),
        '@codemirror/state': resolve('node_modules/@codemirror/state')
      },
      extensions: ['.js', '.json', '.ts', '.vue', '.tsx', '.jsx']
    },
    css: {
      preprocessorOptions: {
        scss: {
          additionalData: additionalScss ? additionalScss : '',
          api: 'modern-compiler'
        }
      }
    },
    plugins: [
      nodePolyfills(),
      vue(),
      VueJsx(),
      UnoCss(),
      createSvgIconsPlugin({
        symbolId: 'icon-[name]',
        iconDirs: [
          resolve('./node_modules/taos-ui/assets/icons'),
          resolve('./src/assets/icons'),
          ...(commonIcons ? [resolve('../common/assets/icons')] : [])
        ]
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
        dirs: [resolve('src/store/modules/**')],
        imports: ['vue', 'pinia', 'vue-i18n', 'vue-router', '@vueuse/core'],
        dts: resolve('./types/auto-imports.d.ts')
      }),
      Components({
        resolvers: [
          ElementPlusResolver({
            importStyle: 'sass'
          })
        ],
        // 只导入文件夹下第一层的 index.vue 文件
        globs: ['src/components/*/index.vue', 'src/components/*.{vue,ts,tsx}'],
        dts: resolve('./types/components.d.ts')
      }),
      EslintPlugin(),
      lifecycle === 'report'
        ? visualizer({
            open: true,
            gzipSize: true,
            brotliSize: true,
            filename: 'report.html'
          })
        : null,
      // viteCompression({
      //   algorithm: 'gzip',
      //   threshold: 5120,
      //   deleteOriginFile: true,
      //   verbose: true
      // }),
      viteImagemin({
        plugins: {
          jpg: imageminMozjpeg({
            quality: 20
          }),
          gif: imageGifsicle({
            optimizationLevel: 7,
            interlaced: false
          }),
          png: imageminPngquant({
            quality: [0.6, 0.8]
          }),
          svg: imageSvgo({
            plugins: [
              {
                name: 'removeViewBox'
              },
              {
                name: 'removeEmptyAttrs',
                active: false
              }
            ]
          })
        },
        makeWebp: {
          plugins: {
            jpg: imageminWebp()
          }
        }
      }),
      deployUrl && viteDeploy ? viteDeploy(deployUrl) : null
    ],
    server: {
      port: VITE_SERVICE_PORT,
      hmr: true
    },
    build: {
      // sourcemap: false,
      // reportCompressedSize: false,
      // commonjsOptions: {
      //   sourceMap: false
      // },
      target: 'esnext',
      sourcemap: false,
      chunkSizeWarningLimit: 2500, // Default is 500
      minify: 'esbuild',
      rollupOptions: {
        external: [
          '**/*.test.ts', // 排除测试文件
          '**/*.spec.ts' // 排除测试文件
        ],
        output: {
          // 入口 js 文件
          entryFileNames: 'js/[name]-[hash].js',
          // 影响分包结果
          chunkFileNames: 'js/[name]-[hash].js',
          // 除了 js 的其他文件
          assetFileNames(assetInfo) {
            if (assetInfo.name?.endsWith('.css')) {
              return 'css/[name]-[hash].css';
            }
            const imgExts = ['.png', '.jpg', '.jpeg', '.webp', '.svg', '.gif', '.icon'];
            if (imgExts.some(ext => assetInfo.name?.endsWith(ext))) {
              return 'imgs/[name]-[hash][ext]';
            }
            return 'assets/[name]-[hash].[ext]';
          }
        }
      }
    },
    test: {
      globals: true,
      environment: 'jsdom',
      setupFiles: '../config/vitest.setup.ts',
      server: {
        deps: {
          inline: ['element-plus', '@vueuse/core', 'pinia', 'vue-i18n', 'vue-router']
        }
      },
      // 启用覆盖率报告
      coverage: {
        // 指定覆盖率报告的输出目录
        reportsDirectory: './coverage',
        // 指定哪些文件需要收集覆盖率信息
        include: ['**/*.{js,ts,vue}'],
        // 指定哪些文件排除在覆盖率统计之外
        exclude: ['**/main.ts', '**/router/*', 'i18n/*', 'constants1/*', 'config/*', '**/APP.vue'],
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
    }
  };
  return baseConfig;
}
