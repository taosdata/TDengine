import { defineConfig, loadEnv } from 'vite';
import vue from '@vitejs/plugin-vue';
import path, { join } from 'path';
import stylelintPlugin from 'vite-plugin-stylelint';
import EslintPlugin from 'vite-plugin-eslint';
import AutoImport from 'unplugin-auto-import/vite';
import { createHtmlPlugin } from 'vite-plugin-html';
// import VueDevTools from 'vite-plugin-vue-devtools';
import viteImagemin from '@vheemstra/vite-plugin-imagemin';
import imageminMozjpeg from 'imagemin-mozjpeg';
import imageminWebp from 'imagemin-webp';
import imageGifsicle from 'imagemin-gifsicle';
import imageminPngquant from 'imagemin-pngquant';
import imageSvgo from 'imagemin-svgo';
// import viteCompression from 'vite-plugin-compression';
import { visualizer } from 'rollup-plugin-visualizer';
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers';
import Components from 'unplugin-vue-components/vite';
import { createSvgIconsPlugin } from 'vite-plugin-svg-icons';
import UnoCss from 'unocss/vite';
import { nodePolyfills } from 'vite-plugin-node-polyfills';
import VueJsx from '@vitejs/plugin-vue-jsx';

export function resolve(path: string) {
  return join(process.cwd(), path);
}

// https://vitejs.dev/config/
export default ({ mode }: { mode: any }) => {
  const env = loadEnv(mode, process.cwd());
  const lifecycle = process.env.npm_lifecycle_event;
  return defineConfig({
    base: '/',
    resolve: {
      alias: {
        // 配置别名
        '@': path.resolve(__dirname, './src')
      },
      extensions: ['.js', '.json', '.ts', '.vue', '.mjs', '.jsx', '.tsx']
    },
    server: {
      port: 8080,
      hmr: true,
      host: true
    },
    css: {
      preprocessorOptions: {
        scss: {
          api: 'modern-compiler',
          additionalData: (content: string, loaderContext: any) => {
            // 检查文件路径是否包含 node_modules
            if (loaderContext.includes('taos-ui')) {
              return content;
            }
            // 如果不包含 node_modules，则引入全局 SCSS 文件
            return (
              `
              @use "@/styles/variables.scss" as *;
              @use "@/styles/index.scss" as *;
              @use "@/styles/element-plus/index.scss" as *;
              ` + content
            );
          }
        }
      }
    },
    plugins: [
      nodePolyfills(),
      vue(),
      VueJsx(),
      UnoCss(),
      // VueDevTools()
      createSvgIconsPlugin({
        symbolId: 'icon-[name]',
        iconDirs: [resolve('./src/assets/icons'), resolve('./node_modules/taos-ui/assets/icons')]
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
        // dirs: [resolve('src/store/modules/**')],
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
      createHtmlPlugin({
        /**
         * 需要注入 index.html ejs 模版的数据
         */
        inject: {
          data: {
            VITE_APP_CUS_NAME: env.VITE_APP_CUS_NAME,
            VITE_APP_CUS_PROMPT: env.VITE_APP_CUS_PROMPT,
            VITE_APP_CUS_COMMUNITY: env.VITE_APP_CUS_COMMUNITY,
            VITE_APP_INDUSTRY: env.VITE_APP_INDUSTRY
          }
        }
      }),

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
      })
    ],
    build: {
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
              return 'imgs/[name]-[hash].[ext]';
            }
            return 'assets/[name]-[hash].[ext]';
          }
        }
      }
    }
  });
};
