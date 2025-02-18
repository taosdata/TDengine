import { defineConfig, presetUno, presetIcons, presetTypography, presetWind } from 'unocss';
export default defineConfig({
  presets: [
    {
      name: 'custom',
      rules: [],
      shortcuts: [
        {
          'no-wrap': 'whitespace-nowrap overflow-hidden text-ellipsis',
          'flex-center': 'flex items-center justify-center',
          'flex-between': 'flex items-center justify-between',
          'flex-end': 'flex items-center justify-end',
          'flex-start': 'flex items-center justify-start'
        }
      ]
    },
    presetUno(),
    presetIcons({
      extraProperties: {
        display: 'inline-block',
        'vertical-align': 'middle'
        // ...
      }
    }),
    presetTypography(),
    presetWind()
  ]
});
