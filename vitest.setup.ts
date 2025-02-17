import { config } from '@vue/test-utils';
import { buildVueDompurifyHTMLDirective } from 'vue-dompurify-html';
import { i18n } from './locales/index';
import 'virtual:uno.css';

config.global.directives = {
  'dompurify-html': buildVueDompurifyHTMLDirective()
};
config.global.plugins = [i18n];
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query: Recordable) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(), // deprecated
    removeListener: vi.fn(), // deprecated
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn()
  }))
});
