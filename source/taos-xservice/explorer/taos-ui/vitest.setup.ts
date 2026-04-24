import { config } from '@vue/test-utils';
import { buildVueDompurifyHTMLDirective } from 'vue-dompurify-html';
import { i18n } from './locales/index';
import 'virtual:uno.css';

config.global.directives = {
  'dompurify-html': buildVueDompurifyHTMLDirective()
};
config.global.plugins = [i18n];
Object.defineProperty(URL, 'createObjectURL', {
  writable: true,
  value: vi.fn(() => 'blob:url')
});

Object.defineProperty(URL, 'revokeObjectURL', {
  writable: true,
  value: vi.fn()
});

HTMLCanvasElement.prototype.getContext = vi.fn(() => ({
  drawImage: vi.fn()
})) as any;
HTMLCanvasElement.prototype.toDataURL = vi.fn(() => 'data:image/png;base64,ZmFrZQ==');

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
