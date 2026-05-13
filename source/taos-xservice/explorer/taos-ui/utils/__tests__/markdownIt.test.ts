import { describe, it, expect } from 'vitest';
import markdownIt from '../markdownIt';
import hljs from 'highlight.js';

describe('markdownIt', () => {
  it('should highlight code blocks correctly', () => {
    const code = 'console.log("Hello, world!");';
    const lang = 'javascript';
    const result = markdownIt.render(`\`\`\`${lang}\n${code}\n\`\`\``);
    expect(result).toContain(hljs.highlight(code, { language: lang }).value);
  });

  it('should replace image src correctly', () => {
    const markdown = '![Alt text](/api/image.png)';
    const result = markdownIt.render(markdown);
    expect(result).toContain('src="/app/image.png"');
  });

  it('should not replace image src if not matching /api or /app', () => {
    const markdown = '![Alt text](/other/image.png)';
    const result = markdownIt.render(markdown);
    expect(result).toContain('src="/other/image.png"');
  });

  it('should handle invalid language gracefully', () => {
    const code = 'console.log("Hello, world!");';
    const lang = 'invalidlang';
    const result = markdownIt.render(`\`\`\`${lang}\n${code}\n\`\`\``);
    expect(result).toContain('');
  });
});
