import MdRender from '../MdRender.vue';
import { mount, VueWrapper } from '@vue/test-utils';
import { describe, it, expect } from 'vitest';

describe('MdRender component', () => {
  const testContent = `# Hello, world!
This is a test markdown file.
- list item 1
- list item 2
- list item 3
`;
  const wrapper: VueWrapper<InstanceType<typeof MdRender>> = mount<typeof MdRender>(MdRender, {
    props: {
      content: testContent
    }
  });
  it('should mount the component', () => {
    expect(wrapper.exists()).toBeTruthy();
  });

  it('should render the content', () => {
    const h1 = wrapper.find('h1');
    expect(h1.exists()).toBeTruthy();
    expect(h1.text()).toBe('Hello, world!');
    const list = wrapper.findAll('li');
    expect(list.length).toBe(3);
    expect(list[2].text()).toBe('list item 3');
  });

  it('should render the content with html', () => {
    wrapper.setProps({
      content:
        testContent +
        `
<div type='button-wrapper'>
  <button>click me</button>
</div>
`
    });
    const button = wrapper.find('button');
    expect(button.exists()).toBeFalsy();
    const div = wrapper.find(`[type='button-wrapper']`);
    expect(div.exists()).toBeFalsy();
  });
});
