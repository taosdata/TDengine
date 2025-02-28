import { describe, it, expect } from 'vitest';
// 给 TextCopy 组件添加 完整的组件测试
import { mount } from '@vue/test-utils';
import TextCopy from '../TextCopy.vue';

describe('TextCopy.vue', () => {
  const text = 'new message';
  const wrapper = mount(TextCopy, {
    props: { text }
  });
  it('renders props.msg when passed', () => {
    expect(wrapper.text()).toMatch(text);
  });

  it.todo('The bytton should copy the text to the clipboard', async () => {
    const copyBtn = wrapper.find('button');
    expect(copyBtn.exists()).toBeTruthy();
  });
});
