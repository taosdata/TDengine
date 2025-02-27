import { mount, type VueWrapper } from '@vue/test-utils';
import Echarts from '../Echarts';
import { nextTick } from 'vue';
import { beforeEach, describe, expect, it } from 'vitest';

describe('Echarts component', () => {
  let wrapper: VueWrapper<any, InstanceType<typeof Echarts>>;

  beforeEach(() => {
    wrapper = mount(Echarts, {
      props: {
        option: {},
        height: '400px',
        width: '100%',
        svg: false
      }
    });
  });

  it('should mount the component', () => {
    expect(wrapper.exists()).toBe(true);
  });

  it('should have default height and width', () => {
    const { height, width } = wrapper.props() as { height: string; width: string };
    expect(height).toBe('400px');
    expect(width).toBe('100%');
  });

  it('emits chartMounted event on mount', async () => {
    await nextTick();
    expect(wrapper.emitted()).toHaveProperty('chartMounted');
  });
});
