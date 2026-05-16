<script lang="ts">
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
import { defineComponent, h, inject, renderSlot } from 'vue';

import { useNamespace } from 'element-plus/es/hooks/index';
import type { ComponentInternalInstance } from 'vue';
import type { RootTreeType } from './tree.type';

export default defineComponent({
  name: 'TreeNodeContent',
  props: {
    node: {
      type: Object,
      required: true
    },
    // eslint-disable-next-line vue/require-default-prop
    renderContent: Function
  },
  setup(props) {
    const ns = useNamespace('tree');
    const nodeInstance = inject<ComponentInternalInstance>('NodeInstance');
    const tree = inject<RootTreeType>('RootTree');
    return () => {
      const node = props.node;
      const { data, store } = node;
      return props.renderContent
        ? props.renderContent(h, { _self: nodeInstance, node, data, store })
        : renderSlot(tree.ctx.slots, 'default', { node, data }, () => [
            h('span', { class: ns.be('node', 'label') }, [node.label])
          ]);
    };
  }
});
</script>
