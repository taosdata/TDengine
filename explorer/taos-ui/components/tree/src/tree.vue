<template>
  <div
    ref="el$"
    :class="[
      ns.b(),
      ns.is('dragging', !!dragState.draggingNode),
      ns.is('drop-not-allow', !dragState.allowDrop),
      ns.is('drop-inner', dragState.dropType === 'inner'),
      { [ns.m('highlight-current')]: highlightCurrent }
    ]"
    role="tree"
  >
    <tree-node
      v-for="child in root.childNodes"
      :key="getNodeKey(child)"
      :node="child"
      :props="props"
      :accordion="accordion"
      :max-content-height="maxContentHeight"
      :render-after-expand="renderAfterExpand"
      :show-checkbox="showCheckbox"
      :render-content="renderContent"
      @node-expand="handleNodeExpand"
    />
    <div v-if="isEmpty" :class="ns.e('empty-block')">
      <slot name="empty">
        <span :class="ns.e('empty-text')">
          {{ emptyText ?? t('el.tree.emptyText') }}
        </span>
      </slot>
    </div>
    <div v-show="dragState.showDropIndicator" ref="dropIndicator$" :class="ns.e('drop-indicator')" />
  </div>
</template>
<script lang="ts">
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck

import { computed, defineComponent, getCurrentInstance, inject, provide, ref, watch } from 'vue';
import { iconPropType } from 'element-plus/es/utils/index';
import { useLocale, useNamespace } from 'element-plus/es/hooks/index';
import { formItemContextKey } from 'element-plus/es/components/form/src/constants';
import { selectKey } from 'element-plus/es/components/select/src/token';
import TreeStore from './model/tree-store';
import { getNodeKey as getNodeKeyUtil, handleCurrentChange } from './model/util';
import TreeNode from './tree-node.vue';
import { useNodeExpandEventBroadcast } from './model/useNodeExpandEventBroadcast';
import { useDragNodeHandler } from './model/useDragNode';
import { useKeydown } from './model/useKeydown';
import type Node from './model/node';

import type { ComponentInternalInstance, PropType } from 'vue';
import type { Nullable } from 'element-plus/es/utils/index';
import type { TreeComponentProps, TreeData, TreeKey, TreeNodeData } from './tree.type';
import 'element-plus/es/components/base/style/css';
import 'element-plus/theme-chalk/el-tree.css';
import 'element-plus/es/components/checkbox/style/css';

export default defineComponent({
  name: 'Tree',
  components: { TreeNode },
  props: {
    data: {
      type: Array,
      default: () => []
    },
    // eslint-disable-next-line vue/require-default-prop
    emptyText: {
      type: String
    },
    renderAfterExpand: {
      type: Boolean,
      default: true
    },
    // eslint-disable-next-line vue/require-default-prop
    nodeKey: String,
    checkStrictly: Boolean,
    defaultExpandAll: Boolean,
    expandOnClickNode: {
      type: Boolean,
      default: true
    },
    // 当 icon 点击时，是否切换当前选中的节点
    iconClickChangeCurrentNode: {
      type: Boolean,
      default: false
    },
    checkOnClickNode: Boolean,
    checkDescendants: {
      type: Boolean,
      default: false
    },
    autoExpandParent: {
      type: Boolean,
      default: true
    },
    // eslint-disable-next-line vue/require-default-prop
    defaultCheckedKeys: Array as PropType<TreeComponentProps['defaultCheckedKeys']>,
    // eslint-disable-next-line vue/require-default-prop
    defaultExpandedKeys: Array as PropType<TreeComponentProps['defaultExpandedKeys']>,
    // eslint-disable-next-line vue/require-default-prop
    currentNodeKey: [String, Number] as PropType<string | number>,
    // eslint-disable-next-line vue/require-default-prop
    renderContent: Function,
    showCheckbox: {
      type: Boolean,
      default: false
    },
    draggable: {
      type: Boolean,
      default: false
    },
    isDragMoveNode: {
      type: Boolean,
      default: true
    },
    // eslint-disable-next-line vue/require-default-prop
    allowDrag: Function,
    // eslint-disable-next-line vue/require-default-prop
    allowDrop: Function,
    props: {
      type: Object as PropType<TreeComponentProps['props']>,
      default: () => ({
        children: 'children',
        label: 'label',
        disabled: 'disabled'
      })
    },
    lazy: {
      type: Boolean,
      default: false
    },
    highlightCurrent: Boolean,
    // eslint-disable-next-line vue/require-default-prop
    load: Function as PropType<TreeComponentProps['load']>,
    // eslint-disable-next-line vue/require-default-prop
    filterNodeMethod: Function as PropType<TreeComponentProps['filterNodeMethod']>,
    accordion: Boolean,
    indent: {
      type: Number,
      default: 18
    },
    icon: {
      type: iconPropType,
      default: 'CaretRight'
    },
    maxContentHeight: {
      type: String,
      default: ''
    },
    pageSize: {
      type: Number,
      default: 10
    },
    showChildrenLeftLine: {
      type: Boolean,
      default: false
    }
  },
  emits: [
    'check-change',
    'current-change',
    'node-click',
    'node-dblclick',
    'node-contextmenu',
    'node-collapse',
    'node-expand',
    'check',
    'node-drag-start',
    'node-drag-end',
    'node-drop',
    'node-drag-leave',
    'node-drag-enter',
    'node-drag-over',
    'all-child-node-loaded'
  ],
  setup(props, ctx) {
    const { t } = useLocale();
    const ns = useNamespace('tree');
    const selectInfo = inject(selectKey, null);

    const store = ref<TreeStore>(
      new TreeStore({
        key: props.nodeKey,
        data: props.data,
        lazy: props.lazy,
        pageSize: props.pageSize,
        props: props.props,
        load: props.load,
        currentNodeKey: props.currentNodeKey,
        checkStrictly: props.checkStrictly,
        checkDescendants: props.checkDescendants,
        defaultCheckedKeys: props.defaultCheckedKeys,
        defaultExpandedKeys: props.defaultExpandedKeys,
        autoExpandParent: props.autoExpandParent,
        defaultExpandAll: props.defaultExpandAll,
        filterNodeMethod: props.filterNodeMethod
      })
    );

    store.value.initialize();

    const root = ref<Node>(store.value.root);
    const currentNode = computed(() => store.value.currentNode);
    const el$ = ref<Nullable<HTMLElement>>(null);
    const dropIndicator$ = ref<Nullable<HTMLElement>>(null);

    const { broadcastExpanded } = useNodeExpandEventBroadcast(props);

    const { dragState } = useDragNodeHandler({
      props,
      ctx,
      el$,
      dropIndicator$,
      store
    });

    useKeydown({ el$ }, store);

    const isEmpty = computed(() => {
      const { childNodes } = root.value;
      const hasFilteredOptions = selectInfo ? selectInfo.hasFilteredOptions !== 0 : false;
      return (
        (!childNodes || childNodes.length === 0 || childNodes.every(({ visible }) => !visible)) && !hasFilteredOptions
      );
    });

    watch(
      () => props.currentNodeKey,
      newVal => {
        store.value.setCurrentNodeKey(newVal);
      }
    );

    watch(
      () => props.defaultCheckedKeys,
      newVal => {
        store.value.setDefaultCheckedKey(newVal);
      }
    );

    watch(
      () => props.defaultExpandedKeys,
      newVal => {
        store.value.setDefaultExpandedKeys(newVal);
      }
    );

    watch(
      () => props.data,
      newVal => {
        store.value.setData(newVal);
      },
      { deep: true }
    );

    watch(
      () => props.checkStrictly,
      newVal => {
        store.value.checkStrictly = newVal;
      }
    );

    const filter = value => {
      if (!props.filterNodeMethod) throw new Error('[Tree] filterNodeMethod is required when filter');
      store.value.filter(value);
    };

    const getNodeKey = (node: Node) => {
      return getNodeKeyUtil(props.nodeKey, node.data);
    };

    const getNodePath = (data: TreeKey | TreeNodeData) => {
      if (!props.nodeKey) throw new Error('[Tree] nodeKey is required in getNodePath');
      const node = store.value.getNode(data);
      if (!node) return [];
      const path = [node.data];
      let parent = node.parent;
      while (parent && parent !== root.value) {
        path.push(parent.data);
        parent = parent.parent;
      }
      return path.reverse();
    };

    const getCheckedNodes = (leafOnly?: boolean, includeHalfChecked?: boolean): TreeNodeData[] => {
      return store.value.getCheckedNodes(leafOnly, includeHalfChecked);
    };

    const getCheckedKeys = (leafOnly?: boolean): TreeKey[] => {
      return store.value.getCheckedKeys(leafOnly);
    };

    const getCurrentNode = (): TreeNodeData => {
      const currentNode = store.value.getCurrentNode();
      return currentNode ? currentNode : null;
    };

    const getCurrentKey = (): any => {
      if (!props.nodeKey) throw new Error('[Tree] nodeKey is required in getCurrentKey');
      const currentNode = getCurrentNode();
      return currentNode ? currentNode[props.nodeKey] : null;
    };

    const setCheckedNodes = (nodes: Node[], leafOnly?: boolean) => {
      if (!props.nodeKey) throw new Error('[Tree] nodeKey is required in setCheckedNodes');
      store.value.setCheckedNodes(nodes, leafOnly);
    };

    const setCheckedKeys = (keys: TreeKey[], leafOnly?: boolean) => {
      if (!props.nodeKey) throw new Error('[Tree] nodeKey is required in setCheckedKeys');
      store.value.setCheckedKeys(keys, leafOnly);
    };

    const setChecked = (data: TreeKey | TreeNodeData, checked: boolean, deep: boolean) => {
      store.value.setChecked(data, checked, deep);
    };

    const getHalfCheckedNodes = (): TreeNodeData[] => {
      return store.value.getHalfCheckedNodes();
    };

    const getHalfCheckedKeys = (): TreeKey[] => {
      return store.value.getHalfCheckedKeys();
    };

    const setCurrentNode = (node: Node, shouldAutoExpandParent = true) => {
      if (!props.nodeKey) throw new Error('[Tree] nodeKey is required in setCurrentNode');

      handleCurrentChange(store, ctx.emit, () => {
        broadcastExpanded(node);
        store.value.setUserCurrentNode(node, shouldAutoExpandParent);
      });
    };

    const setCurrentKey = (key?: TreeKey, shouldAutoExpandParent = true) => {
      if (props.nodeKey == undefined) throw new Error('[Tree] nodeKey is required in setCurrentKey');

      handleCurrentChange(store, ctx.emit, () => {
        broadcastExpanded();
        store.value.setCurrentNodeKey(key, shouldAutoExpandParent);
      });
    };

    const getNode = (data: TreeKey | TreeNodeData): Node => {
      return store.value.getNode(data);
    };

    const remove = (data: TreeNodeData | Node) => {
      store.value.remove(data);
    };

    const append = (data: TreeNodeData, parentNode: TreeNodeData | TreeKey | Node) => {
      store.value.append(data, parentNode);
    };

    const insertBefore = (data: TreeNodeData, refNode: TreeKey | TreeNodeData | Node) => {
      store.value.insertBefore(data, refNode);
    };

    const insertAfter = (data: TreeNodeData, refNode: TreeKey | TreeNodeData | Node) => {
      store.value.insertAfter(data, refNode);
    };

    const handleNodeExpand = (nodeData: TreeNodeData, node: Node, instance: ComponentInternalInstance) => {
      broadcastExpanded(node);
      ctx.emit('node-expand', nodeData, node, instance);
    };

    const updateKeyChildren = (key: TreeKey, data: TreeData) => {
      if (!props.nodeKey) throw new Error('[Tree] nodeKey is required in updateKeyChild');
      store.value.updateChildren(key, data);
    };
    const updateNodeChildrenByKey = (key: TreeKey, callback?: AnyFunction) => {
      store.value.updateNodeChildrenByKey(key, callback);
    };

    // 根据当前节点的key,获取当前节点的所有层级相对父元素 node 路径列表
    const getNodePathNodeListByKey = (key: TreeKey | TreeData = store.value.currentNode?.key): Node[] => {
      const currentNode = store.value.getNode(key);
      if (!currentNode) return [];
      const path = [currentNode];
      let parent = currentNode.parent;
      while (parent && parent !== root.value) {
        path.push(parent);
        parent = parent.parent;
      }
      return path.reverse();
    };
    const getNodePathByKey = (key: TreeKey | TreeData = store.value.currentNode?.key): TreeData => {
      const currentNode = store.value.getNode(key);
      if (!currentNode) return [];
      const path = [currentNode.data];
      let parent = currentNode.parent;
      while (parent && parent !== root.value) {
        path.push(parent.data);
        parent = parent.parent;
      }
      return path.reverse();
    };
    provide('RootTree', {
      ctx,
      props,
      store,
      root,
      instance: getCurrentInstance()
    } as any);

    provide(formItemContextKey, undefined);

    return {
      ns,
      // ref
      store,
      root,
      currentNode,
      dragState,
      el$,
      dropIndicator$,

      // computed
      isEmpty,
      // methods
      getNodePathByKey,
      getNodePathNodeListByKey,
      filter,
      getNodeKey,
      getNodePath,
      getCheckedNodes,
      getCheckedKeys,
      getCurrentNode,
      getCurrentKey,
      setCheckedNodes,
      setCheckedKeys,
      setChecked,
      getHalfCheckedNodes,
      getHalfCheckedKeys,
      setCurrentNode,
      setCurrentKey,
      t,
      getNode,
      remove,
      append,
      insertBefore,
      insertAfter,
      handleNodeExpand,
      updateKeyChildren,
      updateNodeChildrenByKey
    };
  }
});
</script>
