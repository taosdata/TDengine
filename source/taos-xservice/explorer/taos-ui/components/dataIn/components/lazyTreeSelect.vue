<template>
  <el-tree-select
    ref="treeSelectRef"
    v-model="innerValue"
    class="ds-tree-select"
    :data="treeData"
    :load="loadNode"
    lazy
    node-key="id"
    :props="treeProps"
    :check-strictly="true"
    :render-after-expand="false"
    :placeholder="placeholder"
    :clearable="clearable"
    :disabled="disabled"
    :teleported="true"
    :loading="rootLoading"
    @visible-change="onVisibleChange"
  >
    <template v-if="rootLoading" #empty>
      <div class="ds-tree-loading">
        <el-icon class="is-loading"><Loading /></el-icon>
        <span>Loading...</span>
      </div>
    </template>
  </el-tree-select>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { Loading } from '@element-plus/icons-vue';
import { request } from '@/utils/request.ts';
import pathDetector from '@/utils/pathDetector';
import { formatFromData, sourceForm, agentId, connectivityCheckResult } from '../model/util';

export interface LazyTreeNode {
  key: string;
  label: string;
  id: number;
  isLeaf: boolean;
  children?: LazyTreeNode[];
}

const props = withDefaults(
  defineProps<{
    modelValue: number;
    placeholder?: string;
    disabled?: boolean;
    clearable?: boolean;
    rootLabel?: string;
  }>(),
  {
    modelValue: 0,
    placeholder: '/',
    disabled: false,
    clearable: true,
    rootLabel: '/'
  }
);

const emit = defineEmits<{
  (e: 'update:modelValue', v: number): void;
}>();

const innerValue = computed({
  get: () => props.modelValue,
  set: v => emit('update:modelValue', Number(v))
});

const treeSelectRef = ref<any>(null);
const rootKey = '/';
const rootChildrenLoaded = ref(false);
const rootLoading = ref(false);

const treeData = ref<LazyTreeNode[]>([
  {
    key: rootKey,
    label: props.rootLabel,
    id: 0,
    isLeaf: false
  }
]);

watch(
  () => props.rootLabel,
  v => {
    rootChildrenLoaded.value = false;
    treeData.value = [
      {
        key: rootKey,
        label: v,
        id: 0,
        isLeaf: false
      }
    ];
  }
);

// When connectivity check result changes (e.g. user re-checks with different credentials),
// reset loaded state so nodes will be re-fetched on next dropdown open.
watch(
  () => connectivityCheckResult.value,
  () => {
    rootChildrenLoaded.value = false;
    treeData.value = [
      {
        key: rootKey,
        label: props.rootLabel,
        id: 0,
        isLeaf: false
      }
    ];
  }
);

const treeProps = {
  label: 'label',
  children: 'children',
  isLeaf: 'isLeaf'
} as const;

/**
 * Check whether the connectivity check has passed.
 * Only after the user fills in connection/auth info and clicks "Check Connectivity"
 * successfully should we call the point/options API.
 */
function isConnectionReady(): boolean {
  const r = connectivityCheckResult.value;
  return !!(r && r.data_source && r.valid !== false);
}

/**
 * 获取 Nodes
 * POST /ds/in/point/options
 * Payload: DataSetsReq JSON
 * Response (pspace only): {
 *   "nodes": [
 *     {"id":0,"name":"根节点","LongName":"/", "isLeaf": false},
 *     {"id":150016,"name":"北京","LongName":"/北京", "isLeaf": false}
 *   ]
 * }
 */
async function fetchChildren(parentId: number, parentKey: string): Promise<LazyTreeNode[]> {
  // Using current sourceForm to construct DSN
  const dsn = formatFromData(sourceForm);
  // Always set root to the current parent node ID for lazy children fetching
  (dsn as any).root = Number.isFinite(parentId) ? parentId : 0;
  // If pspace, ensure nodes mode for options
  try {
    const type = (dsn as any)?.type ?? (dsn as any)?.driver;
    if (type === 'pspace') {
      if (!(dsn as any).params || typeof (dsn as any).params !== 'object') {
        (dsn as any).params = {};
      }
      if (!('pspace_mode' in (dsn as any).params) && !('mode' in (dsn as any).params)) {
        (dsn as any).params.pspace_mode = 'nodes';
      }
    }
  } catch (e) {
    // noop: best-effort enrichment for pspace DSN
  }
  const payload: Record<string, any> = {
    from_json: dsn,
    categories: ['groups'],
    pattern: '',
    offset: 0,
    limit: 1000
  };
  if (agentId.value) payload.via = agentId.value;

  try {
    const data = await request({
      baseURL: pathDetector.getXApiBasePath(),
      url: '/ds/in/point/options',
      method: 'post',
      headers: {
        'Content-Type': 'application/json'
      },
      data: payload
    });

    // pSpace response changed to { nodes: [] }. Support both shapes.
    const raw = data as any;
    const list = Array.isArray(raw)
      ? raw
      : Array.isArray(raw?.nodes)
        ? raw.nodes
        : Array.isArray(raw?.data?.nodes)
          ? raw.data.nodes
          : [];

    const normalize = (n: any, idx: number): LazyTreeNode | null => {
      // Precise mapping for pSpace Node
      const key = n?.LongName ?? n?.long_name ?? n?.path ?? `${parentKey}/${idx + 1}`;
      const label = n?.name ?? n?.label ?? String(key);
      const id = Number(n?.id ?? 0);
      const hasChildren = n?.hasChildren ?? (Array.isArray(n?.children) ? n.children.length > 0 : undefined);
      const isLeaf = n?.isLeaf ?? n?.leaf ?? (hasChildren === undefined ? false : !hasChildren);

      return {
        key: String(key),
        label: String(label),
        id,
        isLeaf: Boolean(isLeaf)
      };
    };

    return list.map((n: any, i: number) => normalize(n, i)).filter((x: LazyTreeNode | null): x is LazyTreeNode => !!x);
  } catch (e) {
    // On failure, return empty children so UI won't break
    return [];
  }
}

function loadNode(node: any, resolve: (data: LazyTreeNode[]) => void) {
  // Do not call API if connectivity check has not passed yet.
  if (!isConnectionReady()) {
    resolve([]);
    return;
  }

  // element-plus passes a "Node" instance.
  const parentKey: string = node.level === 0 ? rootKey : String(node.data?.key);
  const parentId: number = node.level === 0 ? 0 : Number(node.data?.id ?? 0);

  fetchChildren(parentId, parentKey)
    .then(children => resolve(children))
    .catch(() => resolve([]));
}

function onVisibleChange(visible: boolean) {
  if (!visible) return;

  // Do not call API if connectivity check has not passed yet.
  if (!isConnectionReady()) return;

  // When opening the dropdown, if root children haven't been loaded yet,
  // proactively fetch them and update treeData so nodes appear immediately.
  if (!rootChildrenLoaded.value) {
    rootLoading.value = true;
    fetchChildren(0, rootKey)
      .then(children => {
        if (children.length > 0) {
          rootChildrenLoaded.value = true;
          treeData.value = [
            {
              key: rootKey,
              label: props.rootLabel,
              id: 0,
              isLeaf: false,
              children
            }
          ];
        }
      })
      .finally(() => {
        rootLoading.value = false;
      });
  }
}
</script>

<style scoped>
.ds-tree-select {
  width: 100%;
}

.ds-tree-loading {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 10px 0;
  color: var(--el-text-color-secondary);
  font-size: 14px;
}
</style>
