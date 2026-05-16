import { withInstall } from 'element-plus/es/utils/index';
import Tree from './src/tree.vue';

import type { SFCWithInstall } from 'element-plus/es/utils/index';

export const ElTree: SFCWithInstall<typeof Tree> = withInstall(Tree);

export default ElTree;

export type { TreeInstance } from './src/instance';
