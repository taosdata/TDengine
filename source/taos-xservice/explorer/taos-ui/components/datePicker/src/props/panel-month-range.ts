import { buildProps } from 'element-plus/es/utils/index';
import { panelRangeSharedProps } from './shared';

import type { ExtractPropTypes } from 'vue';

export const panelMonthRangeProps = buildProps({
  ...panelRangeSharedProps
} as const);

export const panelMonthRangeEmits = ['pick', 'set-picker-option', 'calendar-change'];

export type PanelMonthRangeProps = ExtractPropTypes<typeof panelMonthRangeProps>;
