import { timePickerDefaultProps } from 'element-plus/es/components/time-picker/index';
import { buildProps, definePropType } from 'element-plus/es/utils/index';

import type { ExtractPropTypes } from 'vue';
import type { IDatePickerType } from '../date-picker.type';

export const datePickerProps = buildProps({
  ...timePickerDefaultProps,
  /**
   * @description type of the picker
   */
  type: {
    type: definePropType<IDatePickerType>(String),
    default: 'date'
  }
} as const);

export type DatePickerProps = ExtractPropTypes<typeof datePickerProps>;
