import { withInstall } from 'element-plus/es/utils/index';
import DatePicker from './src/date-picker.jsx';

import type { SFCWithInstall } from 'element-plus/es/utils/index';

export const ElDatePicker: SFCWithInstall<typeof DatePicker> = withInstall(DatePicker);

export default ElDatePicker;
export * from './src/constants.js';
export * from './src/props/date-picker.js';
export type { DatePickerInstance } from './src/instance.js';
