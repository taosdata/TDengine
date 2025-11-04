import { DataItem } from '../SqlCondition/utils';
export interface TDFnDataStruct {
  fn: string;
  params?: Recordable;
}

export interface WindowClauseValue {
  tol_val?: number;
  tol_unit?: string;
  interval_val?: number;
  column?: string;
  interval_unit?: string;
  sliding_val?: number;
  sliding_unit?: string;
  partitionSet?: string[];
  window_type?: string;
  state_column?: string;
}

export interface SubqueryValue extends Partial<WindowClauseValue> {
  dbName: string;
  stbName: string;
  tbName: string;
  resultSet: any[];
  conditionJson: DataItem[];
}
