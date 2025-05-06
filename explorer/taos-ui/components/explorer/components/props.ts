export interface CreateDbProps {
  showTitle?: boolean;
  dbList: Recordable[];
  formData?: Recordable;
  isHa?: boolean;
  isEdit?: boolean;
  version: string;
  updateApi: RequestApiFn;
}

export interface ColumnItemProps {
  modelValue: Recordable;
  isEdit?: boolean;
  isTag?: boolean;
  isAdd?: boolean;
  loading?: boolean;
  placeholder?: string;
  isTimestamp?: boolean;
  isCanSetPrimaryKey?: boolean;
  version: string;
}

export interface CreateStableProps extends ComponentCommonProps {
  columnsArray?: Recordable[];
  showTitle?: boolean;
  stbName?: string;
  isEdit?: boolean;
  version: string;
}

export interface CreateStableForm {
  name: string;
  columns: ColumnStruct[];
  tags: TagStruct[];
}

export interface ColumnStruct extends TagStruct {
  primaryKey?: boolean;
  encode?: string;
  compress?: string;
  level?: string;
}

export interface TagStruct {
  origin_field?: string;
  origin_length?: number;
  field: string;
  type: string;
  length: number;
}

export interface ComponentCommonProps {
  dbData: Recordable;
}

export interface CreateSubTbProps extends ComponentCommonProps {
  stbName: string;
  tbName?: string;
  isEdit?: boolean;
}
export interface CreateSubTbForm {
  name: string;
  stbTmpl: string;
  tags: SubTbTagStruct[];
}

export interface SubTbTagStruct extends ColumnStruct {
  value: string;
}
