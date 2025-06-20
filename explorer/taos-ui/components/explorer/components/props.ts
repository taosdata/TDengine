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
  canMoveToTag?: boolean;
  version: string;
}

export interface VirtualColumnProps {
  modelValue: VirtualNormalTableColumn;
  isEdit?: boolean;
  isAdd?: boolean;
  loading?: boolean;
  placeholder?: string;
  version: string;
  databases?: Recordable[];
  isTimestamp?: boolean;
}

export interface CreateStableProps extends ComponentCommonProps {
  columnsArray?: Recordable[];
  showTitle?: boolean;
  stbName?: string;
  isEdit?: boolean;
}

export interface CreateStableForm {
  name: string;
  columns: ColumnStruct[];
  tags: TagStruct[];
}

export interface CreateVirtualNormalTableProps extends ComponentCommonProps {
  dbName: string;
  tbName: string;
  columns: VirtualNormalTableColumn[];
}

/** 虚拟表创建需要的数据结构 */
export interface VirtualNormalTableColumn {
  /** The field name in the virtual table */
  field: string;
  /** The type of the column in the virtual table */
  type: string;
  /** The length of the column in the virtual table */
  length: number;
  /** The source database name for the column of the virtual table */
  database: string;
  /** Source table name for column of virtual table */
  table: string;
  /** The value here is the column name in the virtual table */
  value: string;
}

export interface CreateVirtualNormalTableForm {
  dbName: string;
  name: string;
  columns: VirtualNormalTableColumn[];
}
export interface CreateTableProps extends ComponentCommonProps {
  dbName: string;
  tbName: string;
  columns: ColumnStruct[];
}

export interface CreateTableForm {
  dbName: string;
  name: string;
  columns: ColumnStruct[];
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
  length2: number;
}

export interface ComponentCommonProps {
  version: string;
  dbData: Recordable;
}

export interface CreateSubTbProps extends ComponentCommonProps {
  dbName: string;
  stbName: string;
  isVirtual?: boolean;
  tbName?: string;
  isEdit?: boolean;
}

export interface VirtualTableColumn {
  database: string;
  table: string;
  value: string;
}
export interface CreateSubTbForm {
  name: string;
  stbTmpl: string;
  tags: SubTbTagStruct[];
  isVirtual: boolean;
  columns: VirtualTableColumn[];
  database?: string;
}

export interface SubTbTagStruct extends ColumnStruct {
  value: string;
}
