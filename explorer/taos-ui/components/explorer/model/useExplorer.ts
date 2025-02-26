import { ComputedRef } from 'vue';
export interface ExplorerProps {
  database: DatabaseProps;
  stable: StableProps;
  table: TableProps;
  favorite: FavoriteProps;
  isCloud: boolean;
  isCommunity: boolean;
  customCompCallback: (event: string) => void;
  pageTitle: string | ComputedRef<string>;
}

type SendSQLReqResponse<T extends boolean> = T extends true ? Recordable[] : RestApiResult;

// 定义 ExecuteSqlApiFn 类型
export interface ExecuteSqlApiFn {
  (sqlStr: string, composeData: true): Promise<SendSQLReqResponse<true>>;
  (sqlStr: string, composeData?: false): Promise<SendSQLReqResponse<false>>;
}
export interface DatabaseProps {
  isCanCreateDatabase: boolean;
  getStructApi: (name: string) => Promise<Recordable>;
  getDataSourceUsedList: RequestApiFn;
  deleteApi: (name: string) => Promise<any>;
  createApi: (data: Recordable) => Promise<any>;
  updateApi: (data: Recordable) => Promise<any>;
}

interface StableProps {
  getPermissionList?: (dbName: string) => Promise<Recordable[]>;
}
interface TableProps {}
interface FavoriteProps {
  api: {
    getList: RequestApiFn<Recordable[]>;
    getSharedList: RequestApiFn<Recordable[]>;
    add: (sql: string | Recordable) => Promise<void>;
    edit: (id: number, data: Recordable) => Promise<void>;
    addShared: (sql: string | Recordable) => Promise<void>;
    delete: (id: string) => Promise<void>;
    deleteShared: (id: string) => Promise<void>;
  };
  isCanDeleteFn?: (item: Recordable) => boolean;
}

export const explorerPropsKey = Symbol('explorerProps');
export const sqlProviderKey = Symbol('sqlProvider');

export function getExplorerProps(): ExplorerProps {
  return inject(explorerPropsKey) as ExplorerProps;
}

export function getSqlProvider() {
  return inject<Recordable>(sqlProviderKey) ?? {};
}

export let customCompCallback = (event: string) => {
  console.log(event);
};

export function setCustomCompCallback(callback: (event: string) => void) {
  customCompCallback = callback;
}
