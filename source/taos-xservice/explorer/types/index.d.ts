/* eslint-disable @typescript-eslint/no-explicit-any */
declare interface Fn<T = any, R = T> {
  (...arg: T[]): R;
}

// declare interface PromiseFn<T = any, R = T> {
//   (...arg: T[]): Promise<R>;
// }
//
declare type RefType<T> = T | null;

declare type DateType = Date | string | number;

declare type ElSize = 'default' | 'large' | 'small' | '';

interface ElementTableFilters {
  text: string;
  value: string;
}
declare type AnyFunction<T = any> = (...args: any[]) => T;
// 分页请求参数
declare interface PageQuery {
  currentPage: number;
  pageSize: number;
}
declare interface Pagination extends PageQuery {
  total: number;
}

declare interface PaginationResult<T = any> {
  content: T[];
  total: string | number;
}
declare type RequestApiFn<T = any> = AnyFunction<Promise<T>>;
declare type PaginationRequestApi<T = any> = RequestApiFn<PaginationResult<T>>;
declare interface LabelValue {
  label: string;
  value: any;
}
declare interface SortQuery {
  sort?: string;
  property?: string;
}
// 分页加排序
declare type PageSort = PageQuery & SortQuery;

declare type ResponseResult<T = any> = {
  code: number;
  data: T;
  msg: string;
};
declare enum ComponentLevelEnum {
  USER = 0,
  ORGANIZATION,
  INSTANCE,
  DATABASE,
  STABLE
}

declare type GrafanaProfile = {
  dashboards: Recordable<string>;
}
declare type ProfileResult = {
  cluster: string;
  cluster_native: string;
  dashboard: string;
  grpc: string;
  version: string;
  x_api: string;
  grafana: GrafanaProfile | null;
}

declare type GlobalCustomProperties = {
  $IS_COMMUNITY: boolean;
  $IS_TSDBLITE: boolean;
  $INDUSTRY: string;
  $IS_OEM: boolean;
  OEM_NAME: string;
  $error: Fn
}
