/* eslint-disable @typescript-eslint/no-explicit-any */

declare const BMap: any;
declare const BMapGL: any;

declare global {
  declare const BMap: any;
  declare const BMapGL: any;
  // window
  declare interface Window {
    baiduMap: any;
    onBMapCallback: any;
    onBMapGLCallback: any;
  }
  // vue
  type VueNode = import('vue').VNodeChild | JSX.Element;

  declare type Writable<T> = {
    -readonly [P in keyof T]: T[P];
  };

  declare type Nullable<T> = T | null;
  declare type NonNullable<T> = T extends null | undefined ? never : T;
  type Recordable<T = any> = Record<string, T>;
  type DateType = string | Date | number;
  declare type AnyFunction<T = any> = (...args: any[]) => T;
  declare type RequestApiFn<T = any> = AnyFunction<Promise<T>>;
  declare interface LabelValue {
    label: string;
    value: any;
  }
  declare interface RestApiResult {
    data: string[][];
    column_meta: string[][];
    rows: number;
    timing?: unknown;
    code: number;
  }
  declare interface ElementTableFilters {
    text: string;
    value: string;
  }
  declare type ReadonlyRecordable<T = any> = {
    readonly [key: string]: T;
  };
  declare type Indexable<T = any> = {
    [key: string]: T;
  };
  declare type DeepPartial<T> = {
    [P in keyof T]?: DeepPartial<T[P]>;
  };
  declare type TimeoutHandle = ReturnType<typeof setTimeout>;
  declare type IntervalHandle = ReturnType<typeof setInterval>;

  declare interface ChangeEvent extends Event {
    target: HTMLInputElement;
  }

  declare interface WheelEvent {
    path?: EventTarget[];
  }
  declare interface ImportMetaEnv extends ViteEnv {
    __: unknown;
  }

  declare interface ViteEnv {
    VITE_PORT: number;
    VITE_PUBLIC_PATH: string;
    VITE_PROXY_DOMAIN: string;
    VITE_PROXY_DOMAIN_REAL: string;
    VITE_ROUTER_HISTORY: string;
    VITE_LEGACY: boolean;
    VITE_APP_BASE_URL?: string;
    VITE_APP_JIRA_URL?: string;
  }

  declare function parseInt(s: string | number, radix?: number): number;

  declare function parseFloat(string: string | number): number;

  namespace JSX {
    // tslint:disable no-empty-interface
    type Element = import('vue').VNode;
    // tslint:disable no-empty-interface
    type ElementClass = import('vue').ComponentRenderProxy;
    interface ElementAttributesProperty {
      $props: any;
    }
    interface IntrinsicElements {
      [elem: string]: any;
    }
    interface IntrinsicAttributes {
      [elem: string]: any;
    }
  }
}

declare module '*.vue' {
  import type { DefineComponent } from 'vue';
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/ban-types
  const component: DefineComponent<{}, {}, any>;
  export default component;
}

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
