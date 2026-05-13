/// <reference types="vite/client" />

interface ImportMetaEnv {
  // vite 开发服务端口
  readonly VITE_SERVICE_PORT?: number;
  // explorer 服务地址
  readonly VITE_APP_EXPLORER_API?: string;
  // taosx 服务地址
  readonly VITE_APP_X_API?: string;
 
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
