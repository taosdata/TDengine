class PathDetector {
  constructor() {
    this._basePath = null;
  }

  // 自动检测基础路径
  detectBasePath() {
    if (this._basePath) return this._basePath;

    // 方法1: 从当前URL路径解析
    const pathname = window.location.pathname;
    const vueAppPattern = /(\/.+?\/)(?=index\.html|$)/;
    const match = pathname.match(vueAppPattern);

    if (match && match[1]) {
      this._basePath = match[1];
    } else {
      // 方法2: 从script标签检测
      const scripts = document.getElementsByTagName('script');
      for (const script of scripts) {
        if (script.src.includes('index-')) {
          const srcPath = script.src.split('/').slice(0, -2).join('/');
          const basePath = new URL(srcPath).pathname;
          this._basePath = basePath.endsWith('/') ? basePath : basePath + '/';
          break;
        }
      }
    }

    // 方法3: 默认使用根路径
    this._basePath = this._basePath || '/';

    // 确保以斜杠开头和结尾
    if (!this._basePath.startsWith('/')) this._basePath = '/' + this._basePath;
    if (!this._basePath.endsWith('/')) this._basePath += '/';

    return this._basePath;
  }

  // 获取API基础路径
  getApiBasePath() {
    const basePath = this.detectBasePath();
    if (process.env.NODE_ENV === 'development') {
      return import.meta.env.VITE_APP_EXPLORER_API;
    }
    return basePath === '/' ? '/api/-' : `${basePath}api/-`;
  }
  getXApiBasePath() {
    const basePath = this.detectBasePath();
    if (process.env.NODE_ENV === 'development') {
      return import.meta.env.VITE_APP_X_API;
    }
    return basePath === '/' ? '/api/x' : `${basePath}api/x`;
  }

  // 为路径添加基础路径
  withBasePath(path) {
    const basePath = this.detectBasePath();
    const normalizedPath = path.startsWith('/') ? path.slice(1) : path;
    return `${basePath}${normalizedPath}`.replace(/\/\//g, '/');
  }

  // 获取相对路径（移除基础路径）
  getRelativePath(fullPath) {
    const basePath = this.detectBasePath();
    if (basePath === '/') return fullPath;

    return fullPath.replace(new RegExp(`^${basePath}`), '/');
  }
}

export default new PathDetector();
