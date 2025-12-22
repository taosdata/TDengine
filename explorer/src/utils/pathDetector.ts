import Cookies from 'js-cookie';
import { trimEnd } from 'lodash-es';
class PathDetector {
  _basePath: string | null = null;
  constructor() {
    this._basePath = null;
  }

  /**
   * 安全地从路径末尾移除指定的模式
   * @param {string} path - 原始路径
   * @param {string} pattern - 要移除的模式
   * @returns {string} 修剪后的路径
   */
  static trimPathEnd(path: string, pattern: string) {
    if (!path || !pattern) return path;

    // 确保模式以斜杠结尾，用于精确匹配
    const normalizedPattern = path.endsWith('/')
      ? pattern.endsWith('/')
        ? pattern
        : pattern + '/'
      : pattern.endsWith('/')
        ? trimEnd(pattern, '/')
        : pattern;

    // 检查路径是否以模式结尾
    if (path.endsWith(normalizedPattern)) {
      return path.slice(0, -normalizedPattern.length) || '/';
    }

    // 检查路径是否等于模式（无斜杠）
    if (path === pattern) {
      return '/';
    }

    return path;
  }
  // 自动检测基础路径
  detectBasePath() {
    if (this._basePath) return this._basePath;
    // get from cookie
    const cookieRoute = Cookies.get('route');
    if (cookieRoute) {
      const route = decodeURIComponent(cookieRoute);
      const url = new URL(window.location.href);
      const pathname = url.pathname;
      this._basePath = PathDetector.trimPathEnd(pathname, route);

      if (!this._basePath.endsWith('/')) {
        this._basePath += '/';
      }
      Cookies.remove('route');
      return this._basePath;
    }

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
  withBasePath(path: string) {
    const basePath = this.detectBasePath();
    const normalizedPath = path.startsWith('/') ? path.slice(1) : path;
    return `${basePath}${normalizedPath}`.replace(/\/\//g, '/');
  }

  // 获取相对路径（移除基础路径）
  getRelativePath(fullPath: string) {
    const basePath = this.detectBasePath();
    if (basePath === '/') return fullPath;

    return fullPath.replace(new RegExp(`^${basePath}`), '/');
  }
}

export default new PathDetector();
