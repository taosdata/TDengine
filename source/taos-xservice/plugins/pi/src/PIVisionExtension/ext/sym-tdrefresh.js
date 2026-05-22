/**
 * TDengine CDR Auto-Refresh Extension for PI Vision
 *
 * 解决 PI Vision 对 CDR（Custom Data Reference）数据源硬编码 120 秒轮询的问题。
 * 通过拦截 DiffForData XHR 请求，修改 EndTime 使每次请求唯一，绕过后端缓存，
 * 实现与原生 PI Point 相同的 5 秒刷新体验。
 *
 * 部署方式：将此文件复制到 PI Vision 安装目录的
 *   Scripts/app/editor/symbols/ext/ 文件夹中，重启 IIS 即可生效。
 *
 * 原理：PI Vision 后端 DataService.dll 中 PIDataQuery.DefaultFutureUpdatingExtraTime = 120
 *       导致 CDR 的 GetValue/PlotValues 每 120 秒才被调用一次。
 *       本扩展通过使每次 DiffForData 请求的 EndTime 不同来绕过此缓存。
 *
 * @version 1.0.0
 * @license Apache-2.0
 */
(function (PV) {
    'use strict';

    // ==================== 配置 ====================

    // 步进计数器达到此值后重置为 1（防止偏移量无限增长）
    var MAX_CYCLES = 300;

    // ==================== 状态 ====================

    var currentStep = 1;
    var installed = false;

    // ==================== 工具函数 ====================

    function isDiffForDataUrl(url) {
        return url && typeof url === 'string' && url.indexOf('DiffForData') !== -1;
    }

    // 将 PI 时间表达式转换为秒数
    // 支持格式: "-1h", "-30m", "-1d", "-1w", "-1mo", "-8h", "*-1h" 等
    // 返回秒数（正数），如 "-1h" → 3600
    function parseTimeRangeToSeconds(startTime) {
        if (typeof startTime === 'string') {
            // 先尝试匹配 "mo"（月），再匹配单字符单位
            var moMatch = startTime.match(/(\d+)\s*mo/i);
            if (moMatch) {
                return parseInt(moMatch[1]) * 2592000; // 30 天
            }
            var match = startTime.match(/(\d+)\s*([whdms])/i);
            if (match) {
                var value = parseInt(match[1]);
                var unit = match[2].toLowerCase();
                if (unit === 'w') return value * 604800;
                if (unit === 'h') return value * 3600;
                if (unit === 'd') return value * 86400;
                if (unit === 'm') return value * 60;
                if (unit === 's') return value;
            }
        }
        return 3600; // 默认 1h
    }

    // ==================== XHR 拦截 ====================

    function installXhrInterceptor() {
        if (installed) return;
        installed = true;
        // interceptor installed silently

        var origOpen = XMLHttpRequest.prototype.open;
        var origSend = XMLHttpRequest.prototype.send;

        // --- 拦截 open：标记 DiffForData 请求 ---
        XMLHttpRequest.prototype.open = function (method, url) {
            this._tdUrl = url;
            return origOpen.apply(this, arguments);
        };

        // --- 拦截 send：修改 DiffForData 请求参数 ---
        XMLHttpRequest.prototype.send = function (body) {
            if (!isDiffForDataUrl(this._tdUrl)) {
                return origSend.apply(this, arguments);
            }

            try {
                var p = JSON.parse(body);

                // per-request 实时/历史判定：EndTime 以 "*" 开头 = 实时模式
                var endTime = p.EndTime;
                if (typeof endTime !== 'string' || endTime.charAt(0) !== '*') {
                    // 历史模式，不干预
                    return origSend.apply(this, arguments);
                }

                // 实时模式：将原始时间范围转换为秒，每次请求加递增偏移使其唯一
                // 例如 "-1h" → 3600s → 发送 "-3601s", "-3602s", ...
                // 这样既绕过后端缓存，又保持时间范围与用户选择一致
                var baseSec = parseTimeRangeToSeconds(p.StartTime);
                var perturbedSec = baseSec + currentStep;
                p.StartTime = '-' + perturbedSec + 's';
                p.EndTime = '*+' + currentStep + 's';
                p.ForceUpdate = true;
                currentStep = (currentStep >= MAX_CYCLES) ? 1 : currentStep + 1;
                return origSend.apply(this, [JSON.stringify(p)]);
            } catch (e) {
                return origSend.apply(this, arguments);
            }
        };
    }

    // ==================== 初始化 ====================

    installXhrInterceptor();

    // ==================== PI Vision 自定义 Symbol 注册 ====================
    // 注册一个不可见的空 Symbol，确保 PI Vision 的模块加载器不报错。
    // 该 Symbol 无实际功能，仅作为扩展载体。

    if (!PV) return;

    function symbolVis() { }

    if (PV.deriveVisualizationFromBase) {
        PV.deriveVisualizationFromBase(symbolVis);
    }

    var definition = {
        typeName: 'tdrefresh',
        displayName: 'TDengine Auto Refresh',
        datasourceBehavior: PV.Extensibility ? PV.Extensibility.Enums.DatasourceBehaviors.None : 0,
        visObjectType: symbolVis,
        getDefaultConfig: function () {
            return {
                Height: 0,
                Width: 0
            };
        }
    };

    if (PV.symbolCatalog && PV.symbolCatalog.register) {
        PV.symbolCatalog.register(definition);
    }

})(window.PIVisualization);
