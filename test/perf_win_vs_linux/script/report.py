#!/usr/bin/env python3
"""Render a Markdown performance report from a main.py result JSON."""

import os
import sys
import json
import datetime


def _f(v, unit='', nd=2, dash='—'):
    if v is None or v == '':
        return dash
    try:
        v = float(v)
    except (TypeError, ValueError):
        return str(v)
    if nd == 0:
        s = '{:,.0f}'.format(v)
    else:
        s = '{:,.{nd}f}'.format(v, nd=nd)
    return s + unit


def _write_table(rows, header):
    out = ['| ' + ' | '.join(header) + ' |',
           '|' + '|'.join(['---'] * len(header)) + '|']
    for r in rows:
        out.append('| ' + ' | '.join(str(c) for c in r) + ' |')
    return out


def build_md(res, compare=None):
    L = []
    A = L.append
    plat = res.get('platform', '?')
    A('# TDengine TSDB 性能测试报告 — %s' % plat)
    A('')
    A('> 生成时间：%s  ' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    A('> 测试窗口：%s → %s  ' % (res.get('start', '?'), res.get('end', '?')))
    A('> 采集脚本：`script/main.py`')
    A('')

    # ---------------- 1. 结论
    A('## 1. 结论摘要')
    A('')
    w = res.get('writes', [])
    q = res.get('queries', [])
    if w:
        best = max(w, key=lambda x: (x.get('insert', {}) or {}).get('real_rows_per_sec') or 0)
        ins = best.get('insert', {})
        A('- 写入峰值：**%s**（%s / %s 轮），real 口径 **%s rows/s**，耗时 %s s。' % (
            best.get('label'), best.get('mode'), best.get('round'),
            _f(ins.get('real_rows_per_sec'), nd=0), _f(ins.get('real_seconds'), nd=1)))
        cold = [x for x in w if x.get('round') == 'cold']
        hot = [x for x in w if x.get('round') == 'hot']
        if cold and hot:
            c = sum((x.get('insert', {}) or {}).get('real_rows_per_sec') or 0 for x in cold) / len(cold)
            h = sum((x.get('insert', {}) or {}).get('real_rows_per_sec') or 0 for x in hot) / len(hot)
            if c:
                A('- 冷/热缓存写入差异：冷 %s / 热 %s rows/s（%+.1f%%）。' % (
                    _f(c, nd=0), _f(h, nd=0), (h - c) / c * 100))
    else:
        A('- 本次未执行写入阶段（`--only queries`）。')
    if q:
        fastest = min(q, key=lambda x: x.get('wall') or 0)
        slowest = max(q, key=lambda x: x.get('wall') or 0)
        A('- 查询共 %d 项：最快 %s（%s s），最慢 %s（%s s）。' % (
            len(q), fastest.get('label'), _f(fastest.get('wall'), nd=1),
            slowest.get('label'), _f(slowest.get('wall'), nd=1)))
        A('- 逐项指标见第 4 节，资源占用见第 5 节，口径见第 6 节。')
    else:
        A('- 本次未执行查询阶段（`--only writes`）。')
    A('')

    # ---------------- 2. 环境
    A('## 2. 测试环境')
    A('')
    env = [
        ('操作系统', res.get('os')),
        ('内核/构建', res.get('kernel')),
        ('CPU', '%s（%s 逻辑核）' % (res.get('cpu'), res.get('ncpu'))),
        ('内存', _f(res.get('mem_gb'), ' GB', 1)),
        ('数据盘', res.get('disk')),
        ('TDengine 版本', res.get('version')),
        ('taosBenchmark', res.get('bench_version')),
        ('安装包', res.get('package') or '（未安装，复用已装版本）'),
        ('数据目录', res.get('data_dir')),
        ('CPU 调频策略', res.get('governor')),
        ('开机时长', res.get('uptime')),
    ]
    L += _write_table([(k, v if v not in (None, '') else '—') for k, v in env],
                      ['项目', '取值'])
    A('')

    # ---------------- 3. 写入
    A('## 3. 写入性能')
    A('')
    A('写入共 4 轮：W1（拼接 SQL，`taosc`）与 W2（参数绑定，`stmt2`）各跑冷启动与热缓存。'
      '每轮 DROP + CREATE 数据库后写入 10 亿行（10000 子表 × 100000 行，8 线程，每请求 5000 行）。')
    A('')
    rows = []
    for x in w:
        ins = x.get('insert', {}) or {}
        r = x.get('res', {}) or {}
        rows.append([
            x.get('label'),
            x.get('mode'),
            x.get('round'),
            _f(ins.get('real_rows_per_sec'), nd=0),
            _f(ins.get('real_seconds'), nd=1),
            _f(ins.get('delay_p90_ms'), ' ms'),
            _f(ins.get('delay_p99_ms'), ' ms'),
            _f(r.get('taosd_cpu_cores'), ' 核'),
            _f(r.get('client_cpu_cores'), ' 核'),
            _f(r.get('rss_avg_mb'), ' MB', 0),
            _f(r.get('disk_mb_s'), ' MB/s', 1),
        ])
    L += _write_table(rows, ['轮次', '写入方式', '缓存', '吞吐 (rows/s)', 'wall (s)',
                             'p90', 'p99', 'taosd CPU', '客户端 CPU', 'taosd RSS', '磁盘写'])
    A('')

    # ---------------- 4. 查询
    A('## 4. 查询性能')
    A('')
    A('所有查询在最后一份写入数据上执行（单一热数据集），客户端与服务端同机，native 连接。')
    A('')
    rows = []
    for x in q:
        p = x.get('parse', {}) or {}
        sqls = p.get('sqls') or []
        tot = p.get('total') or {}
        s0 = sqls[0] if sqls else {}
        # per-execution time = total spend / query_times
        times = x.get('times') or 1
        per = (s0.get('spend_s') / times) if s0.get('spend_s') else None
        rows.append([
            x.get('label'),
            '`%s`' % (x.get('sql') or s0.get('sql') or ''),
            x.get('threads'),
            times,
            _f(per * 1000 if per else None, ' ms', 2),
            _f(tot.get('qps') or s0.get('qps'), nd=1),
            _f(s0.get('delay_p90_s') and s0['delay_p90_s'] * 1000, ' ms', 2),
            _f(s0.get('delay_p99_s') and s0['delay_p99_s'] * 1000, ' ms', 2),
            _f(x.get('wall'), ' s', 2),
        ])
    L += _write_table(rows, ['编号', 'SQL', '线程', '执行次数', '单次均值', 'QPS',
                             'p90', 'p99', '进程总耗时'])
    A('')

    # ---------------- 5. 资源
    A('## 5. 资源占用')
    A('')
    rows = []
    for x in w + q:
        r = x.get('res', {}) or {}
        rows.append([
            x.get('label'),
            _f(r.get('taosd_cpu_cores'), ' 核'),
            _f(r.get('client_cpu_cores'), ' 核'),
            _f(r.get('rss_avg_mb'), ' MB', 0),
            _f(r.get('rss_max_mb'), ' MB', 0),
            _f(r.get('threads_avg'), nd=0),
            _f(r.get('disk_iops'), nd=0),
            _f(r.get('disk_mb_s'), ' MB/s', 1),
            _f(r.get('samples'), nd=0),
        ])
    L += _write_table(rows, ['轮次', 'taosd CPU', '客户端 CPU', 'RSS 均值', 'RSS 峰值',
                             '线程数', '磁盘 IOPS', '磁盘吞吐', '采样点'])
    A('')

    # ---------------- 6. 口径
    A('## 6. 口径与偏差说明')
    A('')
    A('- **冷启动**：重启 taosd 并清空页缓存（Linux 写入 `/proc/sys/vm/drop_caches`；'
      'Windows 无等价机制，仅重启服务，页缓存由系统自行回收）。')
    A('- **热缓存**：不做任何清理，紧接冷启动轮次执行，此时上一轮刚写入的数据仍在页缓存中。')
    A('- **查询**：仅在最后一份热数据上执行一次，不区分冷热（按需求"只要一份热数据"）。')
    A('- **吞吐**：同时给出 bench 自报口径与 real 口径，real 为端到端真实耗时换算。')
    A('- **CPU**：Linux 取 `/proc/<pid>/stat` 前后差值除以墙钟；Windows 取 typeperf '
      '`Process\\% Processor Time` 均值（单核为 100%，可超 100%）换算为核数。两者均 2 s 采样。')
    A('- **磁盘**：Linux 取 `/proc/diskstats` 整盘计数（含所有进程）；Windows 取承载数据目录的 '
      '`PhysicalDisk` 实例（如 `0 C: F:`），口径基本一致。')
    A('- **注意**：Windows 无官方页缓存丢弃接口，冷启动轮次的"冷"程度弱于 Linux，'
      '读取侧指标尤其应结合热缓存轮次一并解读。')
    A('')
    if compare:
        A('## 7. 与对照平台的对比')
        A('')
        A('对照平台：**%s**（TDengine %s，%s）。比值 = 本平台 / 对照平台，>1 表示本平台更好。'
          % (compare.get('platform'), compare.get('version'), compare.get('os')))
        A('')

        cur_w = {x.get('label'): x for x in (res.get('writes') or [])}
        oth_w = {x.get('label'): x for x in (compare.get('writes') or [])}
        rows = []
        for label in [k for k in cur_w if k in oth_w]:
            a = (cur_w[label].get('insert') or {}).get('real_rows_per_sec')
            b = (oth_w[label].get('insert') or {}).get('real_rows_per_sec')
            rows.append([label, _f(a, nd=0), _f(b, nd=0),
                         ('%.2f×' % (a / b)) if a and b else '—'])
        if rows:
            A('### 7.1 写入吞吐（real 口径，rows/s）')
            A('')
            H = ['轮次', res.get('platform'), compare.get('platform'), '比值']
            L += _write_table(rows, H)
            A('')

        cur_q = {x.get('label'): x for x in (res.get('queries') or [])}
        oth_q = {x.get('label'): x for x in (compare.get('queries') or [])}
        rows = []
        for label in [k for k in cur_q if k in oth_q]:
            a = _per_exec(cur_q[label])
            b = _per_exec(oth_q[label])
            rows.append([
                label,
                '`%s`' % (cur_q[label].get('sql') or ''),
                _f(a, ' ms') if a else '—',
                _f(b, ' ms') if b else '—',
                ('%.2f×' % (b / a)) if a and b else '—',
            ])
        if rows:
            A('### 7.2 查询单次均值')
            A('')
            H = ['编号', 'SQL', res.get('platform'), compare.get('platform'), '快慢比']
            L += _write_table(rows, H)
            A('')
            A('> 「快慢比」= 对照平台耗时 / 本平台耗时，>1 表示本平台更快。')
            A('')
        if not rows and not cur_w:
            A('（无可对比项）')
            A('')
    return '\n'.join(L) + '\n'


def _per_exec(entry):
    """Wall time of one execution round, in milliseconds."""
    p = entry.get('parse') or {}
    sqls = p.get('sqls') or []
    s0 = sqls[0] if sqls else {}
    if not s0.get('spend_s'):
        return None
    times = entry.get('times') or 1
    return s0['spend_s'] / times * 1000.0


def save(res, out_path, compare=None):
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(build_md(res, compare))
    return out_path


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('usage: report.py <result.json> <out.md> [baseline.json]')
        sys.exit(2)
    res = json.load(open(sys.argv[1], encoding='utf-8'))
    base = json.load(open(sys.argv[3], encoding='utf-8')) if len(sys.argv) > 3 else None
    print(save(res, sys.argv[2], base))
