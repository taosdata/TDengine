from __future__ import annotations
import threading
import os


class SqlRecorder:
    """
    默认开启的 SQL 录制器。

    每次 crash_gen 运行自动在 <logDir>/sql_records/ 目录下创建一个新文件
    crash_gen_<NNN>.sql，NNN 按已有文件编号递增（已存在 000 则生成 001，以此类推）。

    存储路径优先级：
      1. TdeInstance.getLogDir()/sql_records/   （TDengine 客户端日志同级目录）
      2. ./sql_records/                          （兜底，仅在 TdeInstance 未初始化时使用）

    文件格式：每行一条 SQL，末尾以 ; 结尾，无任何其他输出。
    可直接用 taos -f crash_gen_<NNN>.sql 回放。
    """
    _lock      = threading.Lock()
    _file      = None   # type: ignore
    _enabled   = False
    _filepath  = None   # type: ignore
    _records_dir = None # type: ignore  # 延迟确定，首次写入时决定

    SUBDIR = "sql_records"

    # ------------------------------------------------------------------ #
    #  内部工具                                                            #
    # ------------------------------------------------------------------ #

    @classmethod
    def _resolve_records_dir(cls) -> str:
        """
        返回 sql_records 目录的绝对路径。
        优先取 TdeInstance 的 logDir，若未就绪则用当前目录。
        """
        try:
            # 延迟导入，避免循环依赖
            # 使用相对导入，因为 sql_recorder 在 crash_gen 包内
            import crash_gen.crash_gen_main as _cgm  # type: ignore
            log_dir = str(_cgm.gContainer.defTdeInstance.getLogDir())
            return os.path.join(log_dir, cls.SUBDIR)
        except Exception:
            pass
        # 兜底：相对于脚本执行目录
        return os.path.join(os.getcwd(), cls.SUBDIR)

    @classmethod
    def _next_id(cls, records_dir: str) -> int:
        """
        扫描 records_dir，返回下一个可用的递增编号。
        已有 crash_gen_000.sql ~ crash_gen_005.sql → 返回 6。
        """
        if not os.path.isdir(records_dir):
            os.makedirs(records_dir, exist_ok=True)
        max_id = 0
        for name in os.listdir(records_dir):
            if name.startswith("crash_gen_") and name.endswith(".sql"):
                try:
                    num = int(name[len("crash_gen_"):-len(".sql")])
                    if num >= max_id:
                        max_id = num + 1
                except ValueError:
                    pass
        return max_id

    @classmethod
    def _ensure_open(cls):
        """
        首次写入时延迟打开文件（此时 TdeInstance 已就绪）。
        调用方需持有 _lock。
        """
        if cls._file is not None:
            return  # 已打开
        if not cls._enabled:
            return  # 已被禁用

        records_dir  = cls._resolve_records_dir()
        run_id       = cls._next_id(records_dir)
        cls._filepath = os.path.join(records_dir, "crash_gen_{:03d}.sql".format(run_id))
        cls._file    = open(cls._filepath, 'w', buffering=1)  # 行缓冲
        # 只打印到 stderr，不污染 SQL 文件
        import sys
        print("SqlRecorder: recording SQL to {}".format(cls._filepath), file=sys.stderr)

    # ------------------------------------------------------------------ #
    #  公开接口                                                            #
    # ------------------------------------------------------------------ #

    @classmethod
    def init(cls):
        """
        在 crash_gen 启动时调用（Config.init() 之后即可）。
        仅标记"已启用"，真正打开文件延迟到首条 SQL 写入时。
        """
        with cls._lock:
            if cls._enabled:
                return  # 防止重复初始化
            cls._enabled = True

    @classmethod
    def record(cls, sql: str):
        """
        记录一条 SQL。每行末尾保证有 ;，无其他内容。
        """
        if not cls._enabled:
            return
        s = sql.strip()
        if not s:
            return
        if not s.endswith(';'):
            s += ';'
        with cls._lock:
            cls._ensure_open()
            if cls._file is not None:
                cls._file.write(s + '\n')

    @classmethod
    def close(cls):
        """
        crash_gen 结束时调用，关闭文件并打印路径。
        """
        import sys
        with cls._lock:
            if cls._file is not None:
                cls._file.close()
                print("SqlRecorder: saved to {}".format(cls._filepath), file=sys.stderr)
                cls._file = None
            cls._enabled = False
