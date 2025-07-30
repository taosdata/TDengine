###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import time
import datetime
import logging


class ColorFormatter(logging.Formatter):
    """自定义带颜色的日志格式化器"""
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    green = "\x1b[32;20m"
    blue = "\x1b[34;20m"
    cyan = "\x1b[36;20m"
    reset = "\x1b[0m"
    
    FORMAT = "[%(asctime)s] [%(levelname)s] %(filename)s:%(lineno)d - %(message)s"

    FORMATS = {
        logging.DEBUG: cyan + FORMAT + reset,
        logging.INFO: green + FORMAT + reset,
        logging.WARNING: yellow + FORMAT + reset,
        logging.ERROR: red + FORMAT + reset,
        logging.CRITICAL: bold_red + FORMAT + reset
    }

    COLOR_MAP = {
        'grey': grey, 'yellow': yellow, 'red': red, 'bold_red': bold_red,
        'green': green, 'blue': blue, 'cyan': cyan, 'reset': reset
    }
    
    def format(self, record):
        if hasattr(record, 'color') and record.color in self.COLOR_MAP:
            color = self.COLOR_MAP[record.color]
            log_fmt = color + self.FORMAT + self.reset
        else:
            log_fmt = self.FORMATS.get(record.levelno)
            
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)


class TDLog:
    def __init__(self):
        self.path = ""
        # 创建logger，禁用传播以避免重复日志
        self.logger = logging.getLogger(__name__)
        self.logger.propagate = False  # 禁止传播到根logger
        #self.logger.setLevel(logging.INFO)

        # 创建控制台handler并设置自定义格式
        ch = logging.StreamHandler()
        ch.setFormatter(ColorFormatter())
        self.logger.addHandler(ch)

    def _log(self, level, msg, color=None, *args, **kwargs):
        # 首先检查日志级别是否应该被记录
        if not self.logger.isEnabledFor(level):
            return
        # 获取调用栈信息
        import inspect
        frame = inspect.currentframe()
        # 向上追溯两层：一层是当前方法，一层是实际的日志方法（info/debug等）
        for _ in range(2):
            frame = frame.f_back
        # 获取调用位置信息
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno
        
        # 创建LogRecord时传入调用位置信息
        self.logger.makeRecord(
            self.logger.name,
            level,
            filename,
            lineno,
            msg,
            args,
            None,
            None,
            **kwargs
        )._info = {  # 添加额外信息
            'filename': filename,
            'lineno': lineno
        }
        
        record = self.logger.makeRecord(
                self.logger.name,
                level,
                filename,
                lineno,
                msg,
                args,
                None,
                None,
                **kwargs
            )
        if color:
            record.color = color
            
        self.logger.handle(record)

    def info(self, info, *args, color=None, **kwargs):
        self._log(logging.INFO, info, color, *args, **kwargs)

    def sleep(self, sec):
        self._log(logging.INFO, f"sleep {sec} seconds")
        time.sleep(sec)

    def debug(self, err, *args, color=None, **kwargs):
        self._log(logging.DEBUG, err, color, *args, **kwargs)

    def success(self, info, *args, **kwargs):
        self._log(logging.INFO, info, *args, **kwargs)

    def notice(self, err, *args, **kwargs):
        self._log(logging.INFO, err, *args, **kwargs)

    def exit(self, err, *args, **kwargs):
        self._log(logging.ERROR, err, *args, **kwargs)
        sys.exit(1)

    def error(self, err, *args, color=None, **kwargs):
        self._log(logging.ERROR, err, color, *args, **kwargs)

    def printNoPrefix(self, info, *args, **kwargs):
        self._log(logging.INFO, info, *args, **kwargs)


tdLog = TDLog()
