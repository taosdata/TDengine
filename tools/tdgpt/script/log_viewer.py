#!/usr/bin/env python3
"""
TDGPT Log Viewer - 图形化日志查看工具
提供实时日志查看、搜索、过滤等功能
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
from pathlib import Path
import threading
import time
from datetime import datetime

class LogViewer:
    def __init__(self, root):
        self.root = root
        self.root.title("TDGPT 日志查看器")
        self.root.geometry("1200x800")

        # 默认日志目录
        self.log_dir = Path(r"C:\TDengine\taosanode\log")
        self.current_file = None
        self.auto_refresh = False
        self.refresh_thread = None

        self.setup_ui()
        self.load_log_files()

    def setup_ui(self):
        """设置用户界面"""
        # 顶部工具栏
        toolbar = ttk.Frame(self.root)
        toolbar.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        # 日志文件选择
        ttk.Label(toolbar, text="日志文件:").pack(side=tk.LEFT, padx=5)
        self.file_combo = ttk.Combobox(toolbar, width=40, state="readonly")
        self.file_combo.pack(side=tk.LEFT, padx=5)
        self.file_combo.bind("<<ComboboxSelected>>", self.on_file_selected)

        # 刷新按钮
        ttk.Button(toolbar, text="刷新", command=self.refresh_log).pack(side=tk.LEFT, padx=5)

        # 自动刷新复选框
        self.auto_refresh_var = tk.BooleanVar()
        ttk.Checkbutton(toolbar, text="自动刷新", variable=self.auto_refresh_var,
                       command=self.toggle_auto_refresh).pack(side=tk.LEFT, padx=5)

        # 清空按钮
        ttk.Button(toolbar, text="清空显示", command=self.clear_display).pack(side=tk.LEFT, padx=5)

        # 打开目录按钮
        ttk.Button(toolbar, text="打开日志目录", command=self.open_log_dir).pack(side=tk.LEFT, padx=5)

        # 搜索栏
        search_frame = ttk.Frame(self.root)
        search_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        ttk.Label(search_frame, text="搜索:").pack(side=tk.LEFT, padx=5)
        self.search_entry = ttk.Entry(search_frame, width=30)
        self.search_entry.pack(side=tk.LEFT, padx=5)
        self.search_entry.bind("<Return>", lambda e: self.search_log())

        ttk.Button(search_frame, text="查找", command=self.search_log).pack(side=tk.LEFT, padx=5)
        ttk.Button(search_frame, text="清除高亮", command=self.clear_highlight).pack(side=tk.LEFT, padx=5)

        # 过滤器
        ttk.Label(search_frame, text="级别:").pack(side=tk.LEFT, padx=5)
        self.level_var = tk.StringVar(value="ALL")
        level_combo = ttk.Combobox(search_frame, textvariable=self.level_var,
                                   values=["ALL", "ERROR", "WARNING", "INFO", "DEBUG"],
                                   width=10, state="readonly")
        level_combo.pack(side=tk.LEFT, padx=5)
        level_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filter())

        # 日志显示区域
        text_frame = ttk.Frame(self.root)
        text_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.log_text = scrolledtext.ScrolledText(text_frame, wrap=tk.WORD,
                                                  font=("Consolas", 9))
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # 配置标签颜色
        self.log_text.tag_config("ERROR", foreground="red")
        self.log_text.tag_config("WARNING", foreground="orange")
        self.log_text.tag_config("INFO", foreground="green")
        self.log_text.tag_config("DEBUG", foreground="gray")
        self.log_text.tag_config("highlight", background="yellow")

        # 状态栏
        self.status_bar = ttk.Label(self.root, text="就绪", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def load_log_files(self):
        """加载日志文件列表"""
        if not self.log_dir.exists():
            messagebox.showwarning("警告", f"日志目录不存在: {self.log_dir}")
            return

        log_files = []
        for file in self.log_dir.glob("*.log"):
            log_files.append(file.name)

        log_files.sort(reverse=True)  # 最新的在前
        self.file_combo['values'] = log_files

        if log_files:
            self.file_combo.current(0)
            self.on_file_selected(None)

    def on_file_selected(self, event):
        """文件选择事件"""
        filename = self.file_combo.get()
        if filename:
            self.current_file = self.log_dir / filename
            self.load_log_content()

    def load_log_content(self):
        """加载日志内容"""
        if not self.current_file or not self.current_file.exists():
            return

        try:
            self.log_text.delete(1.0, tk.END)

            with open(self.current_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            # 按行处理，添加颜色标签
            for line in content.split('\n'):
                if 'ERROR' in line:
                    self.log_text.insert(tk.END, line + '\n', "ERROR")
                elif 'WARNING' in line:
                    self.log_text.insert(tk.END, line + '\n', "WARNING")
                elif 'INFO' in line:
                    self.log_text.insert(tk.END, line + '\n', "INFO")
                elif 'DEBUG' in line:
                    self.log_text.insert(tk.END, line + '\n', "DEBUG")
                else:
                    self.log_text.insert(tk.END, line + '\n')

            # 滚动到底部
            self.log_text.see(tk.END)

            # 更新状态栏
            file_size = self.current_file.stat().st_size
            line_count = content.count('\n')
            self.status_bar.config(text=f"文件: {self.current_file.name} | 大小: {file_size:,} 字节 | 行数: {line_count:,}")

        except Exception as e:
            messagebox.showerror("错误", f"读取日志文件失败: {e}")

    def refresh_log(self):
        """刷新日志"""
        self.load_log_content()

    def toggle_auto_refresh(self):
        """切换自动刷新"""
        self.auto_refresh = self.auto_refresh_var.get()

        if self.auto_refresh:
            self.start_auto_refresh()
        else:
            self.stop_auto_refresh()

    def start_auto_refresh(self):
        """启动自动刷新"""
        if self.refresh_thread and self.refresh_thread.is_alive():
            return

        self.refresh_thread = threading.Thread(target=self.auto_refresh_worker, daemon=True)
        self.refresh_thread.start()

    def stop_auto_refresh(self):
        """停止自动刷新"""
        self.auto_refresh = False

    def auto_refresh_worker(self):
        """自动刷新工作线程"""
        while self.auto_refresh:
            self.root.after(0, self.refresh_log)
            time.sleep(2)  # 每2秒刷新一次

    def search_log(self):
        """搜索日志"""
        search_text = self.search_entry.get()
        if not search_text:
            return

        # 清除之前的高亮
        self.log_text.tag_remove("highlight", 1.0, tk.END)

        # 搜索并高亮
        start_pos = "1.0"
        count = 0
        while True:
            start_pos = self.log_text.search(search_text, start_pos, tk.END, nocase=True)
            if not start_pos:
                break

            end_pos = f"{start_pos}+{len(search_text)}c"
            self.log_text.tag_add("highlight", start_pos, end_pos)
            start_pos = end_pos
            count += 1

        if count > 0:
            self.status_bar.config(text=f"找到 {count} 个匹配项")
            # 滚动到第一个匹配项
            self.log_text.see("1.0")
            self.log_text.see(self.log_text.tag_ranges("highlight")[0])
        else:
            self.status_bar.config(text="未找到匹配项")
            messagebox.showinfo("搜索", "未找到匹配项")

    def clear_highlight(self):
        """清除高亮"""
        self.log_text.tag_remove("highlight", 1.0, tk.END)
        self.status_bar.config(text="已清除高亮")

    def apply_filter(self):
        """应用级别过滤"""
        level = self.level_var.get()
        if level == "ALL":
            self.load_log_content()
            return

        if not self.current_file or not self.current_file.exists():
            return

        try:
            self.log_text.delete(1.0, tk.END)

            with open(self.current_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if level in line:
                        if 'ERROR' in line:
                            self.log_text.insert(tk.END, line, "ERROR")
                        elif 'WARNING' in line:
                            self.log_text.insert(tk.END, line, "WARNING")
                        elif 'INFO' in line:
                            self.log_text.insert(tk.END, line, "INFO")
                        elif 'DEBUG' in line:
                            self.log_text.insert(tk.END, line, "DEBUG")
                        else:
                            self.log_text.insert(tk.END, line)

            self.status_bar.config(text=f"已过滤: {level}")

        except Exception as e:
            messagebox.showerror("错误", f"过滤日志失败: {e}")

    def clear_display(self):
        """清空显示"""
        self.log_text.delete(1.0, tk.END)
        self.status_bar.config(text="已清空显示")

    def open_log_dir(self):
        """打开日志目录"""
        if self.log_dir.exists():
            os.startfile(self.log_dir)
        else:
            messagebox.showwarning("警告", f"日志目录不存在: {self.log_dir}")


def main():
    root = tk.Tk()
    app = LogViewer(root)
    root.mainloop()


if __name__ == "__main__":
    main()
