#!/usr/bin/env python3
"""
TDGPT Configuration Wizard - 配置向导
首次安装后引导用户配置关键参数
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
import json
import re

class ConfigWizard:
    def __init__(self, root):
        self.root = root
        self.root.title("TDGPT 配置向导")
        self.root.geometry("800x600")
        self.root.resizable(False, False)

        # 配置文件路径
        self.config_file = Path(r"C:\TDengine\taosanode\cfg\taosanode.config.py")
        self.config_data = {}

        # 当前页面
        self.current_page = 0
        self.pages = []

        self.setup_ui()
        self.load_config()

    def setup_ui(self):
        """设置用户界面"""
        # 标题
        title_frame = ttk.Frame(self.root)
        title_frame.pack(side=tk.TOP, fill=tk.X, padx=20, pady=20)

        title_label = ttk.Label(title_frame, text="TDGPT 配置向导",
                               font=("Arial", 16, "bold"))
        title_label.pack()

        subtitle_label = ttk.Label(title_frame, text="配置 TDGPT 服务的关键参数",
                                  font=("Arial", 10))
        subtitle_label.pack()

        # 内容区域
        self.content_frame = ttk.Frame(self.root)
        self.content_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=20, pady=10)

        # 创建页面
        self.create_pages()

        # 按钮栏
        button_frame = ttk.Frame(self.root)
        button_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=20, pady=20)

        self.prev_button = ttk.Button(button_frame, text="上一步", command=self.prev_page)
        self.prev_button.pack(side=tk.LEFT, padx=5)

        self.next_button = ttk.Button(button_frame, text="下一步", command=self.next_page)
        self.next_button.pack(side=tk.RIGHT, padx=5)

        self.finish_button = ttk.Button(button_frame, text="完成", command=self.finish)
        self.finish_button.pack(side=tk.RIGHT, padx=5)

        # 显示第一页
        self.show_page(0)

    def create_pages(self):
        """创建配置页面"""
        # 页面1: 数据库连接
        page1 = ttk.Frame(self.content_frame)
        self.pages.append(page1)

        ttk.Label(page1, text="数据库连接配置", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        ttk.Label(page1, text="TDengine 主机地址:").pack(anchor=tk.W, pady=5)
        self.db_host = ttk.Entry(page1, width=50)
        self.db_host.pack(anchor=tk.W, pady=5)
        self.db_host.insert(0, "localhost")

        ttk.Label(page1, text="TDengine 端口:").pack(anchor=tk.W, pady=5)
        self.db_port = ttk.Entry(page1, width=50)
        self.db_port.pack(anchor=tk.W, pady=5)
        self.db_port.insert(0, "6030")

        ttk.Label(page1, text="数据库用户名:").pack(anchor=tk.W, pady=5)
        self.db_user = ttk.Entry(page1, width=50)
        self.db_user.pack(anchor=tk.W, pady=5)
        self.db_user.insert(0, "root")

        ttk.Label(page1, text="数据库密码:").pack(anchor=tk.W, pady=5)
        self.db_password = ttk.Entry(page1, width=50, show="*")
        self.db_password.pack(anchor=tk.W, pady=5)
        self.db_password.insert(0, "taosdata")

        # 页面2: 服务配置
        page2 = ttk.Frame(self.content_frame)
        self.pages.append(page2)

        ttk.Label(page2, text="服务配置", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        ttk.Label(page2, text="主服务端口:").pack(anchor=tk.W, pady=5)
        self.main_port = ttk.Entry(page2, width=50)
        self.main_port.pack(anchor=tk.W, pady=5)
        self.main_port.insert(0, "6035")

        ttk.Label(page2, text="日志级别:").pack(anchor=tk.W, pady=5)
        self.log_level = ttk.Combobox(page2, width=48, state="readonly",
                                     values=["DEBUG", "INFO", "WARNING", "ERROR"])
        self.log_level.pack(anchor=tk.W, pady=5)
        self.log_level.current(1)  # 默认 INFO

        ttk.Label(page2, text="工作线程数:").pack(anchor=tk.W, pady=5)
        self.worker_threads = ttk.Spinbox(page2, from_=1, to=16, width=48)
        self.worker_threads.pack(anchor=tk.W, pady=5)
        self.worker_threads.set(4)

        # 页面3: 模型配置
        page3 = ttk.Frame(self.content_frame)
        self.pages.append(page3)

        ttk.Label(page3, text="模型配置", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        ttk.Label(page3, text="启用的模型服务:").pack(anchor=tk.W, pady=5)

        self.model_tdtsfm = tk.BooleanVar(value=True)
        ttk.Checkbutton(page3, text="TDTSFM (必需)", variable=self.model_tdtsfm,
                       state="disabled").pack(anchor=tk.W, pady=2)

        self.model_timemoe = tk.BooleanVar(value=True)
        ttk.Checkbutton(page3, text="TimeMoE (必需)", variable=self.model_timemoe,
                       state="disabled").pack(anchor=tk.W, pady=2)

        self.model_chronos = tk.BooleanVar(value=False)
        ttk.Checkbutton(page3, text="Chronos (可选)", variable=self.model_chronos).pack(anchor=tk.W, pady=2)

        self.model_timesfm = tk.BooleanVar(value=False)
        ttk.Checkbutton(page3, text="TimesFM (可选)", variable=self.model_timesfm).pack(anchor=tk.W, pady=2)

        self.model_moirai = tk.BooleanVar(value=False)
        ttk.Checkbutton(page3, text="Moirai (可选)", variable=self.model_moirai).pack(anchor=tk.W, pady=2)

        self.model_moment = tk.BooleanVar(value=False)
        ttk.Checkbutton(page3, text="MomentFM (可选)", variable=self.model_moment).pack(anchor=tk.W, pady=2)

        # 页面4: 完成
        page4 = ttk.Frame(self.content_frame)
        self.pages.append(page4)

        ttk.Label(page4, text="配置完成", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=10)

        summary_text = tk.Text(page4, height=20, width=70, wrap=tk.WORD)
        summary_text.pack(anchor=tk.W, pady=10)
        self.summary_text = summary_text

    def show_page(self, page_num):
        """显示指定页面"""
        # 隐藏所有页面
        for page in self.pages:
            page.pack_forget()

        # 显示当前页面
        self.current_page = page_num
        self.pages[page_num].pack(fill=tk.BOTH, expand=True)

        # 更新按钮状态
        if page_num == 0:
            self.prev_button.config(state=tk.DISABLED)
        else:
            self.prev_button.config(state=tk.NORMAL)

        if page_num == len(self.pages) - 1:
            self.next_button.config(state=tk.DISABLED)
            self.finish_button.config(state=tk.NORMAL)
            self.update_summary()
        else:
            self.next_button.config(state=tk.NORMAL)
            self.finish_button.config(state=tk.DISABLED)

    def prev_page(self):
        """上一页"""
        if self.current_page > 0:
            self.show_page(self.current_page - 1)

    def next_page(self):
        """下一页"""
        if self.current_page < len(self.pages) - 1:
            # 验证当前页面
            if not self.validate_current_page():
                return
            self.show_page(self.current_page + 1)

    def validate_current_page(self):
        """验证当前页面"""
        if self.current_page == 0:
            # 验证数据库配置
            if not self.db_host.get():
                messagebox.showerror("错误", "请输入数据库主机地址")
                return False
            if not self.db_port.get().isdigit():
                messagebox.showerror("错误", "端口必须是数字")
                return False
            if not self.db_user.get():
                messagebox.showerror("错误", "请输入数据库用户名")
                return False

        elif self.current_page == 1:
            # 验证服务配置
            if not self.main_port.get().isdigit():
                messagebox.showerror("错误", "端口必须是数字")
                return False

        return True

    def update_summary(self):
        """更新配置摘要"""
        self.summary_text.delete(1.0, tk.END)

        summary = "配置摘要:\n\n"
        summary += "=" * 60 + "\n"
        summary += "数据库连接:\n"
        summary += f"  主机: {self.db_host.get()}\n"
        summary += f"  端口: {self.db_port.get()}\n"
        summary += f"  用户: {self.db_user.get()}\n"
        summary += f"  密码: {'*' * len(self.db_password.get())}\n\n"

        summary += "服务配置:\n"
        summary += f"  主服务端口: {self.main_port.get()}\n"
        summary += f"  日志级别: {self.log_level.get()}\n"
        summary += f"  工作线程数: {self.worker_threads.get()}\n\n"

        summary += "启用的模型:\n"
        summary += "  ✓ TDTSFM (必需)\n"
        summary += "  ✓ TimeMoE (必需)\n"
        if self.model_chronos.get():
            summary += "  ✓ Chronos\n"
        if self.model_timesfm.get():
            summary += "  ✓ TimesFM\n"
        if self.model_moirai.get():
            summary += "  ✓ Moirai\n"
        if self.model_moment.get():
            summary += "  ✓ MomentFM\n"

        summary += "\n" + "=" * 60 + "\n"
        summary += "\n点击"完成"按钮保存配置并启动服务。\n"

        self.summary_text.insert(1.0, summary)

    def load_config(self):
        """加载现有配置"""
        if not self.config_file.exists():
            return

        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # 简单解析配置文件
            # 这里只是示例，实际应该更健壮
            if 'tdengine_host' in content:
                match = re.search(r'tdengine_host\s*=\s*["\']([^"\']+)["\']', content)
                if match:
                    self.db_host.delete(0, tk.END)
                    self.db_host.insert(0, match.group(1))

        except Exception as e:
            print(f"加载配置失败: {e}")

    def save_config(self):
        """保存配置"""
        if not self.config_file.exists():
            messagebox.showerror("错误", f"配置文件不存在: {self.config_file}")
            return False

        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # 替换配置值
            content = re.sub(r'tdengine_host\s*=\s*["\'][^"\']*["\']',
                           f'tdengine_host = "{self.db_host.get()}"', content)
            content = re.sub(r'tdengine_port\s*=\s*\d+',
                           f'tdengine_port = {self.db_port.get()}', content)
            content = re.sub(r'tdengine_user\s*=\s*["\'][^"\']*["\']',
                           f'tdengine_user = "{self.db_user.get()}"', content)
            content = re.sub(r'tdengine_password\s*=\s*["\'][^"\']*["\']',
                           f'tdengine_password = "{self.db_password.get()}"', content)
            content = re.sub(r'main_service_port\s*=\s*\d+',
                           f'main_service_port = {self.main_port.get()}', content)
            content = re.sub(r'log_level\s*=\s*["\'][^"\']*["\']',
                           f'log_level = "{self.log_level.get()}"', content)

            # 备份原配置
            backup_file = self.config_file.with_suffix('.config.py.bak')
            with open(backup_file, 'w', encoding='utf-8') as f:
                with open(self.config_file, 'r', encoding='utf-8') as orig:
                    f.write(orig.read())

            # 保存新配置
            with open(self.config_file, 'w', encoding='utf-8') as f:
                f.write(content)

            return True

        except Exception as e:
            messagebox.showerror("错误", f"保存配置失败: {e}")
            return False

    def finish(self):
        """完成配置"""
        if messagebox.askyesno("确认", "确定要保存配置并启动服务吗？"):
            if self.save_config():
                messagebox.showinfo("成功", "配置已保存！\n\n请使用以下命令启动服务：\nC:\\TDengine\\taosanode\\bin\\start-taosanode.bat")
                self.root.destroy()


def main():
    root = tk.Tk()
    app = ConfigWizard(root)
    root.mainloop()


if __name__ == "__main__":
    main()
