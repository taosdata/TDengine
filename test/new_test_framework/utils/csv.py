# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import csv
import os
import platform

class TDCsv:
    def __init__(self):
        self.__file_name = ''
        self.__file_path = ''

    @property
    def file_name(self):
        return self.__file_name
    
    @file_name.setter
    def file_name(self, value):
        self.__file_name = value

    @property
    def file_path(self):
        return self.__file_path
    
    @file_path.setter
    def file_path(self, value):
        self.__file_path = value
    
    @property
    def file(self):
        if self.file_name and self.file_path:
            print(f"self.file_path {self.file_path}, self.file_name {self.file_name}")
            csv_file = os.path.join(self.file_path, self.file_name)
            if platform.system().lower() == 'windows':
                csv_file = csv_file.replace("\\", "/")                         
            return csv_file
        return None
    
    
    def read(self):
        try:
            with open(self.file, newline='') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    print(row)
        except Exception as errMsg:
            raise Exception(errMsg)

    def __write_with_quotes(self, csv_writer, data):
        for row in data:
            row_with_quotes = [f'"{field}"' if isinstance(field, str) else field for field in row]
            csv_writer.writerow(row_with_quotes)

    def write(self, data: dict):
        try:
            with open(self.file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(data)
                # self.__write_with_quotes(writer, data)
        except Exception as errMsg:
            raise Exception(errMsg)
    
    def delete(self):
        try:
            if os.path.exists(self.file):
                os.remove(self.file)
        except Exception as errMsg:
            raise Exception(errMsg)
       

