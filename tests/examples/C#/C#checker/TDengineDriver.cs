/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace TDengineDriver
{
  enum TDengineDataType {
    TSDB_DATA_TYPE_NULL = 0,     // 1 bytes
    TSDB_DATA_TYPE_BOOL = 1,     // 1 bytes
    TSDB_DATA_TYPE_TINYINT = 2,  // 1 bytes
    TSDB_DATA_TYPE_SMALLINT = 3, // 2 bytes
    TSDB_DATA_TYPE_INT = 4,      // 4 bytes
    TSDB_DATA_TYPE_BIGINT = 5,   // 8 bytes
    TSDB_DATA_TYPE_FLOAT = 6,    // 4 bytes
    TSDB_DATA_TYPE_DOUBLE = 7,   // 8 bytes
    TSDB_DATA_TYPE_BINARY = 8,   // string
    TSDB_DATA_TYPE_TIMESTAMP = 9,// 8 bytes
    TSDB_DATA_TYPE_NCHAR = 10    // unicode string
  }

  enum TDengineInitOption
  {
    TSDB_OPTION_LOCALE = 0,
    TSDB_OPTION_CHARSET = 1,
    TSDB_OPTION_TIMEZONE = 2,
    TDDB_OPTION_CONFIGDIR = 3,
    TDDB_OPTION_SHELL_ACTIVITY_TIMER = 4
  }

  class TDengineMeta
  {
    public string name;
    public short size;
    public byte type;
    public string TypeName()
    {
      switch ((TDengineDataType)type)
      {
        case TDengineDataType.TSDB_DATA_TYPE_BOOL:
          return "BOOLEAN";
        case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
          return "BYTE";
        case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
          return "SHORT";
        case TDengineDataType.TSDB_DATA_TYPE_INT:
          return "INT";
        case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
          return "LONG";
        case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
          return "FLOAT";
        case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
          return "DOUBLE";
        case TDengineDataType.TSDB_DATA_TYPE_BINARY:
          return "STRING";
        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
          return "TIMESTAMP";
        case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
          return "NCHAR";
        default:
          return "undefine";
      }
    }
  }

  class TDengine
  {
    public const int TSDB_CODE_SUCCESS = 0;

    [DllImport("taos.dll", EntryPoint = "taos_init", CallingConvention = CallingConvention.Cdecl)]
    static extern public void Init();

    [DllImport("taos.dll", EntryPoint = "taos_cleanup", CallingConvention = CallingConvention.Cdecl)]
    static extern public void Cleanup();

    [DllImport("taos.dll", EntryPoint = "taos_options", CallingConvention = CallingConvention.Cdecl)]
    static extern public void Options(int option, string value);

    [DllImport("taos.dll", EntryPoint = "taos_connect", CallingConvention = CallingConvention.Cdecl)]
    static extern public IntPtr Connect(string ip, string user, string password, string db, short port);

    [DllImport("taos.dll", EntryPoint = "taos_errstr", CallingConvention = CallingConvention.Cdecl)]
    static extern private IntPtr taos_errstr(IntPtr res);
    static public string Error(IntPtr res)
    {
      IntPtr errPtr = taos_errstr(res);
      return Marshal.PtrToStringAnsi(errPtr);
    }

    [DllImport("taos.dll", EntryPoint = "taos_errno", CallingConvention = CallingConvention.Cdecl)]
    static extern public int ErrorNo(IntPtr res);

    [DllImport("taos.dll", EntryPoint = "taos_query", CallingConvention = CallingConvention.Cdecl)]
    static extern public IntPtr Query(IntPtr conn, string sqlstr);

    [DllImport("taos.dll", EntryPoint = "taos_affected_rows", CallingConvention = CallingConvention.Cdecl)]
    static extern public int AffectRows(IntPtr res);

    [DllImport("taos.dll", EntryPoint = "taos_field_count", CallingConvention = CallingConvention.Cdecl)]
    static extern public int FieldCount(IntPtr res);

    [DllImport("taos.dll", EntryPoint = "taos_fetch_fields", CallingConvention = CallingConvention.Cdecl)]
    static extern private IntPtr taos_fetch_fields(IntPtr res);
    static public List<TDengineMeta> FetchFields(IntPtr res)
    {
      const int fieldSize = 68;

      List<TDengineMeta> metas = new List<TDengineMeta>();
      if (res == IntPtr.Zero)
      {
        return metas;
      }

      int fieldCount = FieldCount(res);
      IntPtr fieldsPtr = taos_fetch_fields(res);
  
      for (int i = 0; i < fieldCount; ++i)
      {
        int offset = i * fieldSize;
        
        TDengineMeta meta = new TDengineMeta();
        meta.name = Marshal.PtrToStringAnsi(fieldsPtr + offset);
        meta.type = Marshal.ReadByte(fieldsPtr + offset + 65);
        meta.size = Marshal.ReadInt16(fieldsPtr + offset + 66);
        metas.Add(meta);
      }

      return metas;
    }

    [DllImport("taos.dll", EntryPoint = "taos_fetch_row", CallingConvention = CallingConvention.Cdecl)]
    static extern public IntPtr FetchRows(IntPtr res);

    [DllImport("taos.dll", EntryPoint = "taos_free_result", CallingConvention = CallingConvention.Cdecl)]
    static extern public IntPtr FreeResult(IntPtr res);

    [DllImport("taos.dll", EntryPoint = "taos_close", CallingConvention = CallingConvention.Cdecl)]
    static extern public int Close(IntPtr taos);
  }
}