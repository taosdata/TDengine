using System;
using System.Windows.Forms;

using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace TDengineExcelPlugins
{
    public class TDExcel
    {
        //获取所选单元格的第一个单元格的对象
        public Range GetSelectionRange()
        {
            Range range = null;
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                if (activeWorksheet == null) return range;

                object selectRange = TDFactory.Application().Selection;
                if (selectRange == null) return range;

                return this.GetFirstCellByRange(selectRange as Range);
            }
            catch (Exception) { }
            finally { }

            return range;
        }

        //获取所选单元格的第一个单元格的地址
        public String GetSelectionAddress()
        {
            String address = TDFactory.Util.TD_TABLE_EMPTY_SELECTION;
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                if (activeWorksheet == null) return address;

                object selectRange = TDFactory.Application().Selection;
                if (selectRange == null) return address;

                Range cell = this.GetFirstCellByRange(selectRange as Range);

                return cell.get_Address(Missing.Value, System.Reflection.Missing.Value, Excel.XlReferenceStyle.xlA1, Missing.Value, Missing.Value);
            }
            catch (Exception e) {
                TDFactory.Util.ShowException(e);
            }
            finally { }

            return address;
        }

        //获取所选单元格的第一个单元格的值
        public String GetSelectionValue()
        {
            String address = TDFactory.Util.TD_TABLE_EMPTY_SELECTION;
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                if (activeWorksheet == null) return address;

                object selectRange = TDFactory.Application().Selection;
                if (selectRange == null) return address;

                Range cell = this.GetFirstCellByRange(selectRange as Range);

                return cell.Value2;
            }
            catch (Exception) { }
            finally { }

            return address;
        }

        //获取Range中的第一个单元格
        public Range GetFirstCellByRange(Range ranges)
        {
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                int beginRow = ranges.Row;
                int beginCol = ranges.Column;
                return activeWorksheet.Cells[beginRow, beginCol];
            }
            catch (Exception) { }
            finally { }

            return null;
        }
        
        //是否是一个有效的单元格地址
        public bool IsRangeAddressValid(String address)
        {
            return this.GetRangeByAddress(address) != null;
        }

        //地址是否只包含一个单元格
        public bool IsRangeAddressSingle(String address)
        {
            return !address.Contains(":");
        }

        //如果是有效的单元格地址，那么获取第一个单元格的值
        public String GetFirstValueByRangeAddress(String address)
        {
            Range ranges = this.GetRangeByAddress(address);
            if (ranges == null) return String.Empty;

            Range cell = this.GetFirstCellByRange(ranges);
            if (cell == null) return String.Empty;

            try
            {
                return cell.Value2 as String;
            }
            catch (Exception) { }
            finally { }

            return String.Empty;
        }

        //如果是有效的单元格地址，那么获取第一个单元格的公式
        public String GetFirstFormulaByRangeAddress(String address)
        {
            Range ranges = this.GetRangeByAddress(address);
            if (ranges == null) return String.Empty;

            Range cell = this.GetFirstCellByRange(ranges);
            if (cell == null) return String.Empty;

            try
            {
                if (cell.Formula == null) return String.Empty;
                return cell.Formula as String;
            }
            catch (Exception) { }
            finally { }

            return String.Empty;
        }

        //如果是有效的单元格地址，那么获取第一个单元格的地址
        public String GetFirstAddressByRangeAddress(String address)
        {
            Range ranges = this.GetRangeByAddress(address);
            if (ranges == null) return String.Empty;

            Range cell = this.GetFirstCellByRange(ranges);
            if (cell == null) return String.Empty;

            try
            {
                return cell.get_Address(Missing.Value, System.Reflection.Missing.Value, Excel.XlReferenceStyle.xlA1, Missing.Value, Missing.Value);
            }
            catch (Exception) { }
            finally { }

            return String.Empty;
        }

        //如果是有效的单元格地址，那么获取第一个单元格的地址
        public Range GetFirstRangeByRangeAddress(String address)
        {
            Range ranges = this.GetRangeByAddress(address);
            if (ranges == null) return null;
            return this.GetFirstCellByRange(ranges);
        }

        //根据地址获取range
        public Range GetRangeByAddress(String rangeAddress)
        {
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                Range target = (Excel.Range)activeWorksheet.Range[rangeAddress];
                return target;
            }
            catch (Exception) { }
            finally { }

            return null;
        }

        public int[] GetUnHiddenColumns(int beginColumn, int columnLength)
        {
            Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;

            int[] columnArray = new int[columnLength];
            int col = 0, columnArrayIndex = 0;

            while (columnArrayIndex < columnLength)
            {
                if (!activeWorksheet.Cells[1, col + beginColumn].EntireColumn.Hidden)
                {
                    columnArray[columnArrayIndex++] = col + beginColumn;
                }
                col++;
            }

            return columnArray;
        }

        public int[] GetUnHiddenRows(int beginRow, int rowLength)
        {
            Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;

            int[] rowArray = new int[rowLength];
            int row = 0, rowArrayIndex = 0;

            while (rowArrayIndex < rowLength)
            {
                if (!activeWorksheet.Cells[row + beginRow, 1].EntireRow.Hidden)
                {
                    rowArray[rowArrayIndex++] = row + beginRow;
                }
                row++;
            }

            return rowArray;
        }

        public String SelectRangeByString(String rangeAddress, TDFormSelectType selectType)
        {
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                Range target = (Excel.Range)activeWorksheet.Range[rangeAddress];
                if (target != null)
                {
                    if (selectType == TDFormSelectType.TD_FORM_SELECT_CELL)
                    {
                        int beginRow = target.Row;
                        int beginCol = target.Column;
                        Range selectedTargt = activeWorksheet.Cells[beginRow, beginCol];
                        selectedTargt.Select();
                        return selectedTargt.get_Address(Missing.Value, System.Reflection.Missing.Value, Excel.XlReferenceStyle.xlA1, Missing.Value, Missing.Value);
                    }
                    else if (selectType == TDFormSelectType.TD_FORM_SELECT_ROW)
                    {
                        TDFactory.Util.ShowError("select row");
                    }
                    else
                    {
                        TDFactory.Util.ShowError("invalid select type");
                    }

                }
            }
            catch (Exception e)
            {
                TDFactory.Util.ShowException(e);
            }
            finally { }

            return "invalid range";
        }


        public String GetRangeValuesIncludeHidden(Range range)
        {
            try
            {
                if (range == null)
                {
                    return "";
                }

                if (range.Value2 == null)
                {
                    return "";
                }

                if (range.Value2.GetType() == typeof(Object[,]))
                {
                    String ret = "";
                    bool firstRow = true;
                    Object[,] rangeArray = range.Value2 as Object[,];
                    foreach (Object rangeValue in rangeArray)
                    {
                        if (rangeValue == null)
                        {
                            continue;
                        }

                        if (firstRow)
                        {
                            firstRow = false;
                            ret = rangeValue.ToString();
                        }
                        else
                        {
                            ret = ret + "," + rangeValue.ToString();
                        }
                    }

                    return ret;
                }
                else
                {
                    return range.Value2.ToString;
                }
            }
            catch (Exception e)
            {
                TDFactory.Util.ShowException(e);
            }
            finally { }

            return "";
        }

        //获取所选单元格的所有值
        public String GetSelectionRangesValue()
        {
            String ret = TDFactory.Util.TD_TABLE_EMPTY_SELECTION;
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                if (activeWorksheet == null) return ret;

                object selectRange = TDFactory.Application().Selection;
                if (selectRange == null) return ret;

                bool firstRow = true;
                foreach (Range range in selectRange as Range)
                {
                    if (range == null)
                    {
                        continue;
                    }

                    if (range.Value2 == null)
                    {
                        continue;
                    }

                    if (range.EntireRow.Hidden || range.EntireColumn.Hidden)
                    {
                        continue;
                    }

                    if (firstRow)
                    {
                        firstRow = false;
                        ret = range.Value2.ToString();
                    }
                    else
                    {
                        ret = ret + "," + range.Value2.ToString();
                    }
                }
            }
            catch (Exception) { }
            finally { }

            return ret;
        }
    }
}
