using System;
using System.Text;
using System.Linq;
using System.Runtime.InteropServices;
using System.Windows.Forms;
using Range = Microsoft.Office.Interop.Excel.Range;
using Newtonsoft.Json.Linq;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// 任务窗格使用的自定义窗体控件
    /// </summary>
    [ComVisible(true)]
    public class TDFAggregationForm : TDControl
    {
        private ListView rawListView;
        private Label label8;
        private Label label9;
        private TextBox inputTextBox;
        private TableLayoutPanel tableLayoutPanel1;
        private Label label4;
        private TableLayoutPanel tableLayoutPanel3;
        private Button calculateButton;
        private Label label5;
        private Label label6;
        private TableLayoutPanel tableLayoutPanel4;
        private Button inputButton;
        private ComboBox functionCombox;
        private TableLayoutPanel tableLayoutPanel2;
        private Button beginButton;
        private TextBox beginTextBox;
        private TableLayoutPanel tableLayoutPanel5;
        private Button endButton;
        private TextBox endTextBox;
        private Label label1;
        private TableLayoutPanel tableLayoutPanel6;
        private Button showButton;
        private Label label7;
        private TableLayoutPanel tableLayoutPanel7;
        private TextBox rangeTextBox;
        private Button rangeButton;
        private Label label2;
        public Label TheLabel;
        public TDFAggregationForm()
        {
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            InitializeComponent();

            foreach (String aggFunction in TDFactory.Util.aggFunctions)
            {
                this.functionCombox.Items.Add(aggFunction);
            }
            this.functionCombox.SelectedIndex = 0;
            this.rawListView.ItemCheck += new System.Windows.Forms.ItemCheckEventHandler(this.ListView_ItemCheck);
        }

        public override void Start()
        {
            this.ClearListView();
            this.inputTextBox.Text = String.Empty;
            this.beginTextBox.Text = String.Empty;
            this.endTextBox.Text = String.Empty;
            this.functionCombox.SelectedIndex = 0;

            String address = TDFactory.Excel.GetSelectionAddress();
            address = address.Replace("$", String.Empty);
            this.rangeTextBox.Text = address;
            if (address == TDFactory.Util.TD_TABLE_EMPTY_SELECTION) return;

            Range range = TDFactory.Excel.GetSelectionRange();
            if (range == null) return;

            String formula = range.Formula;
            if (formula == null || formula == String.Empty) return;

            String table, func, column, begin, end;
            bool parse = this.ParseFunction(formula, out table, out func, out column, out begin, out end);
            if (!parse) return;

            this.inputTextBox.Text = table;
            this.beginTextBox.Text = begin;
            this.endTextBox.Text = end;

            String funcName = TDFactory.Excel.GetFirstValueByRangeAddress(func);
            if (funcName == String.Empty) funcName = func;

            String columnName = TDFactory.Excel.GetFirstValueByRangeAddress(column);
            if (columnName == String.Empty) columnName = column;
            
            this.FillListViewWithColumn(columnName);

            this.functionCombox.SelectedIndex = 0;
            for (int i = 0; i < TDFactory.Util.aggFunctions.Length; ++i)
            {
                if (funcName == TDFactory.Util.aggFunctions[i])
                {
                    this.functionCombox.SelectedIndex = i;
                    break;
                }
            }
        }

        private bool ParseFunction(String formula, out String table, out String func, out String column, out String begin, out String end)
        {
            table = func = column = begin = end = String.Empty;

            if (!formula.StartsWith("=TDAgg(")) return false;
            if (!formula.EndsWith(")")) return false;
            formula = formula.Substring(0, formula.Length - 1);
            formula = formula.Substring(7);

            String[] args = formula.Split(',');
            if (args.Length != 6) return false;

            func = args[0].Replace("\"", String.Empty);
            column = args[1].Replace("\"", String.Empty);
            begin = args[2].Replace("\"", String.Empty);
            end = args[3].Replace("\"", String.Empty);
            table = args[5].Replace("\"", String.Empty);

            return true;
        }

        private void ListView_ItemCheck(object sender, ItemCheckEventArgs e)
        {
            if (e.NewValue == CheckState.Checked)
            {
                foreach (ListViewItem lvi in this.rawListView.CheckedItems)
                {
                    if (e.Index != lvi.Index)
                    {
                        lvi.Checked = false;
                    }
                }
            }   
        }

        private void rangeButton_Click(object sender, EventArgs e)
        {
            this.Start();
        }

        private void inputButton_Click(object sender, System.EventArgs e)
        {
            String address = TDFactory.Excel.GetSelectionAddress();
            address = address.Replace("$", String.Empty);
            this.inputTextBox.Text = address;
        }
        
        private void beginButton_Click(object sender, System.EventArgs e)
        {
            String address = TDFactory.Excel.GetSelectionAddress();
            this.beginTextBox.Text = address;
        }

        private void endButton_Click(object sender, System.EventArgs e)
        {
            String address = TDFactory.Excel.GetSelectionAddress();
            this.endTextBox.Text = address;
        }
        
        private void showButton_Click(object sender, EventArgs e)
        {
            this.FillListView();
        }

        private void calculateButton_Click(object sender, System.EventArgs e)
        {
            String cell = this.rangeTextBox.Text;
            if (cell == String.Empty)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_CELL_INPUT);
                return;
            }
            if (!TDFactory.Excel.IsRangeAddressValid(cell))
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_CELL_ADDRESS);
                return;
            }
            if (!TDFactory.Excel.IsRangeAddressSingle(cell))
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_CELL_ADDRESS_SHOULD_ONLY_ONE);
                return;
            }

            bool isTableAddress = true;
            String table = this.inputTextBox.Text;
            if (table == String.Empty)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_TABLE_INPUT);
                return;
            }
            if (!TDFactory.Excel.IsRangeAddressValid(table))
            {
                isTableAddress = false;
            }
            else
            {
                if (!TDFactory.Excel.IsRangeAddressSingle(table))
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_TABLE_ADDRESS_SHOULD_ONLY_ONE);
                    return;
                }
            }

            bool isBeginTimeAddress = true;
            String beginTimeAddress = this.beginTextBox.Text;
            
            if (beginTimeAddress == String.Empty)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_BEGINTIMESTAMP_INPUT);
                return;
            }
            if (!TDFactory.Excel.IsRangeAddressValid(beginTimeAddress))
            {
                isBeginTimeAddress = false;
            }
            else
            {
                if (!TDFactory.Excel.IsRangeAddressSingle(beginTimeAddress))
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_BEGIN_TS_ADDRESS_SHOULD_ONLY_ONE);
                    return;
                }
            }

            bool isEndTimeAddress = true;
            String endTimeAddress = this.endTextBox.Text;
            if (endTimeAddress == String.Empty)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_ENDTIMESTAMP_INPUT);
                return;
            }
            
            if (!TDFactory.Excel.IsRangeAddressValid(endTimeAddress))
            {
                isEndTimeAddress = false;
            }
            else
            {
                if (!TDFactory.Excel.IsRangeAddressSingle(endTimeAddress))
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_END_TS_ADDRESS_SHOULD_ONLY_ONE);
                    return;
                }
            }

            int aggFuncIndex = this.functionCombox.SelectedIndex;
            if (aggFuncIndex >= TDFactory.Util.aggFunctions.Length - 1 || aggFuncIndex < 0)
            {
                this.functionCombox.SelectedIndex = TDFactory.Util.aggFunctions.Length - 1;
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_FUNCTIONS_SELECTED);
                return;
            }
            String aggFuncName = TDFactory.Util.aggFunctions[aggFuncIndex];

            String selectColumns = this.GetSelectListViewItems();
            if (selectColumns == String.Empty)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_FIELDS_SELECT);
                return;
            }


            TDFactory.StartUpdate();
            
            try
            {
                Microsoft.Office.Interop.Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;

                StringBuilder builder = new StringBuilder(200);
                builder.Append("=TDAgg(");
                builder.Append("\"").Append(aggFuncName).Append("\",\"").Append(selectColumns).Append("\",");
                if (isBeginTimeAddress) builder.Append(beginTimeAddress).Append(",");
                else builder.Append("\"").Append(beginTimeAddress).Append("\",");
                if (isEndTimeAddress) builder.Append(endTimeAddress).Append(",");
                else builder.Append("\"").Append(endTimeAddress).Append("\",");
                builder.Append("\"").Append(TDFactory.Persist.connectDB).Append("\",");
                if (isTableAddress) builder.Append(table).Append(")");
                else builder.Append("\"").Append(table).Append("\")");

                TDFactory.Formula.ClearFormulaResult(builder.ToString());

                TDFactory.Formula.CalcType = TDFormulaCalcType.TD_FORMULA_CALC_SQL;
                Range range = TDFactory.Excel.GetRangeByAddress(cell);
                String formula = builder.ToString().Substring(1);
                Object sql1 = activeWorksheet.Evaluate(formula);
                if (sql1 == null) TDFactory.Util.ShowError(TDFactory.Util.TD_FUNCTION_CAN_NOT_CALCULATE);
                else
                {
                    String sql = sql1 as String;
                    TDFactory.Formula.ClearSqlResult(sql);
                    String result = TDFactory.Formula.CalculateResultAsync(sql).Result;
                    TDFactory.Formula.PutSqlResult(sql, result);
                    TDFactory.Formula.PutFormulaResult(builder.ToString(), result);

                    range.Value2 = result;
                    range.Formula = builder.ToString();
                }

                TDFactory.Formula.CalcType = TDFormulaCalcType.TD_FORMULA_NOT_CALC;
            }
            catch (Exception ex)
            {
                TDFactory.Util.ShowException(ex);
            }
            finally { }

            TDFactory.EndUpdate();
        }

        private String GetSelectListViewItems()
        {
            if (this.rawListView.Items.Count == 0) return String.Empty;
            foreach (ListViewItem lvi in this.rawListView.CheckedItems)
            {
                return lvi.Text;
            }

            return String.Empty;
        }

        private void ClearListViewWithColumn()
        {
            this.rawListView.BeginUpdate();
            this.rawListView.Clear();

            ColumnHeader ch1 = new ColumnHeader();
            ch1.Text = TDFactory.Util.TD_SELECT_FIELD_COLUMN;
            ch1.Width = 400;
            ch1.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch1);
            
            this.rawListView.GridLines = true;
            this.rawListView.FullRowSelect = true;
            this.rawListView.HeaderStyle = ColumnHeaderStyle.Clickable;
            this.rawListView.CheckBoxes = true;

            this.rawListView.EndUpdate();
        }
        
        private void FillListViewWithColumn(String column)
        {
            this.ClearListViewWithColumn();
            ListViewItem lvi = new ListViewItem();
            lvi.Text = column;
            lvi.SubItems.Add(column);
            lvi.Checked = true;
            this.rawListView.Items.Add(lvi);
        }

        private void ClearListView()
        {
            this.rawListView.BeginUpdate();
            this.rawListView.Clear();

            ColumnHeader ch1 = new ColumnHeader();
            ch1.Text = TDFactory.Util.TD_FIELD_NAME_COLUMN;
            ch1.Width = 280;
            ch1.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch1);

            ColumnHeader ch2 = new ColumnHeader();
            ch2.Text = TDFactory.Util.TD_FIELD_TYPE_COLUMN;
            ch2.Width = 120;
            ch2.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch2);

            this.rawListView.GridLines = true;
            this.rawListView.FullRowSelect = true;
            this.rawListView.HeaderStyle = ColumnHeaderStyle.Clickable;
            this.rawListView.CheckBoxes = true;

            this.rawListView.EndUpdate();
        }

        private void FillListView()
        {
            String checkedText = String.Empty;
            foreach (ListViewItem lvi in this.rawListView.CheckedItems)
            {
                checkedText = lvi.Text.ToLower();
                break;
            }

            this.ClearListView();

            String tablename = TDFactory.Excel.GetFirstValueByRangeAddress(this.inputTextBox.Text);
            if (tablename == String.Empty) tablename = this.inputTextBox.Text;
            if (tablename == String.Empty)
            {
                TDFactory.Util.ShowError(TDFactory.Util.TD_NO_TABLE_INPUT);
                return;
            }
            
            String sql = "describe " + TDFactory.Persist.connectDB + "." + tablename;
            TDHttpReturn resp = TDFactory.Util.DoRequest(sql, TDHttpTimestampType.TD_SHOW_TIMESTSAMP).Result;
            JObject jo = resp.jo;
            if (jo == null)
            {
                TDFactory.Util.ShowError(resp.error);
            }
            else
            {
                Array heads = jo.GetValue("head").ToArray();
                Array datas = jo.GetValue("data").ToArray();
                int headLength = heads.GetLength(0);
                int dataLength = datas.GetLength(0);

                if (headLength != 4)
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_INVALID_RESPONSE_FROM_SERVER);
                    return;
                }

                if (dataLength < 2)
                {
                    TDFactory.Util.ShowError(TDFactory.Util.TD_TABLE_DESCRIBE_FAILED);
                    return;
                }

                this.rawListView.BeginUpdate();

                for (int row = 0; row < dataLength; ++row)
                {
                    Array dataCols = (datas.GetValue(row) as JToken).ToArray();
                    String field = dataCols.GetValue(0).ToString();
                    String fieldType = dataCols.GetValue(1).ToString();
                    String tag = dataCols.GetValue(3).ToString();

                    //jump the tag fields of tables
                    if (tag != "")
                    {
                        continue;
                    }

                    if (fieldType == "BINARY" || fieldType == "NCHAR")
                    {
                        fieldType = fieldType + "(" + dataCols.GetValue(2).ToString() + ")";
                    }

                    ListViewItem lvi = new ListViewItem();
                    lvi.Text = field;
                    lvi.SubItems.Add(fieldType.ToLower());
                    if (field == checkedText) lvi.Checked = true;
                    this.rawListView.Items.Add(lvi);
                }

                this.rawListView.EndUpdate();
            }
        }

        private void InitializeComponent()
        {
            this.rawListView = new System.Windows.Forms.ListView();
            this.label8 = new System.Windows.Forms.Label();
            this.label9 = new System.Windows.Forms.Label();
            this.inputTextBox = new System.Windows.Forms.TextBox();
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.tableLayoutPanel7 = new System.Windows.Forms.TableLayoutPanel();
            this.rangeTextBox = new System.Windows.Forms.TextBox();
            this.rangeButton = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.functionCombox = new System.Windows.Forms.ComboBox();
            this.label4 = new System.Windows.Forms.Label();
            this.tableLayoutPanel3 = new System.Windows.Forms.TableLayoutPanel();
            this.calculateButton = new System.Windows.Forms.Button();
            this.label5 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.tableLayoutPanel4 = new System.Windows.Forms.TableLayoutPanel();
            this.inputButton = new System.Windows.Forms.Button();
            this.tableLayoutPanel2 = new System.Windows.Forms.TableLayoutPanel();
            this.beginButton = new System.Windows.Forms.Button();
            this.beginTextBox = new System.Windows.Forms.TextBox();
            this.tableLayoutPanel5 = new System.Windows.Forms.TableLayoutPanel();
            this.endButton = new System.Windows.Forms.Button();
            this.endTextBox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.tableLayoutPanel6 = new System.Windows.Forms.TableLayoutPanel();
            this.showButton = new System.Windows.Forms.Button();
            this.label7 = new System.Windows.Forms.Label();
            this.tableLayoutPanel1.SuspendLayout();
            this.tableLayoutPanel7.SuspendLayout();
            this.tableLayoutPanel3.SuspendLayout();
            this.tableLayoutPanel4.SuspendLayout();
            this.tableLayoutPanel2.SuspendLayout();
            this.tableLayoutPanel5.SuspendLayout();
            this.tableLayoutPanel6.SuspendLayout();
            this.SuspendLayout();
            // 
            // rawListView
            // 
            this.rawListView.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.rawListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rawListView.Location = new System.Drawing.Point(18, 363);
            this.rawListView.MultiSelect = false;
            this.rawListView.Name = "rawListView";
            this.rawListView.Size = new System.Drawing.Size(464, 120);
            this.rawListView.TabIndex = 10;
            this.rawListView.UseCompatibleStateImageBehavior = false;
            this.rawListView.View = System.Windows.Forms.View.Details;
            // 
            // label8
            // 
            this.label8.AutoSize = true;
            this.label8.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label8.Location = new System.Drawing.Point(18, 184);
            this.label8.Margin = new System.Windows.Forms.Padding(3);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(464, 22);
            this.label8.TabIndex = 18;
            this.label8.Text = "timestamp from";
            // 
            // label9
            // 
            this.label9.AutoSize = true;
            this.label9.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label9.Location = new System.Drawing.Point(18, 253);
            this.label9.Margin = new System.Windows.Forms.Padding(3);
            this.label9.Name = "label9";
            this.label9.Size = new System.Drawing.Size(464, 22);
            this.label9.TabIndex = 19;
            this.label9.Text = "timestamp to";
            // 
            // inputTextBox
            // 
            this.inputTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.inputTextBox.Location = new System.Drawing.Point(3, 3);
            this.inputTextBox.Name = "inputTextBox";
            this.inputTextBox.Size = new System.Drawing.Size(342, 29);
            this.inputTextBox.TabIndex = 3;
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel7, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.label2, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.functionCombox, 0, 12);
            this.tableLayoutPanel1.Controls.Add(this.label4, 0, 13);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel3, 0, 14);
            this.tableLayoutPanel1.Controls.Add(this.label5, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.label6, 0, 3);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel4, 0, 4);
            this.tableLayoutPanel1.Controls.Add(this.rawListView, 0, 10);
            this.tableLayoutPanel1.Controls.Add(this.label8, 0, 5);
            this.tableLayoutPanel1.Controls.Add(this.label9, 0, 7);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel2, 0, 6);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel5, 0, 8);
            this.tableLayoutPanel1.Controls.Add(this.label1, 0, 11);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel6, 0, 9);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.Padding = new System.Windows.Forms.Padding(15);
            this.tableLayoutPanel1.RowCount = 16;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(500, 800);
            this.tableLayoutPanel1.TabIndex = 4;
            // 
            // tableLayoutPanel7
            // 
            this.tableLayoutPanel7.ColumnCount = 2;
            this.tableLayoutPanel7.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel7.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel7.Controls.Add(this.rangeTextBox, 0, 0);
            this.tableLayoutPanel7.Controls.Add(this.rangeButton, 1, 0);
            this.tableLayoutPanel7.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel7.Location = new System.Drawing.Point(18, 74);
            this.tableLayoutPanel7.Name = "tableLayoutPanel7";
            this.tableLayoutPanel7.RowCount = 1;
            this.tableLayoutPanel7.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel7.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 35F));
            this.tableLayoutPanel7.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel7.TabIndex = 35;
            // 
            // rangeTextBox
            // 
            this.rangeTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rangeTextBox.Location = new System.Drawing.Point(3, 3);
            this.rangeTextBox.Name = "rangeTextBox";
            this.rangeTextBox.Size = new System.Drawing.Size(342, 29);
            this.rangeTextBox.TabIndex = 1;
            // 
            // rangeButton
            // 
            this.rangeButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rangeButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.rangeButton.Location = new System.Drawing.Point(351, 3);
            this.rangeButton.Name = "rangeButton";
            this.rangeButton.Size = new System.Drawing.Size(110, 29);
            this.rangeButton.TabIndex = 2;
            this.rangeButton.Text = "Select";
            this.rangeButton.UseVisualStyleBackColor = true;
            this.rangeButton.Click += new System.EventHandler(this.rangeButton_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label2.Location = new System.Drawing.Point(18, 46);
            this.label2.Margin = new System.Windows.Forms.Padding(3);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(464, 22);
            this.label2.TabIndex = 34;
            this.label2.Text = "cell to be calculated";
            // 
            // functionCombox
            // 
            this.functionCombox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.functionCombox.DropDownStyle = System.Windows.Forms.ComboBoxStyle.DropDownList;
            this.functionCombox.FormattingEnabled = true;
            this.functionCombox.Location = new System.Drawing.Point(18, 517);
            this.functionCombox.Name = "functionCombox";
            this.functionCombox.Size = new System.Drawing.Size(464, 30);
            this.functionCombox.TabIndex = 11;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label4.Location = new System.Drawing.Point(18, 553);
            this.label4.Margin = new System.Windows.Forms.Padding(3);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(464, 22);
            this.label4.TabIndex = 8;
            this.label4.Text = "Operation";
            // 
            // tableLayoutPanel3
            // 
            this.tableLayoutPanel3.ColumnCount = 2;
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel3.Controls.Add(this.calculateButton, 1, 0);
            this.tableLayoutPanel3.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel3.Location = new System.Drawing.Point(18, 581);
            this.tableLayoutPanel3.Name = "tableLayoutPanel3";
            this.tableLayoutPanel3.RowCount = 1;
            this.tableLayoutPanel3.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel3.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel3.TabIndex = 9;
            // 
            // calculateButton
            // 
            this.calculateButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.calculateButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.calculateButton.Location = new System.Drawing.Point(351, 3);
            this.calculateButton.Margin = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.calculateButton.Name = "calculateButton";
            this.calculateButton.Size = new System.Drawing.Size(113, 29);
            this.calculateButton.TabIndex = 0;
            this.calculateButton.Text = "Calculate";
            this.calculateButton.UseVisualStyleBackColor = true;
            this.calculateButton.Click += new System.EventHandler(this.calculateButton_Click);
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label5.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label5.Location = new System.Drawing.Point(18, 18);
            this.label5.Margin = new System.Windows.Forms.Padding(3);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(464, 22);
            this.label5.TabIndex = 0;
            this.label5.Text = "Input";
            // 
            // label6
            // 
            this.label6.AutoSize = true;
            this.label6.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label6.Location = new System.Drawing.Point(18, 115);
            this.label6.Margin = new System.Windows.Forms.Padding(3);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(464, 22);
            this.label6.TabIndex = 11;
            this.label6.Text = "name of table or stable ";
            // 
            // tableLayoutPanel4
            // 
            this.tableLayoutPanel4.ColumnCount = 2;
            this.tableLayoutPanel4.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel4.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel4.Controls.Add(this.inputTextBox, 0, 0);
            this.tableLayoutPanel4.Controls.Add(this.inputButton, 1, 0);
            this.tableLayoutPanel4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel4.Location = new System.Drawing.Point(18, 143);
            this.tableLayoutPanel4.Name = "tableLayoutPanel4";
            this.tableLayoutPanel4.RowCount = 1;
            this.tableLayoutPanel4.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel4.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Absolute, 35F));
            this.tableLayoutPanel4.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel4.TabIndex = 12;
            // 
            // inputButton
            // 
            this.inputButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.inputButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.inputButton.Location = new System.Drawing.Point(351, 3);
            this.inputButton.Name = "inputButton";
            this.inputButton.Size = new System.Drawing.Size(110, 29);
            this.inputButton.TabIndex = 4;
            this.inputButton.Text = "Select";
            this.inputButton.UseVisualStyleBackColor = true;
            this.inputButton.Click += new System.EventHandler(this.inputButton_Click);
            // 
            // tableLayoutPanel2
            // 
            this.tableLayoutPanel2.ColumnCount = 2;
            this.tableLayoutPanel2.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel2.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel2.Controls.Add(this.beginButton, 1, 0);
            this.tableLayoutPanel2.Controls.Add(this.beginTextBox, 0, 0);
            this.tableLayoutPanel2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel2.Location = new System.Drawing.Point(18, 212);
            this.tableLayoutPanel2.Name = "tableLayoutPanel2";
            this.tableLayoutPanel2.RowCount = 1;
            this.tableLayoutPanel2.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel2.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel2.TabIndex = 29;
            // 
            // beginButton
            // 
            this.beginButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.beginButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.beginButton.Location = new System.Drawing.Point(351, 3);
            this.beginButton.Name = "beginButton";
            this.beginButton.Size = new System.Drawing.Size(110, 29);
            this.beginButton.TabIndex = 6;
            this.beginButton.Text = "Select";
            this.beginButton.UseVisualStyleBackColor = true;
            this.beginButton.Click += new System.EventHandler(this.beginButton_Click);
            // 
            // beginTextBox
            // 
            this.beginTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.beginTextBox.Location = new System.Drawing.Point(3, 3);
            this.beginTextBox.Name = "beginTextBox";
            this.beginTextBox.Size = new System.Drawing.Size(342, 29);
            this.beginTextBox.TabIndex = 5;
            // 
            // tableLayoutPanel5
            // 
            this.tableLayoutPanel5.ColumnCount = 2;
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel5.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel5.Controls.Add(this.endButton, 1, 0);
            this.tableLayoutPanel5.Controls.Add(this.endTextBox, 0, 0);
            this.tableLayoutPanel5.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel5.Location = new System.Drawing.Point(18, 281);
            this.tableLayoutPanel5.Name = "tableLayoutPanel5";
            this.tableLayoutPanel5.RowCount = 1;
            this.tableLayoutPanel5.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel5.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel5.TabIndex = 30;
            // 
            // endButton
            // 
            this.endButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.endButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.endButton.Location = new System.Drawing.Point(351, 3);
            this.endButton.Name = "endButton";
            this.endButton.Size = new System.Drawing.Size(110, 29);
            this.endButton.TabIndex = 8;
            this.endButton.Text = "Select";
            this.endButton.UseVisualStyleBackColor = true;
            this.endButton.Click += new System.EventHandler(this.endButton_Click);
            // 
            // endTextBox
            // 
            this.endTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.endTextBox.Location = new System.Drawing.Point(3, 3);
            this.endTextBox.Name = "endTextBox";
            this.endTextBox.Size = new System.Drawing.Size(342, 29);
            this.endTextBox.TabIndex = 7;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label1.Location = new System.Drawing.Point(18, 489);
            this.label1.Margin = new System.Windows.Forms.Padding(3);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(464, 22);
            this.label1.TabIndex = 32;
            this.label1.Text = "aggregation function";
            // 
            // tableLayoutPanel6
            // 
            this.tableLayoutPanel6.ColumnCount = 2;
            this.tableLayoutPanel6.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 75F));
            this.tableLayoutPanel6.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel6.Controls.Add(this.showButton, 0, 0);
            this.tableLayoutPanel6.Controls.Add(this.label7, 0, 0);
            this.tableLayoutPanel6.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel6.Location = new System.Drawing.Point(18, 322);
            this.tableLayoutPanel6.Name = "tableLayoutPanel6";
            this.tableLayoutPanel6.RowCount = 1;
            this.tableLayoutPanel6.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel6.Size = new System.Drawing.Size(464, 35);
            this.tableLayoutPanel6.TabIndex = 33;
            // 
            // showButton
            // 
            this.showButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.showButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.showButton.Location = new System.Drawing.Point(351, 3);
            this.showButton.Name = "showButton";
            this.showButton.Size = new System.Drawing.Size(110, 29);
            this.showButton.TabIndex = 9;
            this.showButton.Text = "Show";
            this.showButton.UseVisualStyleBackColor = true;
            this.showButton.Click += new System.EventHandler(this.showButton_Click);
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label7.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label7.Location = new System.Drawing.Point(3, 3);
            this.label7.Margin = new System.Windows.Forms.Padding(3);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(342, 29);
            this.label7.TabIndex = 0;
            this.label7.Text = "Columns";
            // 
            // TDFAggregationForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 22F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoScroll = true;
            this.AutoScrollMinSize = new System.Drawing.Size(400, 800);
            this.BackColor = System.Drawing.SystemColors.Control;
            this.Controls.Add(this.tableLayoutPanel1);
            this.DoubleBuffered = true;
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "TDFAggregationForm";
            this.Size = new System.Drawing.Size(500, 800);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.tableLayoutPanel7.ResumeLayout(false);
            this.tableLayoutPanel7.PerformLayout();
            this.tableLayoutPanel3.ResumeLayout(false);
            this.tableLayoutPanel4.ResumeLayout(false);
            this.tableLayoutPanel4.PerformLayout();
            this.tableLayoutPanel2.ResumeLayout(false);
            this.tableLayoutPanel2.PerformLayout();
            this.tableLayoutPanel5.ResumeLayout(false);
            this.tableLayoutPanel5.PerformLayout();
            this.tableLayoutPanel6.ResumeLayout(false);
            this.tableLayoutPanel6.PerformLayout();
            this.ResumeLayout(false);

        }
    }
}
