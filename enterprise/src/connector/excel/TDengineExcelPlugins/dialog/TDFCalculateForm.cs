using System.Runtime.InteropServices;
using System;
using System.Linq;
using System.Windows.Forms;
using System.Threading;
using Excel = Microsoft.Office.Interop.Excel;
using Worksheet = Microsoft.Office.Interop.Excel.Worksheet;
using Range = Microsoft.Office.Interop.Excel.Range;
using Missing = System.Reflection.Missing;
using Newtonsoft.Json.Linq;
using System.Collections;
using System.Threading.Tasks;
using System.ComponentModel;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// 任务窗格使用的自定义窗体控件
    /// </summary>
    [ComVisible(true)]
    public class TDFCalculateForm : TDControl
    {
        private String lastWorkSheetName = "";

        public const int threadNum = 5;
        private bool[] threadsFinished = new bool[threadNum];
        private DateTime beginCalculateTime;
        private ArrayList formulas = new ArrayList();
        private ArrayList mergedFormulas = new ArrayList();
        private BackgroundWorker[] workers = null;
        private BackgroundWorker watchThread = null;

        private TableLayoutPanel tableLayoutPanel1;
        private Label label1;
        private Label label2;
        private Label label4;
        private TableLayoutPanel tableLayoutPanel3;
        private Button stopButton;
        private Label label7;
        private ListView rawListView;
        private Label label3;
        private Button startButton;
        private ProgressBar infoProgressBar;
        private Button applyButton;
        private Button refreshButton;
        private RichTextBox infoTextBox;
        public Label TheLabel;
        public TDFCalculateForm()
        {
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            this.InitializeComponent();
        }

        public override void Initialize()
        {
            this.watchThread = new BackgroundWorker();
            this.watchThread.WorkerSupportsCancellation = true;
            this.watchThread.DoWork += RunWorkerCompleted;
            
            if (this.workers == null)
            {
                this.workers = new BackgroundWorker[TDFCalculateForm.threadNum];
                for (int index = 0; index < TDFCalculateForm.threadNum; index++)
                {
                    workers[index] = new BackgroundWorker();
                    workers[index].WorkerSupportsCancellation = true;
                    workers[index].DoWork += ParallelExecuting;
                    //worker.WorkerReportsProgress = true;
                    //worker.ProgressChanged += ProgressChanged;
                    //worker.RunWorkerCompleted += RunWorkerCompleted;
                }
            }
        }

        public override void Start()
        {
            int totalCells = this.FillListView();
            if (TDFactory.Application().ActiveSheet != null)
                this.lastWorkSheetName = TDFactory.Application().ActiveSheet.Name;
            else
                this.lastWorkSheetName = "";

            this.infoProgressBar.Maximum = totalCells;
            this.infoProgressBar.Minimum = 0;
            this.infoProgressBar.Value = 0;
            this.infoProgressBar.Step = 1;
            this.ClearLog();
            this.AddLog("active worksheet is " + this.lastWorkSheetName);
            this.AddLog("find " + totalCells + " cells contain formula");
        }

        public override void Save()
        {
        }

        private void MergeFormulaSqls()
        {
            this.mergedFormulas.Clear();
            Hashtable formulasMap = new Hashtable();
            foreach (TDExcelFormula formula in this.formulas)
            {
                String key = formula.GetKey();
                Object v = formulasMap[key];
                TDExcelMergedFromula mergedFormula;
                if (v == null)
                {
                    mergedFormula = new TDExcelMergedFromula();
                    formulasMap[key] = mergedFormula;
                    this.mergedFormulas.Add(mergedFormula);
                }
                else
                {
                    mergedFormula = v as TDExcelMergedFromula;
                }

                mergedFormula.Add(formula);
                if (mergedFormula.Full())
                { 
                    formulasMap.Remove(key);
                }
            }
        }

        private void CalcFormulaSqls()
        {
            Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
            foreach (TDExcelFormula formula in this.formulas)
            {
                formula.result = String.Empty;
                formula.finished = false;
                Object result = activeWorksheet.Evaluate(formula.formula.Substring(1));
                if (result != null)
                    formula.sql = result as String;
            }
        }

        private void ParallelExecuting(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;
            if (worker == null) return;

            int index = (int)e.Argument;

            for (int i = index; i < this.mergedFormulas.Count; i += TDFCalculateForm.threadNum)
            {
                if (worker.CancellationPending)
                {
                    e.Cancel = true;
                    break;
                }

                TDExcelMergedFromula formula = this.mergedFormulas[i] as TDExcelMergedFromula;
                formula.Run();
                lock (TDFactory.Formula)
                {
                    foreach (TDExcelFormula f in formula.formulas)
                    {
                        TDFactory.Formula.PutSqlResult(f.sql, f.result);
                        TDFactory.Formula.PutFormulaResult(f.formula, f.result);
                        f.finished = true;
                    }
                }
                
                //Thread.Sleep(100);
            }

            this.threadsFinished[index] = true;
        }

        private int GetFinishedForumlas()
        {
            int finishedForumlas = 0;
            for (int i = 0; i < this.formulas.Count; ++i)
            {
                TDExcelFormula formula = this.formulas[i] as TDExcelFormula;
                if (formula.finished) finishedForumlas++;
            }

            return finishedForumlas;
        }

        private delegate void ProgressChangedInvoke(object sender, ProgressChangedEventArgs e);
        private void ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            if (this.infoProgressBar.InvokeRequired)
            {
                ProgressChangedInvoke d = new ProgressChangedInvoke(ProgressChanged);
                this.Invoke(d, new object[] { sender, e });
            }
            else
            {
                int total = this.GetFinishedForumlas();
                if (this.infoProgressBar.Value < this.infoProgressBar.Maximum)
                    this.infoProgressBar.Value += e.ProgressPercentage;
            }
        }

        private delegate void SetProgressValueInvoke(int total);
        private void SetProgressValue(int total)
        {
            if (this.infoProgressBar.InvokeRequired)
            {
                SetProgressValueInvoke d = new SetProgressValueInvoke(SetProgressValue);
                this.Invoke(d, new object[] { total });
            }
            else
            {
                if (total < this.infoProgressBar.Maximum)
                    this.infoProgressBar.Value = total;
                else
                    this.infoProgressBar.Value = this.infoProgressBar.Maximum;
            }
        }

        private delegate void RunWorkerCompletedInvoke(object sender, DoWorkEventArgs e);
        private void RunWorkerCompleted(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;
            if (worker == null) return;

            int runThreadsNum = TDFCalculateForm.threadNum;
            while (true)
            {
                runThreadsNum = 0;
                for (int index = 0; index < TDFCalculateForm.threadNum; index++)
                {
                    if (!this.threadsFinished[index])
                        runThreadsNum++;
                }
                if (runThreadsNum > 0)
                {
                    int total = this.GetFinishedForumlas();
                    SetProgressValue(total);
                    Thread.Sleep(200);
                }
                else
                {
                    break;
                }
            }

            SetProgressValue(this.formulas.Count);

            if (this.rawListView.InvokeRequired)
            {
                RunWorkerCompletedInvoke d = new RunWorkerCompletedInvoke(RunWorkerCompleted);
                this.Invoke(d, new object[] { sender, e });
            }
            else
            {
                this.rawListView.BeginUpdate();
                for (int i = 0; i < this.formulas.Count; i++)
                {
                    TDExcelFormula formula = this.formulas[i] as TDExcelFormula;
                    if (!formula.finished) continue;
                    ListViewItem lvi = this.rawListView.Items[i];
                    lvi.SubItems[2].Text = formula.result;
                }
                this.rawListView.EndUpdate();

                this.AddLog("total " + (DateTime.Now - this.beginCalculateTime).TotalSeconds + " seconds");
                this.AddLog("total " + this.GetFinishedForumlas() + " forumlas calculated");
                this.AddLog("working thread finished");

                TDFactory.Formula.CalcType = TDFormulaCalcType.TD_FORMULA_NOT_CALC;
                //TDFactory.EndUpdate();
            }
        }

        private void startButton_Click(object sender, EventArgs e)
        {
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                if (activeWorksheet == null || this.lastWorkSheetName == "")
                {
                    this.AddLog("no active worksheet");
                    return;
                }

                if (this.lastWorkSheetName != activeWorksheet.Name)
                {
                    this.AddLog("active worksheet changed from " + this.lastWorkSheetName + " to " + activeWorksheet.Name);
                    return;
                }

                TDFactory.StartUpdate();
                TDFactory.Formula.ClearAll();
                this.AddLog("prepare parse excel formula");

                TDFactory.Formula.CalcType = TDFormulaCalcType.TD_FORMULA_CALC_SQL;
                this.CalcFormulaSqls();
                this.MergeFormulaSqls();
                this.infoProgressBar.Value = 0;
                this.infoProgressBar.Maximum = this.formulas.Count;
                this.AddLog("total " + this.formulas.Count + " forumlas parsed");
                
                this.beginCalculateTime = DateTime.Now;

                for (int index = 0; index < TDFCalculateForm.threadNum; index++)
                {
                    this.threadsFinished[index] = false;
                }
                for (int index = 0; index < TDFCalculateForm.threadNum; index++)
                {
                    BackgroundWorker worker = workers[index];
                    worker.RunWorkerAsync(index);
                }

                this.watchThread.RunWorkerAsync();

                TDFactory.EndUpdate();
                this.AddLog("working thread start");
            }
            catch (Exception ex)
            {
                TDFactory.Util.ShowException(ex);
            }
            finally { }
        }

        private void stopButton_Click(object sender, EventArgs e)
        {
            for (int index = 0; index < TDFCalculateForm.threadNum; index++)
            {
                BackgroundWorker worker = workers[index];
                worker.CancelAsync();
            }
        }

        private void applyButton_Click(object sender, EventArgs e)
        {
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                if (activeWorksheet == null || this.lastWorkSheetName == "")
                {
                    this.infoTextBox.Text = "no active worksheet";
                    return;
                }

                if (this.lastWorkSheetName != activeWorksheet.Name)
                {
                    this.AddLog("active worksheet changed from " + this.lastWorkSheetName + " to " + activeWorksheet.Name);
                    return;
                }

                TDFactory.StartUpdate();

                int applyResults = 0;
                foreach (ListViewItem lvi in this.rawListView.Items)
                {
                    String address = lvi.SubItems[1].Text;
                    String value = lvi.SubItems[2].Text;
                    Range range = activeWorksheet.Range[address];
                    if (range == null) continue;
                    if (value == "-" || value == "failed" || value == "stopped") continue;
                    String formula = range.Formula;
                    range.Value2 = value;
                    range.Formula = formula;
                    applyResults++;
                }
                TDFactory.EndUpdate();
                
                this.AddLog("apply " + applyResults + " results to "  + this.lastWorkSheetName);
            }
            catch (Exception ex)
            {
                TDFactory.Util.ShowException(ex);
            }
            finally { }
        }

        private void refreshButton_Click(object sender, EventArgs e)
        {
            this.Start();
        }

        public int FillListView()
        {
            int totalCells = 0;
            
            try
            {
                Excel.Worksheet activeWorksheet = TDFactory.Application().ActiveSheet;
                if (activeWorksheet == null) return totalCells;

                Range usedRange = activeWorksheet.UsedRange;
                if (usedRange == null) return totalCells;

                this.rawListView.BeginUpdate();
                this.ClearListView();
                int colCount = usedRange.Columns.Count;
                int rowCount = usedRange.Rows.Count;
                this.formulas.Clear();
                for (int iCol = 0; iCol < colCount; ++iCol)
                {
                    for (int iRow = 0; iRow < rowCount; ++iRow)
                    {
                        Range range = activeWorksheet.Cells[iRow + usedRange.Row, iCol + usedRange.Column];
                        if (range == null) continue;
                        if (range.Formula == null) continue;
                        if (!(range.Formula is String)) continue;

                        String forumla = range.Formula as String;
                        if (!forumla.StartsWith("=TD")) continue;

                        totalCells++;
                        ListViewItem lvi = new ListViewItem();
                        lvi.Text = totalCells.ToString();
                        lvi.SubItems.Add(range.Address);
                        lvi.SubItems.Add("-");
                        this.rawListView.Items.Add(lvi);
                        this.formulas.Add(new TDExcelFormula(forumla));
                    }
                }
                this.rawListView.EndUpdate();

                return totalCells;
            }
            catch (Exception e)
            {
                TDFactory.Util.ShowException(e);
            }
            finally { }

            return totalCells;
        }

        private void ClearListView()
        {
            this.rawListView.Clear();

            ColumnHeader ch0 = new ColumnHeader();
            ch0.Text = "No";
            ch0.Width = 55;
            ch0.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch0);

            ColumnHeader ch1 = new ColumnHeader();
            ch1.Text = "Cell";
            ch1.Width = 85;
            ch1.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch1);

            //ColumnHeader ch2 = new ColumnHeader();
            //ch2.Text = "Current";
            //ch2.Width = 100;
            //ch2.TextAlign = HorizontalAlignment.Left;
            //this.rawListView.Columns.Add(ch2);

            ColumnHeader ch3 = new ColumnHeader();
            ch3.Text = "Calculate";
            ch3.Width = 260;
            ch3.TextAlign = HorizontalAlignment.Left;
            this.rawListView.Columns.Add(ch3);

            this.rawListView.GridLines = true;
            this.rawListView.FullRowSelect = true;
            this.rawListView.HeaderStyle = ColumnHeaderStyle.Clickable;
        }

        private void AddLog(String log)
        {
            this.infoTextBox.AppendText(DateTime.Now.ToString("HH:mm:ss") + " " + log + "\r\n");
            this.infoTextBox.Select(this.infoTextBox.TextLength, 0);
        }

        private void ClearLog()
        {
            this.infoTextBox.Clear();
        }

        private void InitializeComponent()
        {
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.label3 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.tableLayoutPanel3 = new System.Windows.Forms.TableLayoutPanel();
            this.stopButton = new System.Windows.Forms.Button();
            this.startButton = new System.Windows.Forms.Button();
            this.applyButton = new System.Windows.Forms.Button();
            this.refreshButton = new System.Windows.Forms.Button();
            this.label7 = new System.Windows.Forms.Label();
            this.rawListView = new System.Windows.Forms.ListView();
            this.infoProgressBar = new System.Windows.Forms.ProgressBar();
            this.infoTextBox = new System.Windows.Forms.RichTextBox();
            this.tableLayoutPanel1.SuspendLayout();
            this.tableLayoutPanel3.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.BackColor = System.Drawing.SystemColors.Control;
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.label3, 0, 5);
            this.tableLayoutPanel1.Controls.Add(this.label1, 0, 3);
            this.tableLayoutPanel1.Controls.Add(this.label2, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.label4, 0, 7);
            this.tableLayoutPanel1.Controls.Add(this.tableLayoutPanel3, 0, 8);
            this.tableLayoutPanel1.Controls.Add(this.label7, 0, 0);
            this.tableLayoutPanel1.Controls.Add(this.rawListView, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.infoProgressBar, 0, 6);
            this.tableLayoutPanel1.Controls.Add(this.infoTextBox, 0, 4);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.Padding = new System.Windows.Forms.Padding(15);
            this.tableLayoutPanel1.RowCount = 10;
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
            this.tableLayoutPanel1.Size = new System.Drawing.Size(500, 700);
            this.tableLayoutPanel1.TabIndex = 4;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label3.Location = new System.Drawing.Point(18, 454);
            this.label3.Margin = new System.Windows.Forms.Padding(3);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(464, 22);
            this.label3.TabIndex = 19;
            this.label3.Text = "progress";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label1.Location = new System.Drawing.Point(18, 280);
            this.label1.Margin = new System.Windows.Forms.Padding(3);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(464, 22);
            this.label1.TabIndex = 3;
            this.label1.Text = "information";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label2.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label2.Location = new System.Drawing.Point(18, 252);
            this.label2.Margin = new System.Windows.Forms.Padding(3);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(464, 22);
            this.label2.TabIndex = 6;
            this.label2.Text = "Output";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label4.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label4.Location = new System.Drawing.Point(18, 511);
            this.label4.Margin = new System.Windows.Forms.Padding(3);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(464, 22);
            this.label4.TabIndex = 8;
            this.label4.Text = "Operation";
            // 
            // tableLayoutPanel3
            // 
            this.tableLayoutPanel3.ColumnCount = 4;
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel3.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 25F));
            this.tableLayoutPanel3.Controls.Add(this.stopButton, 1, 0);
            this.tableLayoutPanel3.Controls.Add(this.startButton, 0, 0);
            this.tableLayoutPanel3.Controls.Add(this.applyButton, 2, 0);
            this.tableLayoutPanel3.Controls.Add(this.refreshButton, 3, 0);
            this.tableLayoutPanel3.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel3.Location = new System.Drawing.Point(18, 539);
            this.tableLayoutPanel3.Margin = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.tableLayoutPanel3.Name = "tableLayoutPanel3";
            this.tableLayoutPanel3.RowCount = 1;
            this.tableLayoutPanel3.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel3.Size = new System.Drawing.Size(467, 35);
            this.tableLayoutPanel3.TabIndex = 9;
            // 
            // stopButton
            // 
            this.stopButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.stopButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.stopButton.Location = new System.Drawing.Point(119, 3);
            this.stopButton.Margin = new System.Windows.Forms.Padding(3, 3, 0, 3);
            this.stopButton.Name = "stopButton";
            this.stopButton.Size = new System.Drawing.Size(113, 29);
            this.stopButton.TabIndex = 0;
            this.stopButton.Text = "Stop";
            this.stopButton.UseVisualStyleBackColor = true;
            this.stopButton.Click += new System.EventHandler(this.stopButton_Click);
            // 
            // startButton
            // 
            this.startButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.startButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.startButton.Location = new System.Drawing.Point(3, 3);
            this.startButton.Name = "startButton";
            this.startButton.Size = new System.Drawing.Size(110, 29);
            this.startButton.TabIndex = 1;
            this.startButton.Text = "Start";
            this.startButton.UseVisualStyleBackColor = true;
            this.startButton.Click += new System.EventHandler(this.startButton_Click);
            // 
            // applyButton
            // 
            this.applyButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.applyButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.applyButton.Location = new System.Drawing.Point(235, 3);
            this.applyButton.Name = "applyButton";
            this.applyButton.Size = new System.Drawing.Size(110, 29);
            this.applyButton.TabIndex = 2;
            this.applyButton.Text = "Apply";
            this.applyButton.UseVisualStyleBackColor = true;
            this.applyButton.Click += new System.EventHandler(this.applyButton_Click);
            // 
            // refreshButton
            // 
            this.refreshButton.Dock = System.Windows.Forms.DockStyle.Fill;
            this.refreshButton.FlatStyle = System.Windows.Forms.FlatStyle.Popup;
            this.refreshButton.Location = new System.Drawing.Point(351, 3);
            this.refreshButton.Name = "refreshButton";
            this.refreshButton.Size = new System.Drawing.Size(113, 29);
            this.refreshButton.TabIndex = 3;
            this.refreshButton.Text = "Refresh";
            this.refreshButton.UseVisualStyleBackColor = true;
            this.refreshButton.Click += new System.EventHandler(this.refreshButton_Click);
            // 
            // label7
            // 
            this.label7.AutoSize = true;
            this.label7.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label7.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label7.Location = new System.Drawing.Point(18, 18);
            this.label7.Margin = new System.Windows.Forms.Padding(3);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(464, 22);
            this.label7.TabIndex = 16;
            this.label7.Text = "Columns contain TDengine Formula";
            // 
            // rawListView
            // 
            this.rawListView.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle;
            this.rawListView.Dock = System.Windows.Forms.DockStyle.Fill;
            this.rawListView.Location = new System.Drawing.Point(18, 46);
            this.rawListView.Name = "rawListView";
            this.rawListView.Size = new System.Drawing.Size(464, 200);
            this.rawListView.TabIndex = 17;
            this.rawListView.UseCompatibleStateImageBehavior = false;
            this.rawListView.View = System.Windows.Forms.View.Details;
            // 
            // infoProgressBar
            // 
            this.infoProgressBar.Dock = System.Windows.Forms.DockStyle.Fill;
            this.infoProgressBar.Location = new System.Drawing.Point(18, 482);
            this.infoProgressBar.Name = "infoProgressBar";
            this.infoProgressBar.Size = new System.Drawing.Size(464, 23);
            this.infoProgressBar.TabIndex = 20;
            // 
            // infoTextBox
            // 
            this.infoTextBox.Dock = System.Windows.Forms.DockStyle.Fill;
            this.infoTextBox.Location = new System.Drawing.Point(18, 308);
            this.infoTextBox.Name = "infoTextBox";
            this.infoTextBox.Size = new System.Drawing.Size(464, 140);
            this.infoTextBox.TabIndex = 21;
            this.infoTextBox.Text = "";
            // 
            // TDFCalculateForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 22F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoScroll = true;
            this.AutoScrollMinSize = new System.Drawing.Size(400, 700);
            this.BackColor = System.Drawing.SystemColors.Control;
            this.Controls.Add(this.tableLayoutPanel1);
            this.DoubleBuffered = true;
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(3, 4, 3, 4);
            this.Name = "TDFCalculateForm";
            this.Size = new System.Drawing.Size(500, 700);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.tableLayoutPanel3.ResumeLayout(false);
            this.ResumeLayout(false);

        }
    }
}
