using System.Windows.Forms;
using System.Runtime.InteropServices;

namespace TDengineExcelPlugins
{
    /// <summary>
    /// 任务窗格使用的自定义窗体控件
    /// </summary>
    [ComVisible(true)]
    public class TDAboutForm : TDControl
    {
        private TableLayoutPanel tableLayoutPanel1;
        private Label serverVersionLable;
        private Label clientVersionLabel;
        private Label emailLabel;
        private Label label1;
        public Label TheLabel;
        public TDAboutForm()
        {
            this.DoubleBuffered = true;
            this.SetStyle(ControlStyles.UserPaint, true);
            this.SetStyle(ControlStyles.AllPaintingInWmPaint, true);
            this.SetStyle(ControlStyles.DoubleBuffer, true);
            InitializeComponent();
        }

        private void InitializeComponent()
        {
            this.tableLayoutPanel1 = new System.Windows.Forms.TableLayoutPanel();
            this.serverVersionLable = new System.Windows.Forms.Label();
            this.clientVersionLabel = new System.Windows.Forms.Label();
            this.emailLabel = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.tableLayoutPanel1.SuspendLayout();
            this.SuspendLayout();
            // 
            // tableLayoutPanel1
            // 
            this.tableLayoutPanel1.ColumnCount = 1;
            this.tableLayoutPanel1.ColumnStyles.Add(new System.Windows.Forms.ColumnStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Controls.Add(this.serverVersionLable, 0, 1);
            this.tableLayoutPanel1.Controls.Add(this.clientVersionLabel, 0, 2);
            this.tableLayoutPanel1.Controls.Add(this.emailLabel, 0, 3);
            this.tableLayoutPanel1.Controls.Add(this.label1, 0, 0);
            this.tableLayoutPanel1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.tableLayoutPanel1.Location = new System.Drawing.Point(0, 0);
            this.tableLayoutPanel1.Name = "tableLayoutPanel1";
            this.tableLayoutPanel1.Padding = new System.Windows.Forms.Padding(15);
            this.tableLayoutPanel1.RowCount = 5;
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle());
            this.tableLayoutPanel1.RowStyles.Add(new System.Windows.Forms.RowStyle(System.Windows.Forms.SizeType.Percent, 100F));
            this.tableLayoutPanel1.Size = new System.Drawing.Size(500, 300);
            this.tableLayoutPanel1.TabIndex = 11;
            // 
            // serverVersionLable
            // 
            this.serverVersionLable.AutoSize = true;
            this.serverVersionLable.Dock = System.Windows.Forms.DockStyle.Fill;
            this.serverVersionLable.Location = new System.Drawing.Point(18, 46);
            this.serverVersionLable.Margin = new System.Windows.Forms.Padding(3);
            this.serverVersionLable.Name = "serverVersionLable";
            this.serverVersionLable.Size = new System.Drawing.Size(464, 22);
            this.serverVersionLable.TabIndex = 0;
            this.serverVersionLable.Text = "TDengine Version: 1.*.*";
            // 
            // clientVersionLabel
            // 
            this.clientVersionLabel.AutoSize = true;
            this.clientVersionLabel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.clientVersionLabel.Location = new System.Drawing.Point(18, 74);
            this.clientVersionLabel.Margin = new System.Windows.Forms.Padding(3);
            this.clientVersionLabel.Name = "clientVersionLabel";
            this.clientVersionLabel.Size = new System.Drawing.Size(464, 22);
            this.clientVersionLabel.TabIndex = 1;
            this.clientVersionLabel.Text = "Plugin Version: 1.0";
            // 
            // emailLabel
            // 
            this.emailLabel.AutoSize = true;
            this.emailLabel.Dock = System.Windows.Forms.DockStyle.Fill;
            this.emailLabel.Location = new System.Drawing.Point(18, 102);
            this.emailLabel.Margin = new System.Windows.Forms.Padding(3);
            this.emailLabel.Name = "emailLabel";
            this.emailLabel.Size = new System.Drawing.Size(464, 22);
            this.emailLabel.TabIndex = 2;
            this.emailLabel.Text = "Email: support@taosdata.com";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.BackColor = System.Drawing.SystemColors.InactiveCaption;
            this.label1.Dock = System.Windows.Forms.DockStyle.Fill;
            this.label1.Location = new System.Drawing.Point(18, 18);
            this.label1.Margin = new System.Windows.Forms.Padding(3);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(464, 22);
            this.label1.TabIndex = 3;
            this.label1.Text = "Information";
            // 
            // TDAboutForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(10F, 22F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.AutoScroll = true;
            this.AutoScrollMinSize = new System.Drawing.Size(400, 300);
            this.BackColor = System.Drawing.SystemColors.Control;
            this.Controls.Add(this.tableLayoutPanel1);
            this.Font = new System.Drawing.Font("Consolas", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.Name = "TDAboutForm";
            this.Size = new System.Drawing.Size(500, 300);
            this.tableLayoutPanel1.ResumeLayout(false);
            this.tableLayoutPanel1.PerformLayout();
            this.ResumeLayout(false);

        }
    }
}
