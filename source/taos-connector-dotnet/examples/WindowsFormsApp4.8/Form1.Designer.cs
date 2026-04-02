namespace WindowsFormsApp4._8
{
    partial class Form1
    {
        /// <summary>
        /// 必需的设计器变量。
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// 清理所有正在使用的资源。
        /// </summary>
        /// <param name="disposing">如果应释放托管资源，为 true；否则为 false。</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows 窗体设计器生成的代码

        /// <summary>
        /// 设计器支持所需的方法 - 不要修改
        /// 使用代码编辑器修改此方法的内容。
        /// </summary>
        private void InitializeComponent()
        {
            this.button_close = new System.Windows.Forms.Button();
            this.button_show = new System.Windows.Forms.Button();
            this.button_connect = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // button_close
            // 
            this.button_close.Enabled = false;
            this.button_close.Location = new System.Drawing.Point(212, 325);
            this.button_close.Name = "button_close";
            this.button_close.Size = new System.Drawing.Size(294, 113);
            this.button_close.TabIndex = 0;
            this.button_close.Text = "Close";
            this.button_close.UseVisualStyleBackColor = true;
            this.button_close.Click += new System.EventHandler(this.Close);
            // 
            // button_show
            // 
            this.button_show.Enabled = false;
            this.button_show.Location = new System.Drawing.Point(212, 166);
            this.button_show.Name = "button_show";
            this.button_show.Size = new System.Drawing.Size(294, 113);
            this.button_show.TabIndex = 1;
            this.button_show.Text = "Show DB";
            this.button_show.UseVisualStyleBackColor = true;
            this.button_show.Click += new System.EventHandler(this.ShowDatabase);
            // 
            // button_connect
            // 
            this.button_connect.Location = new System.Drawing.Point(212, 8);
            this.button_connect.Name = "button_connect";
            this.button_connect.Size = new System.Drawing.Size(294, 113);
            this.button_connect.TabIndex = 2;
            this.button_connect.Text = "Connect";
            this.button_connect.UseVisualStyleBackColor = true;
            this.button_connect.Click += new System.EventHandler(this.Connect);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.button_connect);
            this.Controls.Add(this.button_show);
            this.Controls.Add(this.button_close);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button button_close;
        private System.Windows.Forms.Button button_show;
        private System.Windows.Forms.Button button_connect;
    }
}

