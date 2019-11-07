namespace excel2010
{
    partial class design : Microsoft.Office.Tools.Ribbon.RibbonBase
    {
        /// <summary>
        /// 必需的设计器变量。
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        public design()
            : base(Globals.Factory.GetRibbonFactory())
        {
            InitializeComponent();
        }

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

        #region 组件设计器生成的代码

        /// <summary>
        /// 设计器支持所需的方法 - 不要修改
        /// 使用代码编辑器修改此方法的内容。
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(design));
            this.mainTab = this.Factory.CreateRibbonTab();
            this.metaGroup = this.Factory.CreateRibbonGroup();
            this.dataGroup = this.Factory.CreateRibbonGroup();
            this.othersGroup = this.Factory.CreateRibbonGroup();
            this.colorDialog1 = new System.Windows.Forms.ColorDialog();
            this.stablesButton = this.Factory.CreateRibbonButton();
            this.tablesButton = this.Factory.CreateRibbonButton();
            this.aggButton = this.Factory.CreateRibbonButton();
            this.detailButton = this.Factory.CreateRibbonButton();
            this.sliceButton = this.Factory.CreateRibbonButton();
            this.connectButton = this.Factory.CreateRibbonButton();
            this.aboutButton = this.Factory.CreateRibbonButton();
            this.mainTab.SuspendLayout();
            this.metaGroup.SuspendLayout();
            this.dataGroup.SuspendLayout();
            this.othersGroup.SuspendLayout();
            this.SuspendLayout();
            // 
            // mainTab
            // 
            this.mainTab.ControlId.ControlIdType = Microsoft.Office.Tools.Ribbon.RibbonControlIdType.Office;
            this.mainTab.Groups.Add(this.metaGroup);
            this.mainTab.Groups.Add(this.dataGroup);
            this.mainTab.Groups.Add(this.othersGroup);
            this.mainTab.Label = "TDengine";
            this.mainTab.Name = "mainTab";
            // 
            // metaGroup
            // 
            this.metaGroup.Items.Add(this.stablesButton);
            this.metaGroup.Items.Add(this.tablesButton);
            this.metaGroup.Label = "meta";
            this.metaGroup.Name = "metaGroup";
            // 
            // dataGroup
            // 
            this.dataGroup.Items.Add(this.aggButton);
            this.dataGroup.Items.Add(this.detailButton);
            this.dataGroup.Items.Add(this.sliceButton);
            this.dataGroup.Label = "data";
            this.dataGroup.Name = "dataGroup";
            // 
            // othersGroup
            // 
            this.othersGroup.Items.Add(this.connectButton);
            this.othersGroup.Items.Add(this.aboutButton);
            this.othersGroup.Label = "others";
            this.othersGroup.Name = "othersGroup";
            // 
            // stablesButton
            // 
            this.stablesButton.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge;
            this.stablesButton.Image = ((System.Drawing.Image)(resources.GetObject("stablesButton.Image")));
            this.stablesButton.Label = "Query STables";
            this.stablesButton.Name = "stablesButton";
            this.stablesButton.ShowImage = true;
            this.stablesButton.Click += new Microsoft.Office.Tools.Ribbon.RibbonControlEventHandler(this.StablesButton_Click);
            // 
            // tablesButton
            // 
            this.tablesButton.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge;
            this.tablesButton.Image = ((System.Drawing.Image)(resources.GetObject("tablesButton.Image")));
            this.tablesButton.Label = "Query Tables";
            this.tablesButton.Name = "tablesButton";
            this.tablesButton.ShowImage = true;
            this.tablesButton.Click += new Microsoft.Office.Tools.Ribbon.RibbonControlEventHandler(this.TablesButton_Click);
            // 
            // aggButton
            // 
            this.aggButton.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge;
            this.aggButton.Image = ((System.Drawing.Image)(resources.GetObject("aggButton.Image")));
            this.aggButton.Label = "Aggregation";
            this.aggButton.Name = "aggButton";
            this.aggButton.ShowImage = true;
            this.aggButton.Click += new Microsoft.Office.Tools.Ribbon.RibbonControlEventHandler(this.AggButton_Click);
            // 
            // detailButton
            // 
            this.detailButton.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge;
            this.detailButton.Image = ((System.Drawing.Image)(resources.GetObject("detailButton.Image")));
            this.detailButton.Label = "Detail Data Query ";
            this.detailButton.Name = "detailButton";
            this.detailButton.ShowImage = true;
            this.detailButton.Click += new Microsoft.Office.Tools.Ribbon.RibbonControlEventHandler(this.DetailButton_Click);
            // 
            // sliceButton
            // 
            this.sliceButton.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge;
            this.sliceButton.Image = ((System.Drawing.Image)(resources.GetObject("sliceButton.Image")));
            this.sliceButton.Label = "Slice Data Query";
            this.sliceButton.Name = "sliceButton";
            this.sliceButton.ShowImage = true;
            this.sliceButton.Click += new Microsoft.Office.Tools.Ribbon.RibbonControlEventHandler(this.SliceButton_Click);
            // 
            // connectButton
            // 
            this.connectButton.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge;
            this.connectButton.Image = ((System.Drawing.Image)(resources.GetObject("connectButton.Image")));
            this.connectButton.Label = "Connect TDengine";
            this.connectButton.Name = "connectButton";
            this.connectButton.ShowImage = true;
            this.connectButton.Click += new Microsoft.Office.Tools.Ribbon.RibbonControlEventHandler(this.ConnectButton_Click);
            // 
            // aboutButton
            // 
            this.aboutButton.ControlSize = Microsoft.Office.Core.RibbonControlSize.RibbonControlSizeLarge;
            this.aboutButton.Image = ((System.Drawing.Image)(resources.GetObject("aboutButton.Image")));
            this.aboutButton.Label = "Contact US";
            this.aboutButton.Name = "aboutButton";
            this.aboutButton.ShowImage = true;
            this.aboutButton.Click += new Microsoft.Office.Tools.Ribbon.RibbonControlEventHandler(this.AboutButton_Click);
            // 
            // design
            // 
            this.Name = "design";
            this.RibbonType = "Microsoft.Excel.Workbook";
            this.Tabs.Add(this.mainTab);
            this.Load += new Microsoft.Office.Tools.Ribbon.RibbonUIEventHandler(this.Ribbon1_Load);
            this.mainTab.ResumeLayout(false);
            this.mainTab.PerformLayout();
            this.metaGroup.ResumeLayout(false);
            this.metaGroup.PerformLayout();
            this.dataGroup.ResumeLayout(false);
            this.dataGroup.PerformLayout();
            this.othersGroup.ResumeLayout(false);
            this.othersGroup.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        internal Microsoft.Office.Tools.Ribbon.RibbonTab mainTab;
        internal Microsoft.Office.Tools.Ribbon.RibbonGroup othersGroup;
        internal Microsoft.Office.Tools.Ribbon.RibbonButton connectButton;
        internal Microsoft.Office.Tools.Ribbon.RibbonGroup metaGroup;
        internal Microsoft.Office.Tools.Ribbon.RibbonButton stablesButton;
        internal Microsoft.Office.Tools.Ribbon.RibbonButton tablesButton;
        internal Microsoft.Office.Tools.Ribbon.RibbonGroup dataGroup;
        internal Microsoft.Office.Tools.Ribbon.RibbonButton aggButton;
        internal Microsoft.Office.Tools.Ribbon.RibbonButton detailButton;
        private System.Windows.Forms.ColorDialog colorDialog1;
        internal Microsoft.Office.Tools.Ribbon.RibbonButton sliceButton;
        internal Microsoft.Office.Tools.Ribbon.RibbonButton aboutButton;
    }

    partial class ThisRibbonCollection
    {
        internal design Ribbon1
        {
            get { return this.GetRibbon<design>(); }
        }
    }
}
