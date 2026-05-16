using OSIsoft.AF;
using OSIsoft.AF.Asset;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using TDEngineDR.TDEngineClient;

namespace TDEngineDR
{
    public partial class ConfigStringEditor : Form
    {
        private TDEngineDataReference dataReference = null;
        private ConfigStringInfo configStringInfo;
        private List<AFElement> tdServerElements;
        private List<string> tdEngineServerNames;
        private TDEngineHttpClient tdEngineHttpClient;

        public ConfigStringEditor(TDEngineDataReference dataReference, bool bReadOnly)
        {
            InitializeComponent();
            rbPIPoint.Select();
            rbPIPoint_CheckedChanged(null, null);
            this.dataReference = dataReference;
            this.LoadTDEngineServerNames();
            this.cbServer.Items.AddRange(this.tdEngineServerNames.ToArray());
            if (!string.IsNullOrEmpty(this.dataReference.ConfigString))
            {
                this.configStringInfo = new ConfigStringInfo(this.dataReference.ConfigString);
                cbServer.Text = configStringInfo.Server;
                tbDatabase.Text = configStringInfo.Database;

                if (!string.IsNullOrEmpty(this.configStringInfo.Table) && !string.IsNullOrEmpty(this.configStringInfo.Column))
                {
                    tbTable.Text = configStringInfo.Table;
                    tbColumn.Text = configStringInfo.Column;
                    rbTable.Select();
                    rbTable_CheckedChanged(null, null);            
                }
                if (!string.IsNullOrEmpty(this.configStringInfo.Element) && !string.IsNullOrEmpty(this.configStringInfo.Attribute))
                {
                    tbTable.Text = configStringInfo.Element;
                    tbColumn.Text = configStringInfo.Attribute;
                    rbAFElement.Select();
                    rbAFAttribute_CheckedChanged(null, null);

                }
                if (!string.IsNullOrEmpty(this.configStringInfo.Point))
                {
                    tbPoint.Text = configStringInfo.Point;
                }
            }
            else
            {
                this.configStringInfo = new ConfigStringInfo();
                tbDatabase.Text = "pi";
            }
          

        }

        private void LoadTDEngineServerNames()
        {
            PISystem piSystem = this.dataReference.PISystem;
            this.tdServerElements = TDEngineServerManager.GetTDEngineServerElements(piSystem);
            this.tdEngineServerNames = tdServerElements.Select(s => s.Name).ToList();
        }

        private bool SaveConfigString()
        {
            try
            {
                this.configStringInfo.Server = cbServer.Text;
                this.configStringInfo.Database = tbDatabase.Text;
                this.configStringInfo.Point = string.Empty;
                this.configStringInfo.Column = string.Empty;
                this.configStringInfo.Table = string.Empty;
                this.configStringInfo.Element = string.Empty;
                this.configStringInfo.Attribute = string.Empty;


                if (rbPIPoint.Checked)
                {
                    this.configStringInfo.Point = tbPoint.Text;
                }

                if (rbAFElement.Checked)
                {
                    this.configStringInfo.Attribute = tbColumn.Text;
                    this.configStringInfo.Element = tbTable.Text;
                }

                if (rbTable.Checked)
                {
                    this.configStringInfo.Column = tbColumn.Text;
                    this.configStringInfo.Table = tbTable.Text;
                }


                string configString = this.configStringInfo.ToString();
                if (!string.IsNullOrEmpty(configString) && !string.IsNullOrEmpty(this.configStringInfo.Server) && !string.IsNullOrEmpty(this.configStringInfo.Database))
                {
                    if (!IsTDEngineServerValid(this.configStringInfo.Server))
                    {
                        MessageBox.Show($"Could not connect to TDengine Server {this.configStringInfo.Server}");
                        return false;
                    }
                    else if (!string.IsNullOrEmpty(this.configStringInfo.Point) && !IsTDEnginePIPointTableValid(this.configStringInfo.Database, this.configStringInfo.Point))
                    {
                        MessageBox.Show($"Table {this.configStringInfo.Database}.{this.configStringInfo.Point} does not exist.", "Error!");
                        return false;
                    }
                    else if (!string.IsNullOrEmpty(this.configStringInfo.Element) && !IsTDEngineAFElementTableValid(this.configStringInfo.Database, this.configStringInfo.Element, this.configStringInfo.Attribute))
                    {
                        MessageBox.Show($"Table {this.configStringInfo.Database}.{this.configStringInfo.Element} or attribute {this.configStringInfo.Attribute} does not exist.", "Error!");
                        return false;
                    }
                    else if (!string.IsNullOrEmpty(this.configStringInfo.Table) && !IsTDEngineTableValid(this.configStringInfo.Database, this.configStringInfo.Table, this.configStringInfo.Column))
                    {
                        MessageBox.Show($"Table {this.configStringInfo.Database}.{this.configStringInfo.Table} or column {this.configStringInfo.Column} does not exist.", "Error!");
                        return false;
                    }
                    else
                    {
                        dataReference.ConfigString = this.configStringInfo.ToString();
                    }
                }
                else
                {
                    MessageBox.Show("Could not save the Config String! Make sure all properties are filled.", "Error!");
                    return false;
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                return false;
            }
            return true;

        }



        private bool IsTDEngineTableValid(string database, string table, string column)
        {
            try
            {
                TDPIStream piStream = this.tdEngineHttpClient.GetTDPIStreamFromTable(database, table, column);
                var value = piStream.GetSnapshotValue();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }


        private bool IsTDEngineAFElementTableValid(string databaseName, string element, string attribute)
        {
            try
            {
                TDPIStream piStream = this.tdEngineHttpClient.GetTDPIStreamFromAF(databaseName, element, attribute);
                var value = piStream.GetSnapshotValue();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private bool IsTDEnginePIPointTableValid(string database, string point)
        {
            try
            {
                TDPIStream piStream = this.tdEngineHttpClient.GetTDPIStreamFromPI(database, point);
                var value = piStream.GetSnapshotValue();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private bool IsTDEngineServerValid(string serverName)
        {
            this.tdEngineHttpClient = TDEngineServerManager.GetTDEngineClient(serverName, this.dataReference.PISystem);

            try
            {
                var version = this.tdEngineHttpClient.GetServerVersion();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            if (!SaveConfigString())
            {
                DialogResult = DialogResult.None;
            }
            else
            {
                this.Close();
            }
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        private void btnCreateTDEngine_Click(object sender, EventArgs e)
        {
            CreateTableForm createTableForm = new CreateTableForm(this.dataReference, this);
            createTableForm.Show();

        }

        private void rbPIPoint_CheckedChanged(object sender, EventArgs e)
        {
            lbTable.Visible = false;
            tbTable.Visible = false;
            lbColumn.Visible = false;
            tbColumn.Visible = false;
            tbPoint.Visible = true;
            lbPoint.Visible = true;
            btnCreateTDEngine.Visible = true;

        }

        private void rbAFAttribute_CheckedChanged(object sender, EventArgs e)
        {
            lbTable.Text = "Element";
            lbColumn.Text = "Attribute";
            lbTable.Visible = true;
            tbTable.Visible = true;
            lbColumn.Visible = true;
            tbColumn.Visible = true;
            tbPoint.Visible = false;
            lbPoint.Visible = false;
            btnCreateTDEngine.Visible = false;

        }

        private void rbTable_CheckedChanged(object sender, EventArgs e)
        {
            lbTable.Text = "Table";
            lbColumn.Text = "Column";
            lbTable.Visible = true;
            tbTable.Visible = true;
            lbColumn.Visible = true;
            tbColumn.Visible = true;
            tbPoint.Visible = false;
            lbPoint.Visible = false;
            btnCreateTDEngine.Visible = false;
        }
    }
}
