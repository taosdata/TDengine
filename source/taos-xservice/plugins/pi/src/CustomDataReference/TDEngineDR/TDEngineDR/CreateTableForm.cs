using System;
using System.Collections.Generic;
using System.Windows.Forms;
using TDEngineDR.TDEngineClient;
using TDEngineDR.TDEngineClient.Models;

namespace TDEngineDR
{
    public partial class CreateTableForm : Form
    {
        private TDEngineDataReference dataReference;
        private ConfigStringEditor configStringEditor;
        private TDEngineHttpClient tdEngineHttpClient;

        public CreateTableForm(TDEngineDataReference dataReference, ConfigStringEditor configStringEditor)
        {
            InitializeComponent();
            this.dataReference = dataReference;
            this.configStringEditor = configStringEditor;
            for (int i = 0; i < configStringEditor.cbServer.Items.Count; i++)
            {
                this.cbServer.Items.Add(configStringEditor.cbServer.Items[i]);
            }
            cbServer.Text = configStringEditor.cbServer.Text;
            tbDatabase.Text = configStringEditor.tbDatabase.Text;
            tbPointName.Text = configStringEditor.tbPoint.Text;

            List<string> pointTypeOptions = new List<string>()
            {
                "Digital", "Int16", "Int32", "Int64", "Float16", "Float32", "Float64", "String", "Timestamp"
            };
            cbPointType.Items.AddRange(pointTypeOptions.ToArray());
            cbPointType.Text = "Float32";

        }

        private void btnOK_Click(object sender, EventArgs e)
        {
            try
            {
                string tdEngineServerName = cbServer.Text;
                string databaseName = tbDatabase.Text;
                string pointName = tbPointName.Text;
                string pointType = cbPointType.Text;

                if (!string.IsNullOrEmpty(tdEngineServerName) && !string.IsNullOrEmpty(pointName))
                {
                    if (!IsTDEngineServerValid(tdEngineServerName))
                    {
                        MessageBox.Show($"Could not connect to TDengine Server {tdEngineServerName}");
                    }
                    else if (IsTDEngineTableValid(databaseName, pointName))
                    {
                        MessageBox.Show($"Table {pointName} already exist.", "Error!");
                    }
                    else
                    {
                        try
                        {
                            CreateTable(tdEngineServerName, databaseName, pointName, pointType);
                            MessageBox.Show($"Table {pointName} created successfully.", "Success!");
                            configStringEditor.tbPoint.Text = pointName;
                            try
                            {
                                this.dataReference.Attribute.Type = AttributeTypeConverter.Convert(pointType);
                                this.dataReference.Database.CheckIn();
                            }
                            catch(Exception)
                            {

                            }
                            this.Close();
                        }
                        catch(Exception ex)
                        {
                            MessageBox.Show($"Could not create TDengine table! {ex.Message}", "Error!");
                        }
                    }
                }
                else
                {
                    MessageBox.Show("Could not create TDengine table! Make sure all properties are filled.", "Error!");
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);

            }

        }

        private void CreateTable(string server, string database, string pointName, string pointType)
        {
            this.tdEngineHttpClient = TDEngineServerManager.GetTDEngineClient(server, this.dataReference.PISystem);
            string tdColumnType = PointTypeConverter.Convert(pointType);
            string superTableName = $"pitag_{tdColumnType.ToLower()}";
            this.tdEngineHttpClient.CreateSuperTableForPIPoint(database, superTableName, tdColumnType);
            this.tdEngineHttpClient.CreateTableForPIPoint(database, pointName, superTableName);
            var tdPoint = this.tdEngineHttpClient.GetTDPIStreamFromPI(database, pointName);
            tdPoint.UpdateValue(new TDValue(0, DateTime.Now, 253, TDValueType.None));
        }

        private bool IsTDEngineTableValid(string databaseName, string pointName)
        {
            try
            {
                TDPIStream point = this.tdEngineHttpClient.GetTDPIStreamFromPI(databaseName, pointName);
                var value = point.GetSnapshotValue();
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

        private void btnCancel_Click(object sender, EventArgs e)
        {
            this.Close();
        }
    }
}
