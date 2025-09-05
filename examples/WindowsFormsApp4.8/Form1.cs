using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace WindowsFormsApp4._8
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private ITDengineClient client;

        private void Close(object sender, EventArgs e)
        {
            if (client != null)
            {
                client.Dispose();
            }
            MessageBox.Show("closed");
            button_show.Enabled = false;
            button_connect.Enabled = true;
            button_close.Enabled = false;
        }

        private void ShowDatabase(object sender, EventArgs e)
        {
            var query = "show databases";
            var rowData = new List<string>();
            using (var rows = client.Query(query))
            {
                while (rows.Read())
                {
                    for (int i = 0; i < rows.FieldCount; i++)
                    {
                        rowData.Add(Encoding.UTF8.GetString((byte[])rows.GetValue(i)));
                    }
                }
            }

            MessageBox.Show(string.Join("\n", rowData));
        }

        private void Connect(object sender, EventArgs e)
        {
            var builder = new ConnectionStringBuilder(
                "protocol=WebSocket;host=localhost;port=6041;useSSL=false;username=root;password=taosdata");
            client = DbDriver.Open(builder);
            MessageBox.Show("connected");
            button_show.Enabled = true;
            button_connect.Enabled = false;
            button_close.Enabled = true;
        }
    }
}