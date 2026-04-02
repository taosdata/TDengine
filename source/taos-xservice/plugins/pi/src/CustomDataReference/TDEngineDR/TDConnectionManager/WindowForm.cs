using OSIsoft.AF;
using OSIsoft.AF.Asset;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using TDConnectionManager.Models;
using TDEngineDR;

namespace TDConnectionManager
{
    public partial class WindowForm : Form
    {
        private PISystem piSystem;
        private AFElement tdEngineServerElement;
        private List<TDengineServer> TDengineServerList;
        private CreateOrEditTDengineForm createOrEditTDengineForm;


        internal TDengineServer TDengineServer { get; private set; }

        public WindowForm()
        {
            InitializeComponent();
            grTDServerInfo.Visible = false;
            btnAdd.Visible = false;
            btnEdit.Visible = false;
            btnDelete.Visible = false;
            lbTDServers.Visible = false;
            lbList.Visible = false;

        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            try
            {
                this.piSystem = this.piSystemPicker1.PISystem;
                this.piSystem.Connect();
                LoadData(false);
                btnAdd.Visible = true;
                btnEdit.Visible = true;
                btnDelete.Visible = true;
                lbTDServers.Visible = true;
                lbList.Visible = true;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Could not connect to the PI System. " + ex.Message, "Error");
            }
        }

        private void LoadData(bool throwExceptionIfNotFound)
        {
            this.piSystem.Refresh();
            lbTDServers.Items.Clear();
            AFDatabase dbConfig = this.piSystem.Databases["Configuration"];
            if (dbConfig == null)
            {
                if (throwExceptionIfNotFound)
                {
                    throw new Exception("Could not connect to the Configuration Database");
                }
            }
            AFElement tdEngineElement = dbConfig.Elements["TDengine"];
            if (tdEngineElement == null)
            {
                if (throwExceptionIfNotFound)
                {
                    throw new Exception("Could not connect to the TDengine root element on the Configuration Database");
                }
            }
            else
            {
                this.tdEngineServerElement = tdEngineElement.Elements["Servers"];
                this.TDengineServerList = new List<TDengineServer>();

                foreach (var element in tdEngineServerElement.Elements)
                {
                    TDengineServer TDengineServer = this.ConvertToTDengineServer(element);
                    TDengineServerList.Add(TDengineServer);
                    lbTDServers.Items.Add(TDengineServer.Name);
                }
            }
        }

        private void lbTDServers_SelectedIndexChanged(object sender, EventArgs e)
        {
            ListBox listBox = (ListBox)sender;
            if (listBox.SelectedItem == null)
            {
                return;
            }
            this.TDengineServer = this.TDengineServerList.Where(t => t.Name == listBox.SelectedItem.ToString()).Single();
            lbName.Text = "Name: " + TDengineServer.Name;
            lbHost.Text = "Host: " + TDengineServer.Host;
            lbPort.Text = "Port: " + TDengineServer.Port.ToString();
            if (TDengineServer.IsCloud)
            {
                lbToken.Text = "Token: " + TDengineServer.Token.Substring(0, 4) + "***********";
                lbUser.Visible = false;
                lbPassword.Visible = false;
                lbToken.Visible = true;
            }
            else
            {
                lbUser.Text = "Username: " + TDengineServer.Username;
                lbPassword.Text = "Password: ***********";
                lbUser.Visible = true;
                lbPassword.Visible = true;
                lbToken.Visible = false;
            }
            grTDServerInfo.Visible = true;
        }

        private TDengineServer ConvertToTDengineServer(AFElement element)
        {
            TDengineServer tdEngineServer = new TDengineServer();
            tdEngineServer.Element = element;
            tdEngineServer.Name = element.Name;
            tdEngineServer.Host = element.Attributes["Host"].GetValue().ToString();
            tdEngineServer.Port = element.Attributes["Port"].GetValue().ValueAsInt32();
            tdEngineServer.IsCloud = Convert.ToBoolean(element.Attributes["Is Cloud"].GetValue().Value);
            string key = element.Attributes["EncryptedKey"].GetValue().ToString();
            if (!tdEngineServer.IsCloud)
            {
                tdEngineServer.Username = element.Attributes["Username"].GetValue().ToString();
                string encryptedPassword = element.Attributes["EncryptedPassword"].GetValue().ToString();
                tdEngineServer.Password = StringCipher.Decrypt(encryptedPassword, key);
            }
            else
            {
                string encryptedToken = element.Attributes["EncryptedToken"].GetValue().ToString();
                tdEngineServer.Token = StringCipher.Decrypt(encryptedToken, key);
            }

            return tdEngineServer;
        }

        internal void Save(TDengineServer tdEngineServer)
        {
            AFElement element = tdEngineServer.Element;
            if (element == null)
            {

                AFDatabase dbConfig = null;
                try
                {
                    dbConfig = CreateCoreAssets();
                }
                catch(Exception ex)
                {
                    throw new Exception("Error creating core assets. " + ex.Message);
                }
                dbConfig.Refresh();
                AFElementTemplate elementTemplateCloud = dbConfig.ElementTemplates["TDEngineServerOnCloud"];
                AFElementTemplate elementTemplateOnPremise = dbConfig.ElementTemplates["TDEngineServerOnPremise"];
                AFElement tdEngineElement = dbConfig.Elements["TDengine"];
                this.tdEngineServerElement = tdEngineElement.Elements["Servers"];

                if (tdEngineServerElement.Elements[tdEngineServer.Name] != null)
                {
                    throw new Exception($"This server {tdEngineServer.Name} already exists. Please choose a different name and try again.");
                }

                if (tdEngineServer.IsCloud)
                {
                    element = tdEngineServerElement.Elements.Add(tdEngineServer.Name, elementTemplateCloud);
                }
                else
                {
                    element = tdEngineServerElement.Elements.Add(tdEngineServer.Name, elementTemplateOnPremise);
                }
            }
            element.Attributes["Host"].SetValue(new AFValue(tdEngineServer.Host));
            element.Attributes["Port"].SetValue(new AFValue(tdEngineServer.Port));
            element.Attributes["Is Cloud"].SetValue(new AFValue(tdEngineServer.IsCloud));
            string key = RandomString(10);
            element.Attributes["EncryptedKey"].SetValue(new AFValue(key));
            if (!tdEngineServer.IsCloud)
            {
                element.Attributes["Username"].SetValue(new AFValue(tdEngineServer.Username));
                string encryptedPassowrd = StringCipher.Encrypt(tdEngineServer.Password, key);
                element.Attributes["EncryptedPassword"].SetValue(new AFValue(encryptedPassowrd));
            }
            else
            {
                string encryptedToken = StringCipher.Encrypt(tdEngineServer.Token, key);
                element.Attributes["EncryptedToken"].SetValue(new AFValue(encryptedToken));
            }
            element.Database.CheckIn();
            LoadData(true);
        }

        private static Random random = new Random();

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private AFDatabase CreateCoreAssets()
        {
            AFDatabase dbConfig = this.piSystem.Databases["Configuration"];
            if (dbConfig == null)
            {
                dbConfig = this.piSystem.Databases.Add("Configuration");
            }

            AFElement tdEngineElement = dbConfig.Elements["TDengine"];
            if (tdEngineElement == null)
            {
                tdEngineElement = dbConfig.Elements.Add("TDengine");
            }
            this.tdEngineServerElement = tdEngineElement.Elements["Servers"];
            if (tdEngineServerElement == null)
            {
                tdEngineServerElement = tdEngineElement.Elements.Add("Servers");
            }

            AFElementTemplate elementTemplateCloud = dbConfig.ElementTemplates["TDEngineServerOnCloud"];
            if (elementTemplateCloud == null)
            {
                elementTemplateCloud = dbConfig.ElementTemplates.Add("TDEngineServerOnCloud");
                AFAttributeTemplate hostAttribute = elementTemplateCloud.AttributeTemplates.Add("Host");
                hostAttribute.Type = typeof(string);
                AFAttributeTemplate portAttribute = elementTemplateCloud.AttributeTemplates.Add("Port");
                portAttribute.Type = typeof(int);
                AFAttributeTemplate isCloudAttribute = elementTemplateCloud.AttributeTemplates.Add("Is Cloud");
                isCloudAttribute.Type = typeof(bool);
                AFAttributeTemplate tokenAttribute = elementTemplateCloud.AttributeTemplates.Add("EncryptedToken");
                tokenAttribute.Type = typeof(string);
                AFAttributeTemplate keyAttribute = elementTemplateCloud.AttributeTemplates.Add("EncryptedKey");
                keyAttribute.Type = typeof(string);
            }
            AFElementTemplate elementTemplateOnPremise = dbConfig.ElementTemplates["TDEngineServerOnPremise"];
            if (elementTemplateOnPremise == null)
            {
                elementTemplateOnPremise = dbConfig.ElementTemplates.Add("TDEngineServerOnPremise");
                AFAttributeTemplate hostAttribute = elementTemplateOnPremise.AttributeTemplates.Add("Host");
                hostAttribute.Type = typeof(string);
                AFAttributeTemplate portAttribute = elementTemplateOnPremise.AttributeTemplates.Add("Port");
                portAttribute.Type = typeof(int);
                AFAttributeTemplate isCloudAttribute = elementTemplateOnPremise.AttributeTemplates.Add("Is Cloud");
                isCloudAttribute.Type = typeof(bool);
                AFAttributeTemplate usernameAttribute = elementTemplateOnPremise.AttributeTemplates.Add("Username");
                usernameAttribute.Type = typeof(string);
                AFAttributeTemplate passwordAttribute = elementTemplateOnPremise.AttributeTemplates.Add("EncryptedPassword");
                passwordAttribute.Type = typeof(string);
                AFAttributeTemplate keyAttribute = elementTemplateOnPremise.AttributeTemplates.Add("EncryptedKey");
                keyAttribute.Type = typeof(string);
            }
            dbConfig.CheckIn();
            return dbConfig;
        }

        private void btnDelete_Click(object sender, EventArgs e)
        {
            var confirmResult = MessageBox.Show("Are you sure to delete?", "Question", MessageBoxButtons.YesNo);
            if (confirmResult == DialogResult.Yes)
            {
                try
                {

                    if (lbTDServers.SelectedItem == null)
                    {
                        return;
                    }
                    AFElement element = this.tdEngineServerElement.Elements[lbTDServers.SelectedItem.ToString()];
                    element.Delete();
                    this.tdEngineServerElement.Database.CheckIn();
                }
                catch(Exception ex)
                {
                    MessageBox.Show("Error deleting the server." + ex.Message);
                }

                try
                {
                    LoadData(true);
                }
                catch (Exception ex)
                {
                    MessageBox.Show("Error loading the data. " + ex.Message);
                }
            }

        }

        private void btnEdit_Click(object sender, EventArgs e)
        {
            if (piSystem == null || !piSystem.ConnectionInfo.IsConnected)
            {
                MessageBox.Show("Please connect to AF Server first.");
                return;
            }
            this.createOrEditTDengineForm = new CreateOrEditTDengineForm(this, this.TDengineServer);
            this.createOrEditTDengineForm.Show();
        }

        private void btnAdd_Click(object sender, EventArgs e)
        {
            if (piSystem == null || !piSystem.ConnectionInfo.IsConnected)
            {
                MessageBox.Show("Please connect to AF Server first.");
                return;
            }
            this.createOrEditTDengineForm = new CreateOrEditTDengineForm(this, null);
            this.createOrEditTDengineForm.Show();
        }
    }
}
