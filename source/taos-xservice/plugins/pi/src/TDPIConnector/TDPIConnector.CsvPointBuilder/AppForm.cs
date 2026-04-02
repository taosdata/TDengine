using OSIsoft.AF.PI;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace TDPIConnector.CsvPointBuilder
{
    public partial class AppForm : Form
    {
        private PIServers piServers;
        private PIServer piServer;
        private List<string> csvPoints;
        private string filePath;
        
        public AppForm()
        {
            filePath = AppDomain.CurrentDomain.BaseDirectory + "Points.csv";
            InitializeComponent();
            LoadPIDataArchiveNames();
            LoadCsvPIPoints();
            grSearch.Visible = false;
          
        }

        private void LoadCsvPIPoints()
        {
            csvPoints = new List<string>();
            lbPointsCsv.Items.Clear();

            List<string> pointNames = new List<string>();
            using (var reader = new StreamReader(filePath))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    pointNames.Add(line);
                }
            }
            pointNames = pointNames.OrderBy(c => c.ToString()).ToList();

            foreach (string pointName in pointNames)
            {
                if (!csvPoints.Contains(pointName))
                {
                    lbPointsCsv.Items.Add(pointName);
                    csvPoints.Add(pointName);
                }
            }
        }

        private void LoadPIDataArchiveNames()
        {
            this.piServers = new PIServers();
            this.cbPIDataArchiveNames.Items.Clear();
            foreach (var piServer in piServers)
            {
                this.cbPIDataArchiveNames.Items.Add(piServer.Name);
            }
        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            string selectedPIDataArchiveName = this.cbPIDataArchiveNames.SelectedItem as string;
            if (string.IsNullOrEmpty(selectedPIDataArchiveName))
            {
                MessageBox.Show("Please select a PI Data Archive! It cannot be null or empty.", "Error!");
                grSearch.Visible = false;
            }
            else
            {
                try
                {
                    this.piServer = this.piServers[selectedPIDataArchiveName];
                    this.piServer.Connect();
                
                    grSearch.Visible = true;

                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Error connecting to PI Data Archive {selectedPIDataArchiveName}: {ex.Message}", "Error!");
                    grSearch.Visible = false;
                }
            }

        }

        private void btnSearch_Click(object sender, EventArgs e)
        {
            PIPointQuery query1 = new PIPointQuery(PICommonPointAttributes.Tag, OSIsoft.AF.Search.AFSearchOperator.Equal, tbPointName.Text);
            PIPointQuery query2 = new PIPointQuery(PICommonPointAttributes.PointSource, OSIsoft.AF.Search.AFSearchOperator.Equal, tbPointSource.Text);
            PIPointQuery query3 = new PIPointQuery(PICommonPointAttributes.InstrumentTag, OSIsoft.AF.Search.AFSearchOperator.Equal, tbInstrumentTag.Text);
            PIPointQuery query4 = new PIPointQuery(PICommonPointAttributes.Descriptor, OSIsoft.AF.Search.AFSearchOperator.Equal, tbDescriptor.Text);
            IEnumerable<PIPoint> foundPoints = PIPoint.FindPIPoints(piServer, new PIPointQuery[] { query1, query2, query3, query4 });

            lbPointsFound.Items.Clear();
            int count = 0;
            foreach (PIPoint foundPoint in foundPoints)
            {
                lbPointsFound.Items.Add(foundPoint.Name);
                count++;
                if (count == 100)
                {
                    break;
                }
            }

        }

        private void btnAddToCsv_Click(object sender, EventArgs e)
        {
            foreach (string selectedItem in lbPointsFound.SelectedItems)
            {
                if (!csvPoints.Contains(selectedItem))
                {
                    csvPoints.Add(selectedItem);
                }
            }
            csvPoints = csvPoints.OrderBy(c => c.ToString()).ToList();
            lbPointsCsv.Items.Clear();
            foreach (string csvPoint in csvPoints)
            {
                lbPointsCsv.Items.Add(csvPoint);
            }
        }

        private void btnDeletePoints_Click(object sender, EventArgs e)
        {
            var selectedItems = lbPointsCsv.SelectedItems;
            List<string> pointsToDelete = new List<string>();
            foreach (string selectedItem in selectedItems)
            {
                pointsToDelete.Add(selectedItem);
            }
            foreach (string pointToDelete in pointsToDelete)
            {
                if (csvPoints.Contains(pointToDelete))
                {
                    lbPointsCsv.Items.Remove(pointToDelete);
                    csvPoints.Remove(pointToDelete);
                }
            }
        }

        private void btnSaveCsv_Click(object sender, EventArgs e)
        {
            string fileContent = string.Empty;
            foreach (string pointName in lbPointsCsv.Items)
            {
                fileContent += pointName + "\n";
            }

            try
            {
             
                File.WriteAllText(filePath, fileContent);
                MessageBox.Show($"Updated the CSV file!", "Success!");
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error writing to CSV: {ex.Message}", "Error!");
            }
        }
    }
}
