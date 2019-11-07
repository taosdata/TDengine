using System.Windows.Forms;

namespace excel2010
{
    public partial class TDAboutForm : TDForm
    {
        public TDAboutForm()
        {
            InitializeComponent();
        }

        private void Form_KeyPress(object sender, KeyPressEventArgs e)
        {
            if (e.KeyChar == (char)Keys.Escape)
            {
                this.GetFactory().CloseForm();
            }
        }
    }
}
