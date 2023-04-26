namespace PISimulator.Core.Config
{
    public class AFSettingsConfig
    {
        public AFSettingsConfig()
        {

        }

        public string AFDatabase { get; set; }

        public string PIPointPrefix { get; set; }

        public ElementTemplateConfig ElementTemplate { get; set; }

        public AFTreeConfig AFTree { get; set; }
    }
}
