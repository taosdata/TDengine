using System.Collections.Generic;

namespace PISimulator.Core.Config
{
    public class ElementTemplateConfig
    {
        public string Name { get; set; }
        public List<AttributeTemplateConfig> AttributeTemplates { get; set; }
    }
}

