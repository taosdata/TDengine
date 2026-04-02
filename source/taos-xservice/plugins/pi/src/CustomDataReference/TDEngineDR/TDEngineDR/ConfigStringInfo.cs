namespace TDEngineDR
{
    internal class ConfigStringInfo
    {
        public ConfigStringInfo()
        {
        }

        public ConfigStringInfo(string configString)
        {
            string[] subStrings = configString.Trim().Split(';');
            foreach (string subString in subStrings)
            {
                if (!string.IsNullOrEmpty(subString))
                {
                    string propertyName = subString.Split('=')[0];
                    string value = subString.Split('=')[1];
                    if (propertyName == "Server")
                    {
                        Server = value;
                    }
                    else if (propertyName == "Database")
                    {
                        Database = value;
                    }
                    else if (propertyName == "Point")
                    {
                        Point = value;
                    }
                    else if (propertyName == "Column")
                    {
                        Column = value;
                    }
                    else if (propertyName == "Table")
                    {
                        Table = value;
                    }
                    else if (propertyName == "Attribute")
                    {
                        Attribute = value;
                    }
                    else if (propertyName == "Element")
                    {
                        Element = value;
                    }
                }
            }
        }

        public string Server { get; internal set; }
        public string Database { get; internal set; }
        public string Point { get; internal set; }
        public string Column { get; internal set; }
        public string Table { get; internal set; }
        public string Element { get; internal set; }
        public string Attribute { get; internal set; }

        public override string ToString()
        {
            if (!string.IsNullOrEmpty(Server) && !string.IsNullOrEmpty(Point))
            {
                return $"Server={Server};Database={Database};Point={Point}";
            }
            if (!string.IsNullOrEmpty(Server) && !string.IsNullOrEmpty(Column) && !string.IsNullOrEmpty(Table))
            {
                return $"Server={Server};Database={Database};Table={Table};Column={Column}";
            }
            if (!string.IsNullOrEmpty(Server) && !string.IsNullOrEmpty(Attribute) && !string.IsNullOrEmpty(Element))
            {
                return $"Server={Server};Database={Database};Element={Element};Attribute={Attribute}";
            }
            return null;
        }
    }
}