using OSIsoft.AF;
using OSIsoft.AF.Asset;
using System;
using System.Collections.Generic;
using TDEngineDR.TDEngineClient;

namespace TDEngineDR
{
    public class TDEngineServerManager
    {
        public static List<AFElement> GetTDEngineServerElements(PISystem piSystem)
        {
            AFDatabase configurationDb = piSystem.Databases["Configuration"];
            if (configurationDb == null)
            {
                throw new Exception("Configuration database not found.");
            }
            AFElement tdEngineElement = configurationDb.Elements["TDengine"];
            if (tdEngineElement == null)
            {
                throw new Exception("TDengine root element not found.");
            }
            AFElement tdEngineServersElement = tdEngineElement.Elements["Servers"];
            if (tdEngineServersElement == null)
            {
                throw new Exception("TDengine Servers element not found.");
            }
            List<AFElement> elements = new List<AFElement>();
            foreach (AFElement element in tdEngineServersElement.Elements)
            {
                elements.Add(element);
            }
            return elements;
        }



        public static TDEngineHttpClient GetTDEngineClient(string serverName, PISystem piSystem)
        {
            var tdEngineServersElements = GetTDEngineServerElements(piSystem);
            foreach (AFElement tdEngineServerElement in tdEngineServersElements)
            {
                if (tdEngineServerElement.Name.ToLower() == serverName.ToLower())
                {
                    bool isCloud = Convert.ToBoolean(tdEngineServerElement.Attributes["Is Cloud"].GetValue().Value);
                    string host = tdEngineServerElement.Attributes["Host"].GetValue().ToString();
                    int port = tdEngineServerElement.Attributes["Port"].GetValue().ValueAsInt32();
                    string key = tdEngineServerElement.Attributes["EncryptedKey"].GetValue().ToString();
                    if (isCloud)
                    {
                        string encryptedToken = tdEngineServerElement.Attributes["EncryptedToken"].GetValue().ToString();
                        string token = StringCipher.Decrypt(encryptedToken, key);
                        return new TDEngineHttpClient(host, port, token);
                    }
                    else
                    {
                        string username = tdEngineServerElement.Attributes["Username"].GetValue().ToString();
                        string encryptedPassword = tdEngineServerElement.Attributes["EncryptedPassword"].GetValue().ToString();
                        string password = StringCipher.Decrypt(encryptedPassword, key);
                        return new TDEngineHttpClient(host, port, username, password);
                    }
                }
            }
            throw new Exception($"TDengine server {serverName} not found.");

        }
    }
}
