using System.Text.Json;

namespace TDPIConnector.TDEngine.TaosxClient
{

    internal class ControlMessageBuilder
    {

        public static string BuildDropMessage(string elementId) {
            var message = new { 
                DROP  = new { 
                    table = elementId
                }
            };
            return JsonSerializer.Serialize(message);
        }


        public static string BuildDeleteMessage(string elementId, string condition) {
            var message = new
            {
              DELETE = new
              {
                  table = elementId,
                  condition
              }
            };
            return JsonSerializer.Serialize(message);
        }

        public static string BuildInsertMessage(string elementId, string column_values)
        {
            var message = new
            {
                INSERT = new
                {
                    table = elementId,
                    column_values
                }
            };
            return JsonSerializer.Serialize(message);
        }

        public static string BuildAlterMessage(string elementId, string alterTableClause) {
            var message = new
            {
                ALTER = new
                {
                    table = elementId,
                    alter_table_clause = alterTableClause
                }
            };
            return JsonSerializer.Serialize(message);
        }
    }
}
