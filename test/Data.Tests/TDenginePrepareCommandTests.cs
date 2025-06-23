using System;
using Xunit;
using TDengine.Data.Client;
using TDengine.Driver;

namespace Data.Tests
{
    public class TDenginePrepareCommandTests
    {
        private readonly TDengineConnection _connection;

        public TDenginePrepareCommandTests()
        {
            _connection =
                new TDengineConnection("host=localhost;port=6030;username=root;password=taosdata;protocol=Native;");
            _connection.Open();
        }

        private void CreateDatabaseAndTable(string db, string table)
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create database if not exists " + db;
                command.ExecuteNonQuery();
                command.CommandText = $"create table if not exists {db}.{table} (ts timestamp,v int)";
                command.ExecuteNonQuery();
            }
        }

        private void DropDatabase(string db)
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "drop database if exists " + db;
                command.ExecuteNonQuery();
                command.CommandText = $"drop table if exists {db}.test_table01";
            }
        }

        [Fact]
        public void PrepareCommandTest()
        {
            using (var command = new TDengineCommand())
            {
                string db = "test_prepare_command";
                string table = "test_table01";
                CreateDatabaseAndTable(db, table);
                try
                {
                    var connection =
                        new TDengineConnection(
                            $"host=localhost;port=6030;username=root;password=taosdata;protocol=Native;db={db};");
                    connection.Open();

                    command.CommandText = $"select * from {db}.{table}";
                    command.Connection = connection;

                    var dbDataReader = command.ExecuteReader();

                    Assert.True(dbDataReader.HasRows);
                }
                finally
                {
                    DropDatabase(db);
                }
            }
        }


        [Fact]
        public void ConnectionInitCommandTest()
        {
            string db = "test_connection_init_command";
            string table = "test_table01";
            CreateDatabaseAndTable(db, table);
            try
            {
                var connection =
                    new TDengineConnection(
                        $"host=localhost;port=6030;username=root;password=taosdata;protocol=Native;db={db};");
                connection.Open();
                using (var command = new TDengineCommand(connection))
                {
                    command.CommandText = $"select * from {db}.{table}";
                    command.Connection = connection;

                    var dbDataReader = command.ExecuteReader();

                    Assert.True(dbDataReader.HasRows);
                }
            }
            finally
            {
                DropDatabase(db);
            }
        }
    }
}