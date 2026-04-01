using System;
using Xunit;
using TDengine.Data.Client;
using TDengine.Driver;

namespace Data.Tests
{
    public class TDengineCommandTests : IDisposable
    {
        private TDengineConnection _connection;
        private TDengineConnection _wsConnection;

        public TDengineCommandTests()
        {
            _connection = new TDengineConnection("username=root;password=taosdata");
            _connection.Open();
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create database if not exists test_command";
                command.ExecuteNonQuery();
                _connection.ChangeDatabase("test_command");
                command.CommandText = "create table if not exists t (ts timestamp,v int)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue(123);
            }

            _wsConnection =
                new TDengineConnection(
                    "username=root;password=taosdata;protocol=WebSocket;host=localhost;port=6041;useSSL=false");
            _wsConnection.Open();
            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "create database if not exists test_command_ws";
                command.ExecuteNonQuery();
                _wsConnection.ChangeDatabase("test_command_ws");
                command.CommandText = "create table if not exists t (ts timestamp,v int)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue(123);
            }
        }

        public void Dispose()
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "drop database if exists test_command";
                command.ExecuteNonQuery();
            }

            _connection.Close();
            _connection.Dispose();

            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "drop database if exists test_command_ws";
                command.ExecuteNonQuery();
            }

            _wsConnection.Close();
            _wsConnection.Dispose();
        }

        [Fact]
        public void SetConnection_DoesNotThrowException()
        {
            //CASE 1: Set an already open connection.
            using(var command = new TDengineCommand())
            {
                command.CommandText = "SELECT * FROM t";
                var ex = Record.Exception(() => command.Connection = _connection);
                Assert.Null(ex);
            }

            //CASE 2: Set an connection that is not yet open.
            using(var command = new TDengineCommand())
            {
                using(var connection = new TDengineConnection("username=root;password=taosdata"))
                {
                    command.CommandText = "SELECT * FROM t";
                    var ex = Record.Exception(() => command.Connection = connection);
                    Assert.Null(ex);
                }
            }
        }

        [Fact]
        public void ExecuteNonQuery_WithDBNull()
        {
            using(var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create table if not exists t_dbnull (ts timestamp,v int,description nchar(100))";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t_dbnull VALUES (?,?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue(123);
                command.Parameters.AddWithValue(DBNull.Value);

                int affectedRows = command.ExecuteNonQuery();

                Assert.Equal(1, affectedRows);
            }
        }

		[Fact]
        public void ExecuteNonQuery_WithValidCommand_ReturnsAffectedRows()
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create table if not exists t1 (ts timestamp,v int)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t1 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue(123);

                int affectedRows = command.ExecuteNonQuery();

                Assert.Equal(1, affectedRows);
            }

            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create table if not exists t2 (ts timestamp,v binary(16))";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t2 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue("5binary");

                int affectedRows = command.ExecuteNonQuery();

                Assert.Equal(1, affectedRows);
            }

            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "create table if not exists t3 (ts timestamp,v nchar(16))";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t3 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue("9nchar");

                int affectedRows = command.ExecuteNonQuery();

                Assert.Equal(1, affectedRows);
            }
        }

        [Fact]
        public void ExecuteScalar_WithValidCommand_ReturnsSingleValue()
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "SELECT COUNT(*) FROM t";

                object result = command.ExecuteScalar();

                Assert.NotNull(result);
                Assert.IsType<long>(result);
            }
        }

        [Fact]
        public void Prepare_WithValidCommand_DoesNotThrowException()
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "SELECT * FROM t";
                var ex = Record.Exception(() => command.Prepare());
                Assert.Null(ex);
            }
        }

        [Fact]
        public void ExecuteDbDataReader_WithValidCommand_ReturnsDbDataReader()
        {
            using (var command = new TDengineCommand(_connection))
            {
                command.CommandText = "SELECT * FROM t";

                using (var reader = command.ExecuteReader())
                {
                    Assert.NotNull(reader);
                    Assert.IsType<TDengineDataReader>(reader);
                }
            }
        }

        [Fact]
        public void WS_ExecuteNonQuery_WithValidCommand_ReturnsAffectedRows()
        {
            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "create table if not exists t1 (ts timestamp,v int)";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t1 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue(123);

                int affectedRows = command.ExecuteNonQuery();

                Assert.Equal(1, affectedRows);
            }

            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "create table if not exists t2 (ts timestamp,v binary(16))";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t2 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue("5binary");

                int affectedRows = command.ExecuteNonQuery();

                Assert.Equal(1, affectedRows);
            }

            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "create table if not exists t3 (ts timestamp,v nchar(16))";
                command.ExecuteNonQuery();
                command.CommandText = "INSERT INTO t3 VALUES (?,?)";
                command.Parameters.AddWithValue(DateTime.Now);
                command.Parameters.AddWithValue("9nchar");

                int affectedRows = command.ExecuteNonQuery();

                Assert.Equal(1, affectedRows);
            }
        }

        [Fact]
        public void WS_ExecuteScalar_WithValidCommand_ReturnsSingleValue()
        {
            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "SELECT COUNT(*) FROM t";

                object result = command.ExecuteScalar();

                Assert.NotNull(result);
                Assert.IsType<long>(result);
            }
        }

        [Fact]
        public void WS_Prepare_WithValidCommand_DoesNotThrowException()
        {
            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "SELECT * FROM t";
                var ex = Record.Exception(() => command.Prepare());
                Assert.Null(ex);
            }
        }

        [Fact]
        public void WS_ExecuteDbDataReader_WithValidCommand_ReturnsDbDataReader()
        {
            using (var command = new TDengineCommand(_wsConnection))
            {
                command.CommandText = "SELECT * FROM t";

                using (var reader = command.ExecuteReader())
                {
                    Assert.NotNull(reader);
                    Assert.IsType<TDengineDataReader>(reader);
                }
            }
        }
    }
}