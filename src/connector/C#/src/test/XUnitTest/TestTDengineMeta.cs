using System;
using Xunit;
using TDengineDriver;

namespace TDengineDriver.Test
{
    public class TestTDengineMeta
    {
        [Fact]
        public void TestTypeNameBool()
        {
            string typeName = "BOOL";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 1;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }

        [Fact]
        public void TestTypeNameTINYINT()
        {
            string typeName = "TINYINT";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 2;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameSMALLINT()
        {
            string typeName = "SMALLINT";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 3;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameINT()
        {
            string typeName = "INT";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 4;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameBIGINT()
        {
            string typeName = "BIGINT";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 5;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUTINYINT()
        {
            string typeName = "TINYINT UNSIGNED";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 11;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUSMALLINT()
        {
            string typeName = "SMALLINT UNSIGNED";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 12;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUINT()
        {
            string typeName = "INT UNSIGNED";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 13;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUBIGINT()
        {
            string typeName = "BIGINT UNSIGNED";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 14;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }

        [Fact]
        public void TestTypeNameFLOAT()
        {
            string typeName = "FLOAT";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 6;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameDOUBLE()
        {
            string typeName = "DOUBLE";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 7;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameSTRING()
        {
            string typeName = "STRING";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 8;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameTIMESTAMP()
        {
            string typeName = "TIMESTAMP";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 9;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameNCHAR()
        {
            string typeName = "NCHAR";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();
            meta.type = 10;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUndefined()
        {
            string typeName = "undefine";
            TDengineDriver.TDengineMeta meta = new TDengineDriver.TDengineMeta();

            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
    }
}
