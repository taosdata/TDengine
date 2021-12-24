using System;
using Test.UtilsTools;
using Cases;

namespace Cases.EntryPoint
{
    class Program
    {

        static void Main(string[] args)
        {
            IntPtr conn = IntPtr.Zero;
            IntPtr stmt = IntPtr.Zero;
            IntPtr res = IntPtr.Zero;

            conn = UtilsTools.TDConnection("127.0.0.1", "root", "taosdata", "", 0);
            UtilsTools.ExecuteQuery(conn, "drop database if  exists csharp");
            UtilsTools.ExecuteQuery(conn, "create database if not exists csharp keep 3650");
            UtilsTools.ExecuteQuery(conn, "use csharp");

            Console.WriteLine("====================StableColumnByColumn===================");
            StableColumnByColumn columnByColumn = new StableColumnByColumn();
            columnByColumn.Test(conn, "stablecolumnbycolumn");
            Console.WriteLine("====================StmtStableQuery===================");
            StmtStableQuery stmtStableQuery = new StmtStableQuery();
            stmtStableQuery.Test(conn, "stablecolumnbycolumn");

            Console.WriteLine("====================StableMutipleLine===================");
            StableMutipleLine mutipleLine = new StableMutipleLine();
            mutipleLine.Test(conn, "stablemutipleline");

            //================================================================================

            Console.WriteLine("====================NtableSingleLine===================");
            NtableSingleLine ntableSingleLine = new NtableSingleLine();
            ntableSingleLine.Test(conn, "stablesingleline");

            Console.WriteLine("====================NtableMutipleLine===================");
            NtableMutipleLine ntableMutipleLine = new NtableMutipleLine();
            ntableMutipleLine.Test(conn, "ntablemutipleline");
            Console.WriteLine("====================StmtNtableQuery===================");
            StmtNtableQuery stmtNtableQuery = new StmtNtableQuery();
            stmtNtableQuery.Test(conn, "ntablemutipleline");

            Console.WriteLine("====================NtableColumnByColumn===================");
            NtableColumnByColumn ntableColumnByColumn = new NtableColumnByColumn();
            ntableColumnByColumn.Test(conn, "ntablecolumnbycolumn");

            Console.WriteLine("====================fetchfeilds===================");
            FetchFields fetchFields = new FetchFields();
            fetchFields.Test(conn, "fetchfeilds");

            Console.WriteLine("===================JsonTagTest====================");
            JsonTagTest jsonTagTest = new JsonTagTest();
            jsonTagTest.Test(conn);

            Console.WriteLine("====================fetchLengthCase===================");
            FetchLengthCase fetchLengthCase = new FetchLengthCase();
            fetchLengthCase.TestRetrieveBinary(conn);

            UtilsTools.ExecuteQuery(conn, "drop database if  exists csharp");
            UtilsTools.CloseConnection(conn);
            UtilsTools.ExitProgram();
            
        }
    }
}
