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
            UtilsTools.ExecuteUpdate(conn, "drop database if  exists csharp");
            UtilsTools.ExecuteUpdate(conn, "create database if not exists csharp keep 3650");
            UtilsTools.ExecuteUpdate(conn, "use csharp");

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
            IntPtr resPtr = UtilsTools.ExecuteQuery(conn, "select * from stablesingleline ");
            UtilsTools.DisplayRes(resPtr);

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

           
            StableStmtCases stableStmtCases = new StableStmtCases();
            Console.WriteLine("====================stableStmtCases.TestBindSingleLineCn===================");
            stableStmtCases.TestBindSingleLineCn(conn, "stablestmtcasestestbindsinglelinecn");

            Console.WriteLine("====================stableStmtCases.TestBindColumnCn===================");
            stableStmtCases.TestBindColumnCn(conn, " stablestmtcasestestbindcolumncn");

            Console.WriteLine("====================stableStmtCases.TestBindMultiLineCn===================");
            stableStmtCases.TestBindMultiLineCn(conn, "stablestmtcasestestbindmultilinecn");

            NormalTableStmtCases normalTableStmtCases = new NormalTableStmtCases();
            Console.WriteLine("====================normalTableStmtCases.TestBindSingleLineCn===================");
            normalTableStmtCases.TestBindSingleLineCn(conn, "normaltablestmtcasestestbindsinglelinecn");

            Console.WriteLine("====================normalTableStmtCases.TestBindColumnCn===================");
            normalTableStmtCases.TestBindColumnCn(conn, "normaltablestmtcasestestbindcolumncn");

            Console.WriteLine("====================normalTableStmtCases.TestBindMultiLineCn===================");
            normalTableStmtCases.TestBindMultiLineCn(conn, "normaltablestmtcasestestbindmultilinecn");

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
