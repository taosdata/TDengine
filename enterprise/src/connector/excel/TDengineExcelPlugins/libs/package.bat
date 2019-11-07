set packpath=%1%

%packpath%ExcelDnaPack %packpath%TDengineExcelPlugins.dna /Y /O %packpath%TDengineExcelPluginsPack.XLL
%packpath%ExcelDnaPack %packpath%TDengineExcelPlugins64.dna /Y /O %packpath%TDengineExcelPluginsPack64.XLL
pause

