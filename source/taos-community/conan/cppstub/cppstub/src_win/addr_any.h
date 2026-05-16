#ifndef __ADDR_ANY_H__
#define __ADDR_ANY_H__

#include <windows.h>
// Now we have to define _NO_CVCONST_H to be able to access 
// various declarations from DbgHelp.h, which are not available by default 
#define _NO_CVCONST_H
#include <dbghelp.h>
//c++
#include <string>
#include <map>

//dbghelp.dll
//symsrv.dll
#pragma comment(lib,"dbghelp.lib")


class AddrAny
{
public:
    AddrAny()
    {
        SymSetOptions(SYMOPT_UNDNAME | SYMOPT_DEFERRED_LOADS | SYMOPT_FAVOR_COMPRESSED);
    }
    
    int get_func_addr(std::string func_name, std::map<std::string,void*>& result)
    {
        CHAR symbolPath[0x2000] = { 0 };
        CHAR szPath[MAX_PATH] = { 0 };
        GetModuleFileName(0, szPath, ARRAYSIZE(szPath));
        CHAR * temp = strchr(szPath, '\\');
        if (temp == NULL)
        {
            return -1;
        }
        *temp = 0;
        strcat(symbolPath, "SRV*");
        strcat(symbolPath, szPath);
        strcat(symbolPath, "*http://msdl.microsoft.com/download/symbols");
        
        HANDLE hProcess;
        hProcess = GetCurrentProcess();
        
        if (!SymInitialize(hProcess, symbolPath, TRUE))
        {
            return -1;
        }

        BYTE memory[0x2000] = {0};
        ZeroMemory(memory, sizeof(memory));
        SYMBOL_INFO * syminfo = (SYMBOL_INFO *)memory;
        syminfo->SizeOfStruct = sizeof(SYMBOL_INFO);
        syminfo->MaxNameLen = MAX_SYM_NAME;
     
        if (!SymFromName(GetCurrentProcess(), func_name.c_str(), syminfo))
        {
            return 0;
        }
        SymCleanup(GetCurrentProcess());
        result.insert ( std::pair<std::string,void *>(func_name,(void*)syminfo->Address));
        return 1;
    }
};
 #endif

 
