plugin_init="$1"
# mechanism plugins
for mech in anonymous crammd5 digestmd5 scram gssapiv2 kerberos4 login ntlm otp passdss plain srp gs2; do
    if [ ${plugin_init} = "${mech}_init.c" ];then

        echo "
#include <config.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifndef macintosh
#include <sys/stat.h>
#endif
#include <fcntl.h>
#include <assert.h>

#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>

#include \"plugin_common.h\"

#ifdef macintosh
#include <sasl_${mech}_plugin_decl.h>
#endif

#ifdef WIN32
BOOL APIENTRY DllMain( HANDLE hModule, 
                       DWORD  ul_reason_for_call, 
                       LPVOID lpReserved
					 )
{
    switch (ul_reason_for_call)
	{
		case DLL_PROCESS_ATTACH:
		case DLL_THREAD_ATTACH:
		case DLL_THREAD_DETACH:
		case DLL_PROCESS_DETACH:
			break;
    }
    return TRUE;
}
#endif

SASL_CLIENT_PLUG_INIT( $mech )
SASL_SERVER_PLUG_INIT( $mech )
"       > ${mech}_init.c
        echo "generating $1"
    fi # End of `if [ ${plugin_init} = "${mech}_init.c" ];then'
done

# auxprop plugins
for auxprop in sasldb sql ldapdb; do
    if [ ${plugin_init} = "${auxprop}_init.c" ];then

        echo "
#include <config.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifndef macintosh
#include <sys/stat.h>
#endif
#include <fcntl.h>
#include <assert.h>

#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>

#include \"plugin_common.h\"

#ifdef WIN32
BOOL APIENTRY DllMain( HANDLE hModule, 
                       DWORD  ul_reason_for_call, 
                       LPVOID lpReserved
					 )
{
    switch (ul_reason_for_call)
	{
		case DLL_PROCESS_ATTACH:
		case DLL_THREAD_ATTACH:
		case DLL_THREAD_DETACH:
		case DLL_PROCESS_DETACH:
			break;
    }
    return TRUE;
}
#endif

SASL_AUXPROP_PLUG_INIT( $auxprop )
"       > ${auxprop}_init.c
        echo "generating $1"
    fi # End of `if [ ${plugin_init} = "${auxprop}_init.c" ];then'
done

# ldapdb is also a canon_user plugin
if [ ${plugin_init} = "ldapdb_init.c" ];then
    echo "SASL_CANONUSER_PLUG_INIT( ldapdb )" >> ldapdb_init.c
fi
