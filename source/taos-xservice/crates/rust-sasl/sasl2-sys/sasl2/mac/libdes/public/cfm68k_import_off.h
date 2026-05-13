/*
 * set a macro for des.h from libdes to not turn all the des functions
 * for cfm68k into pragma imports
 */
#define DESLIB_CFM68K_NO_IMPORTS 1

/*
 * compiler doesnt allow an empty file even for precompiled... go figure
 */
typedef int cfm68_des_enable_braindead;
