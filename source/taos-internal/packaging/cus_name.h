/*
 * =====================================================================================
 *
 *       Filename:  custom_name.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  01/05/2023 08:52:43 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef _CUS_NAME_H_
#define _CUS_NAME_H_

#ifdef CUS_NAME
    char cusName[] = CUS_NAME;
#endif

#ifdef CUS_PROMPT
    char cusPrompt[] = CUS_PROMPT;
#endif

#ifdef CUS_EMAIL
    char cusEmail[] = CUS_EMAIL;
#endif

#endif  // _CUS_NAME_H_
