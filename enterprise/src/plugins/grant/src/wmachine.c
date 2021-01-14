/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies. 
 *  No part of this file may be reproduced, stored, transmitted, 
 *  disclosed or used in any form or by any means other than as 
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/
//version 2.0.4.0

#define _DEFAULT_SOURCE
#include "os.h"
#include "machine.h"

SGrantObj grantObj;

void grantActiveSystem(const char *inputCfgFile) {}

char *grantGetMachineSerials() { return "1234567890"; }
