/*
 * This file is part of the Anthony Lomax C Library.
 *
 * Copyright (C) 2008 Anthony Lomax <anthony@alomax.net www.alomax.net>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */



// AJL: based on TestPicker4.java, 2008.07.14

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "ew_bridge.h"
#include "PickData.h"


/** picker memory class */

PickData* init_PickData() {

	PickData* pickData = calloc(1, sizeof(PickData));

	pickData->polarity = POLARITY_UNKNOWN;
	pickData->polarityWeight = 0.0;
	pickData->indices[0] = pickData->indices[1] = -1;
	pickData->amplitude = 0.0;
	pickData->amplitudeUnits = NO_AMP_UNITS;
	pickData->period = 0.0;

	return(pickData);

}


/** picker memory class */

void set_PickData(PickData* pickData, double index0, double index1, int polarity, double polarityWeight, double amplitude, int amplitudeUnits, double period) {

        pickData->indices[0] = index0;
        pickData->indices[1] = index1;
        pickData->polarity = polarity;
        pickData->polarityWeight = polarityWeight;
        pickData->amplitude = amplitude;
        pickData->amplitudeUnits = amplitudeUnits;
        pickData->period = period;

}



/** clean up pick memory */

void free_PickData(PickData* pickData)
{
	if (pickData == NULL)
		return;

	taosMemoryFree(pickData);

}





/** print data */

int fprintf_PickData(PickData* pickData, FILE* pfile)
{
	if (pfile == NULL || pickData == NULL)
		return(0);

	fprintf(pfile, "%d %f %f %f %f %d %f ",
		pickData->polarity, pickData->polarityWeight, pickData->indices[0], pickData->indices[1], pickData->amplitude, pickData->amplitudeUnits, pickData->period
	       );


	return(1);

}



/** print in NLLOC_OBS format */

char* printNlloc(char* pick_str, PickData* pickData, double dt, char* label, char* inst, char* comp, char* onset,
		 char* phase, int year, int month, int day, int hour, int min, double sec) {


	// first motion
	char first_mot[16];
	long int idate, ihrmin;
	// char error_type[] = "GAU";
        double error;
	// double coda_dur = 0.0;
	// double amplitude;
        // double period;

	strcpy(first_mot, "?");
	if (pickData->polarity == POLARITY_POS)
		strcpy(first_mot, "+");
	if (pickData->polarity == POLARITY_NEG)
		strcpy(first_mot, "-");

	// add pick time to time
	sec += dt * (pickData->indices[0] + pickData->indices[1]) / 2.0;
	while (sec >= 60.0) {
		min++;
		sec-= 60.0;
	}
	while (min >= 60) {
		hour++;
		min-= 60;
	}

	// code data and time integers
	idate = year * 10000 + month * 100 + day;
	ihrmin = hour * 100 + min;

	// error
        // set uncertainty to half width between right and left indices
	error = dt * fabs(pickData->indices[1] - pickData->indices[0]);
	error /= 2.0;
	if (error < 0.0) {
		error = 0.0;
        }

	// misc
	//coda_dur = 0.0;
	//amplitude = pickData->amplitude;
	//period = pickData->period;
	//double apriori_weight = 1.0;

	// write observation part of FORMAT_PHASE_2 phase line
	//sprintf(pick_str,
	//	"%-6s %-4s %-4s %-1s %-6s %-1s %8.8ld %4.4ld %9.4lf %-3s %9.2le %9.2le %9.2le %9.2le %9.4lf",
	// write observation part of orig NLL phase line
	
	/*
	sprintf(pick_str,
		"%-6s %-4s %-4s %-1s %-6s %-1s %8.8ld %4.4ld %9.4lf %-3s %9.3le %9.3le %9.3le %9.3le",
			label,
			inst,
			comp,
			onset,
			phase,
			first_mot,
			//quality
			idate, ihrmin,
			sec,
			error_type, error,
			coda_dur,
			amplitude,
			period//,
			//apriori_weight
		);*/
	//modified by chentong
	sprintf(pick_str,
		"%8.8ld %4.4ld %9.4lf",			
			//quality
			idate, ihrmin,
			sec//,
			//apriori_weight
		);

	return(pick_str);

}



/** add a PickData to a PickData list */

#define SIZE_INCREMENT 16

void addPickToPickList(PickData* pickData, PickData*** ppick_list, int* pnum_picks) {

	PickData** newPickList = NULL;
	int n;

	if (*pnum_picks == 0 || *ppick_list == NULL) {		// list not yet created
		*ppick_list = calloc(SIZE_INCREMENT, sizeof(PickData*));
	}
	else if ((*pnum_picks % SIZE_INCREMENT) == 0) {	// list will be too small
		newPickList = calloc(*pnum_picks + SIZE_INCREMENT, sizeof(PickData*));
		for (n = 0; n < *pnum_picks; n++)
			newPickList[n] = (*ppick_list)[n];
		taosMemoryFree(*ppick_list);
		*ppick_list = newPickList;
	}

	// add PickData
	(*ppick_list)[*pnum_picks] = pickData;
	(*pnum_picks)++;

}





/** clean up pick list memory */

void free_PickList(PickData** pick_list, int num_picks)
{
	int n;
	if (pick_list == NULL || num_picks < 1)
		return;

	for (n = 0; n < num_picks; n++)
		free_PickData(*(pick_list + n));

	taosMemoryFree(pick_list);
}

