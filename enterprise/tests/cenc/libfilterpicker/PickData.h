#ifndef _PICKDATA_H
#define _PICKDATA_H

#include <limits.h>

/** picker structure/class */

enum AmpUnits { NO_AMP_UNITS, DATA_AMP_UNITS, CHAR_FUNCT_AMP_UNITS, INDEP_VAR_UNITS };

#define POLARITY_POS 		1
#define POLARITY_UNKNOWN 	0
#define POLARITY_NEG 		-1

typedef struct
{
	int polarity;                   // pick polarity (POLARITY_POS, POLARITY_NEG or POLARITY_UNKNOWN)
	double polarityWeight;          // pick polarity weight (0.0-1.0)       20121019 AJL - added
	double indices[2];              // indices correspoinding to pick time - and + pick uncertainty,
                                        //    gives upper and lower uncertainty limits of pick;
                                        //    pick time is intermediate between these two indices
	double amplitude;               // amplitude of characteristic function at point where pick accepted
	int amplitudeUnits;             // units of above amplitude
	double period;                  // the period in sec of the band on which the pick triggered
}
PickData;


/* functions */

PickData* init_PickData();

void set_PickData(PickData* pickData, double index0, double index1, int polarity, double polarityWeight, double amplitude, int amplitudeUnits, double period);

void free_PickData(PickData* pickData);

int fprintf_PickData(PickData* pickData, FILE* pfile);

char* printNlloc(char* pick_str, PickData* pickData, double dt, char* label, char* inst, char* comp, char* onset,
		 char* phase, int year, int month, int day, int hour, int min, double sec);

void addPickToPickList(PickData* pickData, PickData*** ppick_list, int* pnum_picks);

void free_PickList(PickData** pick_list, int num_picks);

#endif
