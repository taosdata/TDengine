#ifndef _FILTERPICKER5_H
#define _FILTERPICKER5_H

enum ResultType { TRIGGER, CHAR_FUNC, PICKS };

/* functions */

void Pick(
	double deltaTime, 		// dt or timestep of data samples
	float* sample, 			// array of num_samples data samples
	int num_samples,		// the number of samples in array sample
	const double filterWindow,	// FilterPicker5 filter window
	const double longTermWindow,	// FilterPicker5 long term window
	const double threshold1,	// FilterPicker5 threshold1
	const double threshold2,	// FilterPicker5 threshold1
	const double tUpEvent,		// FilterPicker5 tUpEvent
	FilterPicker5_Memory** mem,	// pointer to memory structure/object so that this function can be called repetedly for packets of data in sequence from the same channel.  The calling routine is responsible for managing and associating the correct mem structures/objects with each channel.  On first call to this function for each channel set mem = NULL.
	BOOLEAN_INT useMemory,	 	// set to TRUE_INT=1 if function is called for packets of data in sequence, FALSE_INT = 0 otherwise

	PickData*** ppick_list,		// returned pointer to array of num_picks PickData structures/objects containing picks
	int* pnum_picks,			// the number of picks in array *ppick_list
 	char* channel_id		// a string identifier for the data channel
	);

#endif
