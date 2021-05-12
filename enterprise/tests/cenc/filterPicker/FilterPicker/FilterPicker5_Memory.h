#ifndef _FILTERPICKER5_MEMORY_H
#define _FILTERPICKER5_MEMORY_H

/** picker memory structure/class */

// $DOC =============================
// $DOC picker memory for realtime processing of packets of data

typedef struct
{

	double longDecayFactor;
	double longDecayConst;
	int nLongTermWindow;

	int indexEnableTriggering;
	BOOLEAN_INT enableTriggering;
	int nTotal;


	int numRecursive ;   // number of powers of 2 to process

	double* xRec;
	double* test;
	double** filteredSample;
	double* lastFilteredSample;
	double* mean_xRec;
	double* mean_stdDev_xRec;
	double* mean_var_xRec;
        double* period;
        double* lowPassConst;
        double* highPassConst;
	double* decayFactor;
	double* decayConst;
	int* indexDelay;

	double window;
	int nDelay;

	double lastSample;
        double lastDiffSample;

	double* charFunctUncertainty;
        double* charFunctUncertaintyLast;
	//double charFunctLast;
	double charFunctLast1Smooth;
	double charFunctLast2Smooth;
	double* uncertaintyThreshold;  // AJL 20091214
	double maxUncertaintyThreshold;
	double minUncertaintyThreshold;
        double maxAllowNewPickThreshold;
        int allowNewPickIndex;
	//double maxAllowNewTriggerThreshold;

        double** polarityDerivativeSum;
        double** polaritySumAbsDerivative;

        double amplitudeUncertainty;
        int** indexUncertainty;  // AJL 20091214
	int indexUncertaintyTrigger;
	int countPolarity;

	BOOLEAN_INT inTriggerEvent;
	double integralCharFunctPick;

        double* charFunctClippedValue;
        double* charFunctValue;
        int* charFunctNumRecursiveIndex;

	int indexUpEvent;
	int indexUpEventEnd;   // prevents disabling of charFunct update
	int nTUpEvent;

// $DOC criticalIntegralCharFunct is tUpEvent * threshold2
	double criticalIntegralCharFunct;

// $DOC integralCharFunctClipped is integral of charFunct values for last nTUpEvent samples, charFunct values possibly limited if around trigger time
	double* integralCharFunctClipped;

// flag to prevent next trigger until charFunc drops below threshold2
	BOOLEAN_INT underThresholdSinceLastTrigger;

	int upEventBufPtr;

	BOOLEAN_INT acceptedPick;
	BOOLEAN_INT willAcceptPick;
	int pickPolarity;
	double pickPolarityWeight;      // 20121019 AJL - added
	int triggerNumRecursiveIndex;

}
FilterPicker5_Memory;




/* functions */

FilterPicker5_Memory* init_filterPicker5_Memory(
			double deltaTime,
			float* sample, int length,
			const double filterWindow,
			const double longTermWindow,
			const double threshold1,
			const double threshold2,
			const double tUpEvent)
;

void free_FilterPicker5_Memory(FilterPicker5_Memory** pfilterPicker5_Memory);

#endif
