/***************************************************************************
 * Routines for dealing with libmseed extra headers stored as JSON.
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2020 Chad Trabant, IRIS Data Management Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

#include "libmseed.h"
#include "parson.h"

/**********************************************************************/ /**
 * @brief Search for and return an extra header value.
 *
 * The extra header value is specified as a path in dot notation, e.g.
 * \c 'objectA.objectB.header'
 *
 * This routine can get used to test for the existence of a value
 * without returning the value by setting \a value to NULL.
 *
 * If the target item is found (and \a value parameter is set) the
 * value will be copied into the memory specified by \c value.  The
 * \a type value specifies the data type expected.
 *
 * @param[in] msr Parsed miniSEED record to query
 * @param[in] path Header value desired, specified in dot notation
 * @param[out] value Buffer for value, of type \c type
 * @param[in] type Type of value expected, one of:
 * @parblock
 * - \c 'n' - \a value is type \a double
 * - \c 's' - \a value is type \a char* (maximum length is: \c maxlength - 1)
 * - \c 'b' - \a value of type \a int (boolean value of 0 or 1)
 * @endparblock
 * @param[in] maxlength Maximum length of string value
 *
 * @retval 0 on success
 * @retval 1 when the value was not found
 * @retval 2 when the value is of a different type
 * @returns A (negative) libmseed error code on error
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mseh_get_path (MS3Record *msr, const char *path, void *value, char type, size_t maxlength)
{
  JSON_Object *rootobject = NULL;
  JSON_Value *rootvalue   = NULL;
  JSON_Value *extravalue  = NULL;
  JSON_Value_Type valuetype;
  const char *stringvalue = NULL;
  int retval              = 0;

  if (!msr || !path)
  {
    ms_log (2, "Required argument not defined: 'msr' or 'path'\n");
    return MS_GENERROR;
  }

  if (!msr->extralength)
    return 1;

  if (!msr->extra)
  {
    ms_log (2, "Expected extra headers (msr->extra) are not present\n");
    return MS_GENERROR;
  }

  json_set_allocation_functions (libmseed_memory.malloc,
                                 libmseed_memory.free);

  /* Parse JSON extra headers */
  rootvalue = json_parse_string (msr->extra);

  if (!rootvalue)
  {
    ms_log (2, "Extra headers are not JSON\n");
    return MS_GENERROR;
  }

  /* Get expected root object */
  rootobject = json_value_get_object (rootvalue);

  if (!rootobject)
  {
    ms_log (2, "Extra headers are not a JSON object\n");
    json_value_free (rootvalue);
    return MS_GENERROR;
  }

  /* Get target value */
  extravalue = json_object_dotget_value (rootobject, path);

  if (!extravalue)
  {
    json_value_free (rootvalue);
    return 1;
  }

  valuetype = json_value_get_type (extravalue);

  if (type == 'n' && valuetype == JSONNumber)
  {
    if (value)
      *((double *)value) = json_value_get_number (extravalue);
  }
  else if (type == 's' && valuetype == JSONString)
  {
    if (value)
    {
      stringvalue = json_value_get_string (extravalue);
      strncpy ((char *)value, stringvalue, maxlength - 1);
      ((char *)value)[maxlength - 1] = '\0';
    }
  }
  else if (type == 'b' && valuetype == JSONBoolean)
  {
    if (value)
      *((int *)value) = json_value_get_boolean (extravalue);
  }
  /* Return wrong type indicator if a value was requested */
  else if (value)
  {
    retval = 2;
  }

  json_value_free (rootvalue);

  return retval;
} /* End of mseh_get_path() */

/**********************************************************************/ /**
 * @brief Set the value of a single extra header value
 *
 * The extra header value is specified as a path in dot notation, e.g.
 * \c 'objectA.objectB.header'.
 *
 * If the \a path or final header values do not exist they will be
 * created.  If the header value exists it will be replaced.
 *
 * The \a type value specifies the data type expected for \c value.
 *
 * @param[in] msr Parsed miniSEED record to query
 * @param[in] path Header value desired, specified in dot notation
 * @param[in] value Buffer for value, of type \c type
 * @param[in] type Type of value expected, one of:
 * @parblock
 * - \c 'n' - \a value is type \a double
 * - \c 's' - \a value is type \a char* (maximum length is: \c maxlength - 1)
 * - \c 'b' - \a value is type \a int (boolean value of 0 or 1)
 * - \c 'A' - \a value is an Array element as JSON_Value*
 * @endparblock
 *
 * @retval 0 on success, otherwise a (negative) libmseed error code.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mseh_set_path (MS3Record *msr, const char *path, void *value, char type)
{
  JSON_Object *rootobject  = NULL;
  JSON_Value *rootvalue    = NULL;
  JSON_Array *array        = NULL;
  size_t serializationsize = 0;
  char *serialized         = NULL;

#define EVALSET(KEY, SET)                            \
  if (SET != JSONSuccess)                            \
  {                                                  \
    ms_log (2, "Cannot set header path: %s\n", KEY); \
    if (rootvalue)                                   \
      json_value_free (rootvalue);                   \
    return MS_GENERROR;                              \
  }

  if (!msr || !value || !path)
  {
    ms_log (2, "Required argument not defined: 'msr', 'value' or 'path'\n");
    return MS_GENERROR;
  }

  json_set_allocation_functions (libmseed_memory.malloc,
                                 libmseed_memory.free);

  /* Parse existing extra headers */
  if (msr->extra && msr->extralength > 0)
  {
    rootvalue = json_parse_string (msr->extra);

    if (!rootvalue)
    {
      ms_log (2, "Extra headers are not JSON\n");
      return MS_GENERROR;
    }

    /* Get expected root object */
    rootobject = json_value_get_object (rootvalue);

    if (!rootobject)
    {
      ms_log (2, "Extra headers are not a JSON object\n");
      json_value_free (rootvalue);
      return MS_GENERROR;
    }
  }
  /* If no existing headers, initialize a new base object */
  else
  {
    rootvalue  = json_value_init_object ();
    rootobject = json_value_get_object (rootvalue);

    if (!rootobject)
    {
      ms_log (2, "Cannot initialize new JSON object\n");
      if (rootvalue)
        json_value_free (rootvalue);
      return MS_GENERROR;
    }
  }

  switch (type)
  {
  case 'n':
    EVALSET (path, json_object_dotset_number (rootobject, path, *((double *)value)));
    break;
  case 's':
    EVALSET (path, json_object_dotset_string (rootobject, path, (const char *)value));
    break;
  case 'b':
    EVALSET (path, json_object_dotset_boolean (rootobject, path, *((int *)value)));
    break;
  case 'A':
    array = json_object_dotget_array (rootobject, path);

    /* Create array if it was not found */
    if (!array)
    {
      EVALSET (path, json_object_dotset_value (rootobject, path, json_value_init_array ()));

      array = json_object_dotget_array (rootobject, path);

      if (!array)
      {
        ms_log (2, "Cannot get extra header array\n");
        if (value)
          json_value_free (rootvalue);
        return MS_GENERROR;
      }
    }

    EVALSET ("Array JSON_Value", json_array_append_value (array, (JSON_Value *)value));
    break;
  default:
    ms_log (2, "Unrecognized type '%d'\n", type);
    json_value_free (rootvalue);
    return MS_GENERROR;
  }

  /* Serialize new JSON */
  serializationsize = json_serialization_size (rootvalue);

  if (!serializationsize)
  {
    ms_log (2, "Cannot determine new serialization size\n");
    json_value_free (rootvalue);
    return MS_GENERROR;
  }

  if (serializationsize > UINT16_MAX)
  {
    ms_log (2, "New serialization size exceeds limit of %d bytes: %" PRIu64 "\n",
            UINT16_MAX, (uint64_t)serializationsize);
    json_value_free (rootvalue);
    return MS_GENERROR;
  }

  serialized = (char *)libmseed_memory.malloc (serializationsize);

  if (!serialized)
  {
    ms_log (2, "Cannot determine new serialization size\n");
    json_value_free (rootvalue);
    return MS_GENERROR;
  }

  if (json_serialize_to_buffer (rootvalue, serialized, serializationsize) != JSONSuccess)
  {
    ms_log (2, "Error serializing JSON for extra headers\n");
    json_value_free (rootvalue);
    return MS_GENERROR;
  }

  if (rootvalue)
    json_value_free (rootvalue);

  /* Set new extra headers, replacing existing headers */
  if (msr->extra)
    libmseed_memory.free (msr->extra);
  msr->extra       = serialized;
  msr->extralength = (uint16_t)serializationsize - 1;

  msr->extra[serializationsize - 1] = '\0';

  return 0;
#undef EVALSET
} /* End of mseh_set_path() */

/**********************************************************************/ /**
 * @brief Add event detection to the extra headers of the given record.
 *
 * If \a path is NULL, the default is \c 'FDSN.Event.Detection'.
 *
 * @param[in] msr Parsed miniSEED record to query
 * @param[in] path Header value desired, specified in dot notation
 * @param[in] eventdetection Structure with event detection values
 *
 * @returns 0 on success, otherwise a (negative) libmseed error code.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mseh_add_event_detection (MS3Record *msr, const char *path,
                          MSEHEventDetection *eventdetection)
{
  JSON_Value *value   = NULL;
  JSON_Object *object = NULL;
  JSON_Array *array   = NULL;
  char timestring[30];

#define EVALSET(KEY, SET)                            \
  if (SET != JSONSuccess)                            \
  {                                                  \
    ms_log (2, "Cannot set header path: %s\n", KEY); \
    if (value)                                       \
      json_value_free (value);                       \
    return MS_GENERROR;                              \
  }

  if (!msr || !eventdetection)
  {
    ms_log (2, "Required argument not defined: 'msr' or 'eventdetection'\n");
    return MS_GENERROR;
  }

  json_set_allocation_functions (libmseed_memory.malloc,
                                 libmseed_memory.free);

  /* Initialize a new object */
  value  = json_value_init_object ();
  object = json_value_get_object (value);

  if (!object)
  {
    ms_log (2, "Cannot initialize new JSON object\n");
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  /* Add elements to new object */
  if (eventdetection->type[0])
  {
    EVALSET ("Type", json_object_set_string (object, "Type", eventdetection->type));
  }
  if (eventdetection->signalamplitude != 0.0)
  {
    EVALSET ("SignalAmplitude", json_object_set_number (object, "SignalAmplitude", eventdetection->signalamplitude));
  }
  if (eventdetection->signalperiod != 0.0)
  {
    EVALSET ("SignalPeriod", json_object_set_number (object, "SignalPeriod", eventdetection->signalperiod));
  }
  if (eventdetection->backgroundestimate != 0.0)
  {
    EVALSET ("BackgroundEstimate", json_object_set_number (object, "BackgroundEstimate", eventdetection->backgroundestimate));
  }
  if (eventdetection->wave[0])
  {
    EVALSET ("Wave", json_object_set_string (object, "Wave", eventdetection->wave));
  }
  if (eventdetection->units[0])
  {
    EVALSET ("Units", json_object_set_string (object, "Units", eventdetection->units));
  }
  if (eventdetection->onsettime != NSTERROR)
  {
    if (ms_nstime2timestrz (eventdetection->onsettime, timestring, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      EVALSET ("OnsetTime", json_object_set_string (object, "OnsetTime", timestring));
    }
    else
    {
      ms_log (2, "Cannot create time string for %"PRId64"\n", eventdetection->onsettime);
      return MS_GENERROR;
    }
  }
  if (memcmp (eventdetection->medsnr, (uint8_t[]){0, 0, 0, 0, 0, 0}, 6))
  {
    EVALSET ("MEDSNR", json_object_set_value (object, "MEDSNR", json_value_init_array ()));

    array = json_object_get_array (object, "MEDSNR");

    if (!array)
    {
      ms_log (2, "Cannot get MEDSNR array\n");
      if (value)
        json_value_free (value);
      return MS_GENERROR;
    }

    /* Array containing the 6 SNR values */
    EVALSET ("MEDSNR-0", json_array_append_number (array, (double)eventdetection->medsnr[0]));
    EVALSET ("MEDSNR-1", json_array_append_number (array, (double)eventdetection->medsnr[1]));
    EVALSET ("MEDSNR-2", json_array_append_number (array, (double)eventdetection->medsnr[2]));
    EVALSET ("MEDSNR-3", json_array_append_number (array, (double)eventdetection->medsnr[3]));
    EVALSET ("MEDSNR-4", json_array_append_number (array, (double)eventdetection->medsnr[4]));
    EVALSET ("MEDSNR-5", json_array_append_number (array, (double)eventdetection->medsnr[5]));
  }
  if (eventdetection->medlookback >= 0)
  {
    EVALSET ("MEDLookback", json_object_set_number (object, "MEDLookback", (double)eventdetection->medlookback));
  }
  if (eventdetection->medpickalgorithm >= 0)
  {
    EVALSET ("MEDPickAlgorithm", json_object_set_number (object, "MEDPickAlgorithm", (double)eventdetection->medpickalgorithm));
  }
  if (eventdetection->detector[0])
  {
    EVALSET ("Detector", json_object_set_string (object, "Detector", eventdetection->detector));
  }

  /* Add new object to array, created 'value' will be free'd on successful return */
  if (mseh_set_path (msr, (path) ? path : "FDSN.Event.Detection", value, 'A'))
  {
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  return 0;
#undef EVALSET
} /* End of mseh_add_event_detection() */

/**********************************************************************/ /**
 * @brief Add calibration to the extra headers of the given record.
 *
 * If \a path is NULL, the default is \c 'FDSN.Calibration.Sequence'.
 *
 * @param[in] msr Parsed miniSEED record to query
 * @param[in] path Header value desired, specified in dot notation
 * @param[in] calibration Structure with calibration values
 *
 * @returns 0 on success, otherwise a (negative) libmseed error code.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mseh_add_calibration (MS3Record *msr, const char *path,
                      MSEHCalibration *calibration)
{
  JSON_Value *value   = NULL;
  JSON_Object *object = NULL;
  char beginstring[31];
  char endstring[31];

#define EVALSET(KEY, SET)                            \
  if (SET != JSONSuccess)                            \
  {                                                  \
    ms_log (2, "Cannot set header path: %s\n", KEY); \
    if (value)                                       \
      json_value_free (value);                       \
    return MS_GENERROR;                              \
  }

  if (!msr || !calibration)
  {
    ms_log (2, "Required argument not defined: 'msr' or 'calibration'\n");
    return MS_GENERROR;
  }

  json_set_allocation_functions (libmseed_memory.malloc,
                                 libmseed_memory.free);

  /* Initialize a new object */
  value  = json_value_init_object ();
  object = json_value_get_object (value);

  if (!object)
  {
    ms_log (2, "Cannot initialize new JSON object\n");
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  /* Add elements to new object */
  if (calibration->type[0])
  {
    EVALSET ("Type", json_object_set_string (object, "Type", calibration->type));
  }
  if (calibration->begintime != NSTERROR)
  {
    if (ms_nstime2timestrz (calibration->begintime, beginstring, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      EVALSET ("BeginTime", json_object_set_string (object, "BeginTime", beginstring));
    }
    else
    {
      return MS_GENERROR;
    }
  }
  if (calibration->endtime != NSTERROR)
  {
    if (ms_nstime2timestrz (calibration->endtime, endstring, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      EVALSET ("EndTime", json_object_set_string (object, "EndTime", endstring));
    }
    else
    {
      return MS_GENERROR;
    }
  }
  if (calibration->steps >= 0)
  {
    EVALSET ("Steps", json_object_set_number (object, "Steps", (double)calibration->steps));
  }
  if (calibration->firstpulsepositive >= 0)
  {
    EVALSET ("FirstPulsePositive", json_object_set_boolean (object, "FirstPulsePositive", calibration->firstpulsepositive));
  }
  if (calibration->alternatesign >= 0)
  {
    EVALSET ("AlternateSign", json_object_set_boolean (object, "AlternateSign", calibration->alternatesign));
  }
  if (calibration->trigger[0])
  {
    EVALSET ("Trigger", json_object_set_string (object, "Trigger", calibration->trigger));
  }
  if (calibration->continued >= 0)
  {
    EVALSET ("Continued", json_object_set_boolean (object, "Continued", calibration->continued));
  }
  if (calibration->amplitude != 0.0)
  {
    EVALSET ("Amplitude", json_object_set_number (object, "Amplitude", calibration->amplitude));
  }
  if (calibration->inputunits[0])
  {
    EVALSET ("InputUnits", json_object_set_string (object, "InputUnits", calibration->inputunits));
  }
  if (calibration->amplituderange[0])
  {
    EVALSET ("AmplitudeRange", json_object_set_string (object, "AmplitudeRange", calibration->amplituderange));
  }
  if (calibration->duration != 0.0)
  {
    EVALSET ("Duration", json_object_set_number (object, "Duration", calibration->duration));
  }
  if (calibration->sineperiod != 0.0)
  {
    EVALSET ("SinePeriod", json_object_set_number (object, "SinePeriod", calibration->sineperiod));
  }
  if (calibration->stepbetween != 0.0)
  {
    EVALSET ("StepBetween", json_object_set_number (object, "StepBetween", calibration->stepbetween));
  }
  if (calibration->inputchannel[0])
  {
    EVALSET ("InputChannel", json_object_set_string (object, "InputChannel", calibration->inputchannel));
  }
  if (calibration->refamplitude != 0.0)
  {
    EVALSET ("ReferenceAmplitude", json_object_set_number (object, "ReferenceAmplitude", calibration->refamplitude));
  }
  if (calibration->coupling[0])
  {
    EVALSET ("Coupling", json_object_set_string (object, "Coupling", calibration->coupling));
  }
  if (calibration->rolloff[0])
  {
    EVALSET ("Rolloff", json_object_set_string (object, "Rolloff", calibration->rolloff));
  }
  if (calibration->noise[0])
  {
    EVALSET ("Noise", json_object_set_string (object, "Noise", calibration->noise));
  }

  /* Add new object to array, created 'value' will be free'd on successful return */
  if (mseh_set_path (msr, (path) ? path : "FDSN.Calibration.Sequence", value, 'A'))
  {
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  return 0;
#undef EVALSET
} /* End of mseh_add_calibration() */

/**********************************************************************/ /**
 * @brief Add timing exception to the extra headers of the given record.
 *
 * If \a path is NULL, the default is \c 'FDSN.Time.Exception'.
 *
 * @param[in] msr Parsed miniSEED record to query
 * @param[in] path Header value desired, specified in dot notation
 * @param[in] exception Structure with timing exception values
 *
 * @returns 0 on success, otherwise a (negative) libmseed error code.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mseh_add_timing_exception (MS3Record *msr, const char *path,
                           MSEHTimingException *exception)
{
  JSON_Value *value   = NULL;
  JSON_Object *object = NULL;
  char timestring[30];

#define EVALSET(KEY, SET)                            \
  if (SET != JSONSuccess)                            \
  {                                                  \
    ms_log (2, "Cannot set header path: %s\n", KEY); \
    if (value)                                       \
      json_value_free (value);                       \
    return MS_GENERROR;                              \
  }

  if (!msr || !exception)
  {
    ms_log (2, "Required argument not defined: 'msr' or 'exception'\n");
    return MS_GENERROR;
  }

  json_set_allocation_functions (libmseed_memory.malloc,
                                 libmseed_memory.free);

  /* Initialize a new object */
  value  = json_value_init_object ();
  object = json_value_get_object (value);

  if (!object)
  {
    ms_log (2, "Cannot initialize new JSON object\n");
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  /* Add elements to new object */
  if (exception->vcocorrection >= 0.0)
  {
    EVALSET ("VCOCorrection", json_object_set_number (object, "VCOCorrection", (double)exception->vcocorrection));
  }
  if (exception->time != NSTERROR)
  {
    if (ms_nstime2timestrz (exception->time, timestring, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      EVALSET ("Time", json_object_set_string (object, "Time", timestring));
    }
    else
    {
      return MS_GENERROR;
    }
  }
  if (exception->receptionquality >= 0)
  {
    EVALSET ("ReceptionQuality", json_object_set_number (object, "ReceptionQuality", (double)exception->receptionquality));
  }
  if (exception->count > 0)
  {
    EVALSET ("Count", json_object_set_number (object, "Count", (double)exception->count));
  }
  if (exception->type[0])
  {
    EVALSET ("Type", json_object_set_string (object, "Type", exception->type));
  }
  if (exception->clockstatus[0])
  {
    EVALSET ("ClockStatus", json_object_set_string (object, "ClockStatus", exception->clockstatus));
  }

  /* Add new object to array, created 'value' will be free'd on successful return */
  if (mseh_set_path (msr, (path) ? path : "FDSN.Time.Exception", value, 'A'))
  {
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  return 0;
#undef EVALSET
} /* End of mseh_add_timing_exception() */

/**********************************************************************/ /**
 * @brief Add recenter event to the extra headers of the given record.
 *
 * If \a path is NULL, the default is \c 'FDSN.Recenter.Sequence'.
 *
 * @param[in] msr Parsed miniSEED record to query
 * @param[in] path Header value desired, specified in dot notation
 * @param[in] recenter Structure with recenter values
 *
 * @returns 0 on success, otherwise a (negative) libmseed error code.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
mseh_add_recenter (MS3Record *msr, const char *path, MSEHRecenter *recenter)
{
  JSON_Value *value   = NULL;
  JSON_Object *object = NULL;
  char beginstring[31];
  char endstring[31];

#define EVALSET(KEY, SET)                            \
  if (SET != JSONSuccess)                            \
  {                                                  \
    ms_log (2, "Cannot set header path: %s\n", KEY); \
    if (value)                                       \
      json_value_free (value);                       \
    return MS_GENERROR;                              \
  }

  if (!msr || !recenter)
  {
    ms_log (2, "Required argument not defined: 'msr' or 'recenter'\n");
    return MS_GENERROR;
  }

  json_set_allocation_functions (libmseed_memory.malloc,
                                 libmseed_memory.free);

  /* Initialize a new object */
  value  = json_value_init_object ();
  object = json_value_get_object (value);

  if (!object)
  {
    ms_log (2, "Cannot initialize new JSON object\n");
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  /* Add elements to new object */
  if (recenter->type[0])
  {
    EVALSET ("Type", json_object_set_string (object, "Type", recenter->type));
  }
  if (recenter->begintime != NSTERROR)
  {
    if (ms_nstime2timestrz (recenter->begintime, beginstring, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      EVALSET ("BeginTime", json_object_set_string (object, "BeginTime", beginstring));
    }
    else
    {
      return MS_GENERROR;
    }
  }
  if (recenter->endtime != NSTERROR)
  {
    if (ms_nstime2timestrz (recenter->endtime, endstring, ISOMONTHDAY, NANO_MICRO_NONE))
    {
      EVALSET ("EndTime", json_object_set_string (object, "EndTime", endstring));
    }
    else
    {
      return MS_GENERROR;
    }
  }
  if (recenter->trigger[0])
  {
    EVALSET ("Trigger", json_object_set_string (object, "Trigger", recenter->trigger));
  }

  /* Add new object to array, created 'value' will be free'd on successful return */
  if (mseh_set_path (msr, (path) ? path : "FDSN.Recenter.Sequence", value, 'A'))
  {
    if (value)
      json_value_free (value);
    return MS_GENERROR;
  }

  return 0;
#undef EVALSET
} /* End of mseh_add_recenter() */

/**********************************************************************/ /**
 * @brief Print the extra header structure for the specified MS3Record.
 *
 * Output is printed in a pretty, formatted form for readability and
 * the anonymous, root object container is not printed.
 *
 * @returns 0 on success and a (negative) libmseed error code on error.
 ***************************************************************************/
int
mseh_print (MS3Record *msr, int indent)
{
  char *extra;
  int idx;
  int instring = 0;

  if (!msr)
    return MS_GENERROR;

  if (!msr->extra || !msr->extralength)
    return MS_NOERROR;

  extra = msr->extra;

  if (extra[0] != '{' || extra[msr->extralength - 1] != '}')
  {
    ms_log (1, "Warning, something is wrong, extra headers are not a clean {object}\n");
  }

  /* Print JSON character-by-character, inserting
   * indentation, spaces and newlines for readability. */
  ms_log (0, "%*s", indent, "");
  for (idx = 1; idx < (msr->extralength - 1); idx++)
  {
    /* Toggle "in string" flag for double quotes */
    if (extra[idx] == '"')
      instring = (instring) ? 0 : 1;

    if (!instring)
    {
      if (extra[idx] == ':')
      {
        ms_log (0, ": ");
      }
      else if (extra[idx] == ',')
      {
        ms_log (0, ",\n%*s", indent, "");
      }
      else if (extra[idx] == '{')
      {
        indent += 2;
        ms_log (0, "{\n%*s", indent, "");
      }
      else if (extra[idx] == '[')
      {
        indent += 2;
        ms_log (0, "[\n%*s", indent, "");
      }
      else if (extra[idx] == '}')
      {
        indent -= 2;
        ms_log (0, "\n%*s}", indent, "");
      }
      else if (extra[idx] == ']')
      {
        indent -= 2;
        ms_log (0, "\n%*s]", indent, "");
      }
      else
      {
        ms_log (0, "%c", extra[idx]);
      }
    }
    else
    {
      ms_log (0, "%c", extra[idx]);
    }
  }
  ms_log (0, "\n");

  return MS_NOERROR;
} /* End of mseh_print() */
