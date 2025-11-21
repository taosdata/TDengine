#include "ttqTime.h"

#include <unistd.h>

#include "tmqttInt.h"

#if _POSIX_TIMERS > 0 && defined(_POSIX_MONOTONIC_CLOCK)
static clockid_t time_clock;
#endif

void tmqtt_time_init(void) {
#if _POSIX_TIMERS > 0 && defined(_POSIX_MONOTONIC_CLOCK)
  struct timespec tp;

#ifdef CLOCK_BOOTTIME
  if (clock_gettime(CLOCK_BOOTTIME, &tp) == 0) {
    time_clock = CLOCK_BOOTTIME;
  } else {
    time_clock = CLOCK_MONOTONIC;
  }
#else
  time_clock = CLOCK_MONOTONIC;
#endif
#endif
}

time_t tmqtt_time(void) {
#if _POSIX_TIMERS > 0 && defined(_POSIX_MONOTONIC_CLOCK)
  struct timespec tp;

  if (clock_gettime(time_clock, &tp) == 0) return tp.tv_sec;

  return (time_t)-1;
#elif defined(__APPLE__)
  static mach_timebase_info_data_t tb;
  uint64_t                         ticks;
  uint64_t                         sec;

  ticks = mach_absolute_time();

  if (tb.denom == 0) {
    mach_timebase_info(&tb);
  }
  sec = ticks * tb.numer / tb.denom / 1000000000;

  return (time_t)sec;
#else
  return time(NULL);
#endif
}
