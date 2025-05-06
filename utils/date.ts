import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import duration from 'dayjs/plugin/duration';
import isSameOrBefore from 'dayjs/plugin/isSameOrBefore';
import isSameOrAfter from 'dayjs/plugin/isSameOrAfter';
import isBetween from 'dayjs/plugin/isBetween';

import { t } from 'locales';
import dayJs, { type ManipulateType, type Dayjs } from 'dayjs';

dayJs.extend(utc);
dayJs.extend(timezone);
dayJs.extend(duration);
dayJs.extend(isSameOrBefore);
dayJs.extend(isSameOrAfter);
dayJs.extend(isBetween);

// 设置默认时区
export function setTimezone(timezone: string): void {
  dayJs.tz.setDefault(timezone);
}

export default dayJs;

/**
 * Whether the recharging date time is valid
 *
 * @returns {boolean}
 */
export function invalidRecharge(): boolean {
  const currentDate = dayJs();
  return currentDate.date() === 1 && currentDate.hour() === 0;
}

/**
 * Parse the time to string
 *
 * @param {(Object|string|number)} time
 * @param {string} pattern
 * @returns {string}
 */
export function parseTime(time?: DateType | null, pattern = 'YYYY-MM-DD HH:mm:ss'): string {
  if (time === null) return '';
  const timeNumber = Number(time);
  return dayJs.tz(isNaN(timeNumber) ? time : timeNumber).format(pattern) || '';
}

/**
 * Get the duration time from now
 *
 * @param {number} time
 * @param {string} option
 * @returns {string}
 */
export function getDurationFromNow(time: string | number, pattern?: string): string {
  if (('' + time).length === 10) {
    time = parseInt(time) * 1000;
  } else {
    time = +time;
  }
  const d: Date = new Date(time);
  const now: number = Date.now();

  const diff: number = (now - d.getTime()) / 1000;

  if (diff < 30) {
    return '刚刚';
  } else if (diff < 3600) {
    // less 1 hour
    return Math.ceil(diff / 60) + '分钟前';
  } else if (diff < 3600 * 24) {
    return Math.ceil(diff / 3600) + '小时前';
  } else if (diff < 3600 * 24 * 2) {
    return '1天前';
  }
  if (pattern) {
    return parseTime(time, pattern);
  } else {
    return d.getMonth() + 1 + '月' + d.getDate() + '日' + d.getHours() + '时' + d.getMinutes() + '分';
  }
}

/**
 * Get the precise duration time from start time to end time or duration milliseconds
 *
 * @param {number} durationMs duration in milliseconds
 * @param {string} pattern:
 * @returns {string}
 */
export function getPreciseDurationFromNow(
  durationMs: number,
  fromTime?: number,
  toTime?: number,
  noMs?: boolean
): string {
  if (!durationMs || durationMs <= 0) return '';
  const duration = dayJs.duration(durationMs);
  const years = Math.floor(duration.asYears());
  const months = duration.months();
  const days = duration.days();
  const hours = duration.hours();
  const minutes = duration.minutes();
  const seconds = duration.seconds();
  const milliseconds = duration.milliseconds();
  let formattedDuration = '';
  if (years > 0) {
    formattedDuration += `${years} ${t('date.duration.year')}  `;
  }
  if (months > 0) {
    formattedDuration += `${months} ${t('date.duration.month')}  `;
  }
  if (days > 0) {
    formattedDuration += `${days} ${t('date.duration.day')}  `;
  }
  if (hours > 0) {
    formattedDuration += `${hours} ${t('date.duration.hours')}  `;
  }
  if (minutes > 0) {
    formattedDuration += `${minutes} ${t('date.duration.minutes')}  `;
  }
  if (seconds > 0) {
    formattedDuration += `${seconds} ${t('date.duration.seconds')}  `;
  }
  if (!noMs && milliseconds > 0) {
    formattedDuration += `${milliseconds} ${t('date.duration.milliseconds')}`;
  }
  if (fromTime && toTime && fromTime.toString().length > 13 && fromTime.toString().length <= 16) {
    let diffMicroseconds = Number(BigInt(String(fromTime)) - BigInt(String(toTime))); // eslint-disable-line
    diffMicroseconds = diffMicroseconds % 1000;
    if (diffMicroseconds > 0) {
      formattedDuration += diffMicroseconds + t('date.duration.microseconds');
    }
  }

  if (fromTime && toTime && fromTime.toString().length >= 19) {
    let diffNanoseconds = Number(BigInt(String(fromTime)) - BigInt(String(toTime))); // eslint-disable-line
    diffNanoseconds = diffNanoseconds % 1000;
    if (diffNanoseconds > 0) {
      formattedDuration += diffNanoseconds + t('date.duration.nanoseconds');
    }
  }

  return formattedDuration;
}

/**
 * 判断时间戳位数
 *
 * @param timestamp
 * @returns {number}
 */
export function convertTsToMilliseconds(timestamp: number): number {
  if (timestamp && timestamp.toString().length >= 19) {
    return Number(String(timestamp / 1000000).split('.')[0]);
  } else if (timestamp && timestamp.toString().length > 13 && timestamp.toString().length <= 16) {
    return Number(String(timestamp / 1000).split('.')[0]);
  } else {
    return timestamp;
  }
}
/**
 * 格式化日期并考虑时区
 * @param time
 * @param pattern
 * @returns
 */
export function formatDateInTimeZone(time?: DateType | null, pattern = ''): string {
  if (time === null) return '';
  const timeNumber = Number(time);
  return (
    dayJs(isNaN(timeNumber) ? time : timeNumber)
      .tz()
      .format(pattern) || ''
  );
}

/**
 * Get the time in the specified format
 * @param format='YYYY-MM-DD HH:mm:ss'
 */
export function handleDateTime(time: DateType, format = 'YYYY-MM-DD HH:mm:ss') {
  return parseTime(time, format);
}

/**
 * Get the time in the specified format
 * @param format='YYYY-MM-DD'
 */
export function handleDate(time: DateType, format = 'YYYY-MM-DD') {
  return parseTime(time, format);
}

interface ResultDateMap {
  string: string;
  Date: Date;
  Dayjs: Dayjs;
  number: number;
}
type ResultType = keyof ResultDateMap;
type ResultDate<T extends ResultType> = ResultDateMap[T];
interface startAndEndDate {
  time?: Parameters<typeof dayJs>[0];
  unit?: ManipulateType;
  mode?: 'add' | 'subtract';
  isTime?: boolean;
  num?: number;
  resultType?: ResultType;
  format?: string;
}

/**
 * 获取从指定时间的开始时间和结束时间
 */
export function getStartAndEndDate<T extends ResultType>({
  num = 0,
  unit = 'd',
  time = Date.now(),
  mode = 'add',
  isTime = false,
  resultType = 'string' as T,
  format = 'YYYY-MM-DD HH:mm:ss'
}: startAndEndDate & { resultType: T }): [ResultDate<T>, ResultDate<T>] {
  const currentDayjs = isTime ? dayJs.tz(time) : dayJs.tz(dayJs.tz(time).format('YYYY-MM-DD') + ' 23:59:59');
  const beforeDayjs = (
    isTime ? currentDayjs[mode](num, unit) : currentDayjs[mode](num, unit).hour(0).minute(0).second(0)
  ).tz();
  switch (resultType) {
    case 'string':
      return [beforeDayjs.format(format), currentDayjs.format(format)] as [ResultDate<T>, ResultDate<T>];
    case 'Date':
      return [new Date(beforeDayjs.format(format)), new Date(currentDayjs.format(format))] as [
        ResultDate<T>,
        ResultDate<T>
      ];
    case 'Dayjs':
      return [beforeDayjs, currentDayjs] as [ResultDate<T>, ResultDate<T>];
    case 'number':
      return [beforeDayjs.valueOf(), currentDayjs.valueOf()] as [ResultDate<T>, ResultDate<T>];
    default:
      return [beforeDayjs.format(format), currentDayjs.format(format)] as [ResultDate<T>, ResultDate<T>];
  }
}

const timeUnits = ['ns', 'μs', 'ms', 's', 'min', 'h', 'd', 'w', 'M', 'y'];
const timeNums = [1000, 1000, 1000, 60, 60, 24, 7, 4.35, 12];
/**
 * 转换时间单位
 */
export function transformTime(time: string | number, unit = 's', split = false): string | [number, string] {
  if (typeof time === 'string') {
    time = parseInt(time);
  }
  let index = timeUnits.indexOf(unit);
  while (index < timeUnits.length - 1 && time > timeNums[index]) {
    time = time / timeNums[index];
    index++;
  }
  if (split) {
    return [time, timeUnits[index]];
  }
  return time + ' ' + timeUnits[index];
}

/**
 *日期范围快捷键
 *日、周、月
 */
export const DateRangePirckerShortcuts = [
  {
    text: t('date.yesterday'),
    value: () => {
      return getStartAndEndDate({ num: 1, unit: 'd', mode: 'subtract', resultType: 'Date' });
    }
  },
  {
    text: t('date.lastWeek'),
    value: () => {
      return getStartAndEndDate({ num: 7, unit: 'd', mode: 'subtract', resultType: 'Date' });
    }
  },
  {
    text: t('date.lastMonth'),
    value: () => {
      return getStartAndEndDate({ num: 1, unit: 'M', mode: 'subtract', resultType: 'Date' });
    }
  },
  {
    text: t('date.lastThreeMonths'),
    value: () => {
      return getStartAndEndDate({ num: 3, unit: 'M', mode: 'subtract', resultType: 'Date' });
    }
  }
];

/**
 * 日期时间范围快捷键
 * 单位：小时
 */

export const DateTimeRangePickerShortcuts = [
  {
    text: t('date.lastHour'),
    value: () => {
      return getStartAndEndDate({ num: 1, unit: 'h', mode: 'subtract', resultType: 'Date', isTime: true });
    }
  },
  {
    text: t('date.lastThreeHours'),
    value: () => {
      return getStartAndEndDate({ num: 3, unit: 'h', mode: 'subtract', resultType: 'Date', isTime: true });
    }
  },
  {
    text: t('date.lastSixHours'),
    value: () => {
      return getStartAndEndDate({ num: 6, unit: 'h', mode: 'subtract', resultType: 'Date', isTime: true });
    }
  },
  {
    text: t('date.lastTwelveHours'),
    value: () => {
      return getStartAndEndDate({ num: 12, unit: 'h', mode: 'subtract', resultType: 'Date', isTime: true });
    }
  }
].concat(DateRangePirckerShortcuts);

/**
 * 一周内的日期范围快捷键
 * 单位：天
 */
export const DateRangePirckerShortcutsInWeek = [
  {
    text: t('date.today'),
    value: () => {
      return getStartAndEndDate({ num: 0, unit: 'd', mode: 'subtract', resultType: 'Date' });
    }
  },
  {
    text: t('date.yesterday'),
    value: () => {
      return getStartAndEndDate({ num: 1, unit: 'd', mode: 'subtract', resultType: 'Date' });
    }
  },
  {
    text: t('date.lastThreeDays'),
    value: () => {
      return getStartAndEndDate({ num: 3, unit: 'd', mode: 'subtract', resultType: 'Date' });
    }
  },
  {
    text: t('date.lastWeek'),
    value: () => {
      return getStartAndEndDate({ num: 7, unit: 'd', mode: 'subtract', resultType: 'Date' });
    }
  }
];
