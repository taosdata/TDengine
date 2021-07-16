package com.taosdata.tsync.utils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public final class Utils {

    private Utils() {
    }

    public static Range<Integer> closedRange(String rangeStr) {
        if (rangeStr.contains("..")) {
            String[] split = rangeStr.split("\\.\\.");
            int lowerEndpoint = Integer.parseInt(split[0]);
            int upperEndpoint = Integer.parseInt(split[1]);
            return Range.closed(lowerEndpoint, upperEndpoint);
        }
        return Range.closed(Integer.parseInt(rangeStr), Integer.parseInt(rangeStr));
    }

    public static long toMicroSecond(Timestamp timestamp) {
        long high13digits = timestamp.getTime();
        long low3digits = timestamp.getNanos() % 1000_000l / 1000;
        return high13digits * 1000 + low3digits;
    }

    /**
     * 把从 startInclude 到 endExclude 的数，按照每组 groupOfN 个，分为若干组
     */
    public static Map<Long, Range<Long>> divideIntoGroupsOfN(long startInclude, long endExclude, long groupOfN) {
        Map<Long, Range<Long>> batchMap = new HashMap<>();
        for (long batchIndex = 0, ind = startInclude; ind < endExclude; batchIndex++, ind += groupOfN) {
            long lower = ind;
            long upper = Math.min(ind + groupOfN, endExclude);
            batchMap.put(batchIndex, Range.closedOpen(lower, upper));
        }
        return batchMap;
    }


    public static Map<Long, Range<Long>> divideIntoArrGroups(long many, long[] groups) {
        Map<Long, Range<Long>> map = new HashMap<>();

        final long gap = Math.round((0.0d + many) / groups.length);
        IntStream.range(0, groups.length).forEach(index -> {

            long startInd = index * gap;
            long endInd = Math.min(((index + 1) * gap), many);

            if (startInd >= many) {
                map.put(groups[index], Range.closedOpen(many, many));
            } else if (index == groups.length - 1) {
                map.put(groups[index], Range.closedOpen(startInd, many));
            } else {
                map.put(groups[index], Range.closedOpen(startInd, endInd));
            }
        });
        return map;
    }

    public static List<Integer[]> divideArrayIntoGroups(int[] arr, int groups) {
        List<Integer[]> list = new ArrayList<>();

        List<Long> sizePerGroup = divideIntoGroups(arr.length, groups);
        int offset = 0;
        for (int i = 0; i < sizePerGroup.size(); i++) {
            int start = offset;
            int end = start + sizePerGroup.get(i).intValue();
            int[] subArr = Arrays.copyOfRange(arr, start, end);
            list.add(Arrays.stream(subArr).boxed().toArray(Integer[]::new));
            offset += sizePerGroup.get(i).intValue();
        }

        return list;
    }

    public static List<Range<Long>> divideIntoRangeList(long number, long groups) {
        List<Range<Long>> ranges = new ArrayList<>();
        if (number < groups) {
            return LongStream.range(0, number).mapToObj(i -> Range.closedOpen(i, i + 1)).collect(Collectors.toList());
        }

        final long gap = Math.round((0.0d + number) / groups);
        LongStream.range(0, groups).forEach(index -> {
            long startInd = index * gap;
            long endInd = Math.min(((index + 1) * gap), number);
            if (startInd >= number) {
                ranges.add(Range.closedOpen(number, number));
            } else if (index == groups - 1) {
                ranges.add(Range.closedOpen(startInd, number));
            } else {
                ranges.add(Range.closedOpen(startInd, endInd));
            }
        });
        return ranges.stream().filter(range -> !range.isEmpty()).collect(Collectors.toList());
    }

    public static List<Long> divideIntoGroups(long many, long groups) {
        List<Range<Long>> ranges = divideIntoRangeList(many, (int) groups);
        return ranges.stream().map(range -> range.upperEndpoint() - range.lowerEndpoint()).collect(Collectors.toList());
    }

    public static Map<Long, Long> divideIntoGroups(long many, Range<Long> range) {
        Map<Long, Long> map = new HashMap<>();
        List<Long> groups = divideIntoGroups(many, range.upperEndpoint() - range.lowerEndpoint());
        IntStream.range(0, groups.size()).forEach(i -> map.put(range.lowerEndpoint() + i, groups.get(i)));
        return map;
    }

    public static Map<Integer, Range<Long>> divideRangeIntoArrayGroups(Range<Long> range, int[] arr) {
        Map<Integer, Range<Long>> map = new HashMap<>();
        List<Range<Long>> ranges = divideRangeIntoGroups(range, arr.length);
        for (int i = 0; i < ranges.size(); i++) {
            map.put(arr[i], ranges.get(i));
        }
        return map;
    }

    public static List<Range<Long>> divideRangeIntoGroups(Range<Long> range, int groups) {
        List<Range<Long>> ranges = new ArrayList<>();
        long rangSize = range.upperEndpoint() - range.lowerEndpoint();
        List<Long> list = divideIntoGroups(rangSize, groups);

        long offset = 0;
        for (int i = 0; i < list.size(); i++) {
            long start = range.lowerEndpoint() + offset;
            long end = start + list.get(i);
            ranges.add(Range.closedOpen(start, end));
            offset += list.get(i);
        }
        return ranges;
    }
}
