package com.taosdata.tsync.utils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
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
     * 把 many ,按照每组 groupsOfN 个，分为若干组
     *
     * @param many
     * @param groupOfN
     * @return
     */
    public static Map<Long, Long> divideIntoGroupsOfN(long many, long groupOfN) {
        Map<Long, Long> map = new HashMap<>();
        for (long batchIndex = 0, ind = 0; ind < many; batchIndex++, ind += groupOfN) {
            long lower = ind;
            long upper = Math.min(ind + groupOfN, many);
            map.put(batchIndex, upper - lower);
        }
        return map;
    }

    /**
     * 把从 startInclude 到 endExclude 的数，按照每组 groupOfN 个，分为若干组
     *
     * @param startInclude
     * @param endExclude
     * @param groupOfN
     * @return
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

    /***
     * 把一个数平均的分成n组
     * @param many 被分的数
     * @param groups 组数
     * @return
     */
    public static Map<Integer, Range<Long>> divideIntoGroups(long many, int groups) {
        Map<Integer, Range<Long>> map = new HashMap<>();

        final long gap = Math.round((0.0d + many) / groups);
        IntStream.range(0, groups).forEach(index -> {
            long startInd = index * gap;
            long endInd = Math.min(((index + 1) * gap), many);
            if (startInd >= many) {
                map.put(index, Range.closedOpen(many, many));
            } else if (index == groups - 1) {
                map.put(index, Range.closedOpen(startInd, many));
            } else {
                map.put(index, Range.closedOpen(startInd, endInd));
            }
        });
        return map;
    }

    public static Map<Long, Range<Long>> divideIntoGroups(long many, long groups) {
        Map<Long, Range<Long>> map = new HashMap<>();

        final long gap = (int) Math.round((0.0d + many) / groups);
        LongStream.range(0, groups).forEach(index -> {
            long startInd = index * gap;
            long endInd = Math.min(((index + 1) * gap), many);
            if (startInd >= many) {
                map.put(index, Range.closedOpen(many, many));
            } else if (index == groups - 1) {
                map.put(index, Range.closedOpen(startInd, many));
            } else {
                map.put(index, Range.closedOpen(startInd, endInd));
            }
        });
        return map;
    }

    public static Map<Integer, Range<Long>> divideIntoArrGroups(long many, int[] groups) {
        Map<Integer, Range<Long>> map = new HashMap<>();

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

    public static Map<Integer, Range<Long>> divideIntoArrGroups(long startInclude, long endExclude, int[] groups) {
        Map<Integer, Range<Long>> map = new HashMap<>();

        long manySize = endExclude - startInclude;
        final long gap = Math.round((0.0d + manySize) / groups.length);
        IntStream.range(0, groups.length).forEach(index -> {
            long startInd = startInclude + index * gap;
            long endInd = Math.min(startInclude + (index + 1) * gap, endExclude);
            if (startInd >= endExclude) {
                map.put(groups[index], Range.closedOpen(manySize, endExclude));
            } else if (index == groups.length - 1) {
                map.put(groups[index], Range.closedOpen(startInd, endExclude));
            } else {
                map.put(groups[index], Range.closedOpen(startInd, endInd));
            }
        });
        return map;
    }

    public static Multimap<Integer, Integer> divideArrIntoGroups(int[] arr, int groups) {
        Multimap<Integer, Integer> multimap = HashMultimap.create();
        final int gap = (int) Math.round((0.0d + arr.length) / groups);
        IntStream.range(0, groups).forEach(groupIndex -> {
            int startIndex = groupIndex * gap;
            int endIndex = Math.min((groupIndex + 1) * gap, arr.length);
            if (startIndex >= arr.length)
                return;
            if (groupIndex == groups - 1) {
                for (int i = startIndex; i < arr.length; i++) {
                    multimap.put(groupIndex, arr[i]);
                }
            } else {
                for (int i = startIndex; i < endIndex; i++) {
                    multimap.put(groupIndex, arr[i]);
                }
            }
        });
        return multimap;
    }


}
