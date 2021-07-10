package com.taosdata.tsync.utils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.sun.media.jfxmediaimpl.HostUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void divideRangeIntoArrayGroups() {
        // when
        Map<Integer, Range<Long>> rangeMap = Utils.divideRangeIntoArrayGroups(Range.closedOpen(0L, 100L), new int[]{10, 11, 12, 13, 14, 15, 16, 17, 18, 19});

        // then
        Assert.assertEquals(0, rangeMap.get(10).lowerEndpoint().longValue());
        Assert.assertEquals(10, rangeMap.get(10).upperEndpoint().longValue());
        Assert.assertEquals(60, rangeMap.get(16).lowerEndpoint().longValue());
        Assert.assertEquals(70, rangeMap.get(16).upperEndpoint().longValue());
        Assert.assertEquals(90, rangeMap.get(19).lowerEndpoint().longValue());
        Assert.assertEquals(100, rangeMap.get(19).upperEndpoint().longValue());
    }

    @Test
    public void divideRangeIntoGroups() {
        // when
        List<Range<Long>> ranges = Utils.divideRangeIntoGroups(Range.closedOpen(0L, 100L), 10);

        // then
        Assert.assertEquals(0, ranges.get(0).lowerEndpoint().longValue());
        Assert.assertEquals(10, ranges.get(0).upperEndpoint().longValue());
        Assert.assertEquals(50, ranges.get(5).lowerEndpoint().longValue());
        Assert.assertEquals(60, ranges.get(5).upperEndpoint().longValue());
        Assert.assertEquals(90, ranges.get(9).lowerEndpoint().longValue());
        Assert.assertEquals(100, ranges.get(9).upperEndpoint().longValue());
    }

    @Test
    public void divideIntoRangeGroups() {
        // when
        Map<Long, Long> map = Utils.divideIntoGroups(100, Range.closedOpen(10L, 20L));

        // then
        Assert.assertEquals(10, map.get(10L).longValue());
        Assert.assertEquals(10, map.get(11L).longValue());
        Assert.assertEquals(10, map.get(12L).longValue());
        Assert.assertEquals(10, map.get(13L).longValue());
        Assert.assertEquals(10, map.get(14L).longValue());
        Assert.assertEquals(10, map.get(15L).longValue());
        Assert.assertEquals(10, map.get(16L).longValue());
        Assert.assertEquals(10, map.get(17L).longValue());
        Assert.assertEquals(10, map.get(18L).longValue());
        Assert.assertEquals(10, map.get(19L).longValue());

    }

    @Test
    public void divideIntoRangeList() {
        // when
        List<Range<Long>> ranges = Utils.divideIntoRangeList(1, 10);
        // then
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(0L, ranges.get(0).lowerEndpoint().longValue());
        Assert.assertEquals(1L, ranges.get(0).upperEndpoint().longValue());

        // when
        ranges = Utils.divideIntoRangeList(2, 10);
        // then
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(0L, ranges.get(0).lowerEndpoint().longValue());
        Assert.assertEquals(1L, ranges.get(0).upperEndpoint().longValue());
        Assert.assertEquals(1L, ranges.get(1).lowerEndpoint().longValue());
        Assert.assertEquals(2L, ranges.get(1).upperEndpoint().longValue());

        // when
        ranges = Utils.divideIntoRangeList(10, 2);
        // then
        Assert.assertEquals(0L, ranges.get(0).lowerEndpoint().longValue());
        Assert.assertEquals(5L, ranges.get(0).upperEndpoint().longValue());
        Assert.assertEquals(5L, ranges.get(1).lowerEndpoint().longValue());
        Assert.assertEquals(10L, ranges.get(1).upperEndpoint().longValue());

        // when
        ranges = Utils.divideIntoRangeList(1000_0000L, 10);
        // then
        Assert.assertEquals(0L, ranges.get(0).lowerEndpoint().longValue());
        Assert.assertEquals(100_0000L, ranges.get(0).upperEndpoint().longValue());
        Assert.assertEquals(100_0000L, ranges.get(1).lowerEndpoint().longValue());
        Assert.assertEquals(200_0000L, ranges.get(1).upperEndpoint().longValue());
        Assert.assertEquals(200_0000L, ranges.get(2).lowerEndpoint().longValue());
        Assert.assertEquals(300_0000L, ranges.get(2).upperEndpoint().longValue());
        Assert.assertEquals(300_0000L, ranges.get(3).lowerEndpoint().longValue());
        Assert.assertEquals(400_0000L, ranges.get(3).upperEndpoint().longValue());
        Assert.assertEquals(400_0000L, ranges.get(4).lowerEndpoint().longValue());
        Assert.assertEquals(500_0000L, ranges.get(4).upperEndpoint().longValue());
        Assert.assertEquals(500_0000L, ranges.get(5).lowerEndpoint().longValue());
        Assert.assertEquals(600_0000L, ranges.get(5).upperEndpoint().longValue());
        Assert.assertEquals(600_0000L, ranges.get(6).lowerEndpoint().longValue());
        Assert.assertEquals(700_0000L, ranges.get(6).upperEndpoint().longValue());
        Assert.assertEquals(700_0000L, ranges.get(7).lowerEndpoint().longValue());
        Assert.assertEquals(800_0000L, ranges.get(7).upperEndpoint().longValue());
        Assert.assertEquals(800_0000L, ranges.get(8).lowerEndpoint().longValue());
        Assert.assertEquals(900_0000L, ranges.get(8).upperEndpoint().longValue());
        Assert.assertEquals(900_0000L, ranges.get(9).lowerEndpoint().longValue());
        Assert.assertEquals(1000_0000L, ranges.get(9).upperEndpoint().longValue());

    }

    @Test
    public void divide10NumberIntoGroupsOf3() {
        //when
        Map<Long, Range<Long>> map = Utils.divideIntoGroupsOfN(0L, 10L, 3);

        // then
        assertEquals(4, map.size());

        Assert.assertTrue(map.containsKey(0L));
        assertEquals(new Long(0), map.get(0L).lowerEndpoint());
        assertEquals(new Long(3), map.get(0L).upperEndpoint());

        assertTrue(map.containsKey(1L));
        assertEquals(new Long(3), map.get(1L).lowerEndpoint());
        assertEquals(new Long(6), map.get(1L).upperEndpoint());

        assertTrue(map.containsKey(2L));
        assertEquals(new Long(6), map.get(2L).lowerEndpoint());
        assertEquals(new Long(9), map.get(2L).upperEndpoint());

        assertTrue(map.containsKey(3L));
        assertEquals(new Long(9), map.get(3L).lowerEndpoint());
        assertEquals(new Long(10), map.get(3L).upperEndpoint());
    }

    @Test
    public void divide3NumberInto5Groups() {
//        for (int i = 1; i <= 120; i++) {
//            List<Long> groups = Utils.divideIntoGroups(i, 10);
//            System.out.println("divide " + i + " into 10 groups： " + groups);
//        }

        // when
        List<Long> groups = Utils.divideIntoGroups(3L, 5L);

        // then
        Assert.assertEquals(3, groups.size());
        Assert.assertEquals(1, groups.get(0).longValue());
        Assert.assertEquals(1, groups.get(1).longValue());
        Assert.assertEquals(1, groups.get(2).longValue());
        // 9 / 4

        // 10 / 3

        // 10 / 4

        // 11 / 4
    }

    @Test
    public void divideArrayIntoGroups() {
//        for (int i = 1; i <= 100; i++) {
//            int[] arr = IntStream.range(1, i).toArray();
//            List<Integer[]> arrayGroups = Utils.divideArrayIntoGroups(arr, 10);
//            arrayGroups.forEach(g -> System.out.print(Arrays.toString(g) + " "));
//            System.out.println();
//        }

        // when
        int[] arr = IntStream.range(1, 101).toArray();
        List<Integer[]> arrayGroups = Utils.divideArrayIntoGroups(arr, 10);

        // then
        Assert.assertEquals(1, arrayGroups.get(0)[0].intValue());
        Assert.assertEquals(10, arrayGroups.get(0)[9].intValue());

        Assert.assertEquals(51, arrayGroups.get(5)[0].intValue());
        Assert.assertEquals(60, arrayGroups.get(5)[9].intValue());

        Assert.assertEquals(91, arrayGroups.get(9)[0].intValue());
        Assert.assertEquals(100, arrayGroups.get(9)[9].intValue());
    }

}