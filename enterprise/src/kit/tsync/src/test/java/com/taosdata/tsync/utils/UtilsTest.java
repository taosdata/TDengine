package com.taosdata.tsync.utils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

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
        for (int i = 1; i <= 100; i++) {
            Map<Long, Long> map = Utils.divideIntoGroups(i, Range.closedOpen(3L, 13L));
            System.out.println(map);
        }
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
//        for (int i = 1; i <= 101; i++) {
//            int[] partitions = IntStream.range(1, i).toArray();
//            Multimap<Integer, Integer> multimap = Utils.divideArrayIntoGroups(partitions, 10);
//            System.out.println(multimap);
//        }

        // when
        Multimap<Integer, Integer> map = Utils.divideArrayIntoGroups(new int[]{1, 2, 3, 4}, 5);

        // then
        assertTrue(map.containsKey(0));
        assertEquals(1, map.get(0).size());
        assertTrue(map.get(0).contains(1));

        assertTrue(map.containsKey(1));
        assertEquals(1, map.get(1).size());
        assertTrue(map.get(1).contains(2));

        assertTrue(map.containsKey(2));
        assertEquals(1, map.get(2).size());
        assertTrue(map.get(2).contains(3));

        assertTrue(map.containsKey(3));
        assertEquals(1, map.get(3).size());
        assertTrue(map.get(3).contains(4));

        assertFalse(map.containsKey(4));
    }

    @Test
    public void divide8NumberInto5Groups() {
        // when
        Multimap<Integer, Integer> map = Utils.divideArrayIntoGroups(new int[]{1, 2, 3, 4, 5, 6, 7, 8}, 5);

        // then
        assertTrue(map.containsKey(0));
        assertEquals(2, map.get(0).size());
        assertTrue(map.get(0).contains(1));
        assertTrue(map.get(0).contains(2));

        assertTrue(map.containsKey(1));
        assertEquals(2, map.get(1).size());
        assertTrue(map.get(1).contains(3));
        assertTrue(map.get(1).contains(4));

        assertTrue(map.containsKey(2));
        assertEquals(2, map.get(2).size());
        assertTrue(map.get(2).contains(5));
        assertTrue(map.get(2).contains(6));

        assertTrue(map.containsKey(3));
        assertEquals(2, map.get(3).size());
        assertTrue(map.get(3).contains(7));
        assertTrue(map.get(3).contains(8));

        assertFalse(map.containsKey(4));
    }

    @Test
    public void divide9NumberArrInto4Groups() {
        // when
        Multimap<Integer, Integer> map = Utils.divideArrayIntoGroups(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, 4);

        // then
        assertTrue(map.containsKey(0));
        assertEquals(2, map.get(0).size());
        assertTrue(map.get(0).contains(1));
        assertTrue(map.get(0).contains(2));

        assertTrue(map.containsKey(1));
        assertEquals(2, map.get(1).size());
        assertTrue(map.get(1).contains(3));
        assertTrue(map.get(1).contains(4));

        assertTrue(map.containsKey(2));
        assertEquals(2, map.get(2).size());
        assertTrue(map.get(2).contains(5));
        assertTrue(map.get(2).contains(6));

        assertTrue(map.containsKey(3));
        assertEquals(3, map.get(3).size());
        assertTrue(map.get(3).contains(7));
        assertTrue(map.get(3).contains(8));
        assertTrue(map.get(3).contains(9));
    }

}