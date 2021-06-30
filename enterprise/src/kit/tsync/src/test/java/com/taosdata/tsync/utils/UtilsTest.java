package com.taosdata.tsync.utils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class UtilsTest {

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
        // when
        Map<Long, Range<Long>> map = Utils.divideIntoGroups(3L, 5L);

        // then
        assertFalse(map.get(0L).isEmpty());
        assertEquals(new Long(0), map.get(0L).lowerEndpoint());
        assertEquals(new Long(1), map.get(0L).upperEndpoint());

        assertFalse(map.get(1L).isEmpty());
        assertEquals(new Long(1), map.get(1L).lowerEndpoint());
        assertEquals(new Long(2), map.get(1L).upperEndpoint());

        assertFalse(map.get(2L).isEmpty());
        assertEquals(new Long(2), map.get(2L).lowerEndpoint());
        assertEquals(new Long(3), map.get(2L).upperEndpoint());

        assertTrue(map.get(3L).isEmpty());
        assertEquals(new Long(3), map.get(3L).lowerEndpoint());
        assertEquals(new Long(3), map.get(3L).upperEndpoint());

        assertTrue(map.get(4L).isEmpty());
        assertEquals(new Long(3), map.get(4L).lowerEndpoint());
        assertEquals(new Long(3), map.get(4L).upperEndpoint());
    }

    @Test
    public void divide10NumberInto4Groups() {
        // when
        Map<Long, Range<Long>> map = Utils.divideIntoGroups(10L, 4L);

        // then
        assertFalse(map.get(0L).isEmpty());
        assertEquals(new Long(0), map.get(0L).lowerEndpoint());
        assertEquals(new Long(3), map.get(0L).upperEndpoint());

        assertFalse(map.get(1L).isEmpty());
        assertEquals(new Long(3), map.get(1L).lowerEndpoint());
        assertEquals(new Long(6), map.get(1L).upperEndpoint());

        assertFalse(map.get(2L).isEmpty());
        assertEquals(new Long(6), map.get(2L).lowerEndpoint());
        assertEquals(new Long(9), map.get(2L).upperEndpoint());

        assertFalse(map.get(3L).isEmpty());
        assertEquals(new Long(9), map.get(3L).lowerEndpoint());
        assertEquals(new Long(10), map.get(3L).upperEndpoint());
    }

    @Test
    public void divide11NumberInto4Groups() {
        // when
        Map<Integer, Range<Long>> map = Utils.divideIntoGroups(11, 4);

        // then
        assertFalse(map.get(0).isEmpty());
        assertEquals(new Long(0), map.get(0).lowerEndpoint());
        assertEquals(new Long(3), map.get(0).upperEndpoint());

        assertFalse(map.get(1).isEmpty());
        assertEquals(new Long(3), map.get(1).lowerEndpoint());
        assertEquals(new Long(6), map.get(1).upperEndpoint());

        assertFalse(map.get(2).isEmpty());
        assertEquals(new Long(6), map.get(2).lowerEndpoint());
        assertEquals(new Long(9), map.get(2).upperEndpoint());

        assertFalse(map.get(3).isEmpty());
        assertEquals(new Long(9), map.get(3).lowerEndpoint());
        assertEquals(new Long(11), map.get(3).upperEndpoint());
    }

    @Test
    public void divide9NumberInto4Groups() {
        // when
        Map<Integer, Range<Long>> map = Utils.divideIntoGroups(9, 4);

        // then
        assertFalse(map.get(0).isEmpty());
        assertEquals(new Long(0), map.get(0).lowerEndpoint());
        assertEquals(new Long(2), map.get(0).upperEndpoint());

        assertFalse(map.get(1).isEmpty());
        assertEquals(new Long(2), map.get(1).lowerEndpoint());
        assertEquals(new Long(4), map.get(1).upperEndpoint());

        assertFalse(map.get(2).isEmpty());
        assertEquals(new Long(4), map.get(2).lowerEndpoint());
        assertEquals(new Long(6), map.get(2).upperEndpoint());

        assertFalse(map.get(3).isEmpty());
        assertEquals(new Long(6), map.get(3).lowerEndpoint());
        assertEquals(new Long(9), map.get(3).upperEndpoint());
    }

    @Test
    public void divide10NumberInto3Groups() {
        // when
        Map<Integer, Range<Long>> map = Utils.divideIntoGroups(10, 3);

        // then
        assertFalse(map.get(0).isEmpty());
        assertEquals(new Long(0), map.get(0).lowerEndpoint());
        assertEquals(new Long(3), map.get(0).upperEndpoint());

        assertFalse(map.get(1).isEmpty());
        assertEquals(new Long(3), map.get(1).lowerEndpoint());
        assertEquals(new Long(6), map.get(1).upperEndpoint());

        assertFalse(map.get(2).isEmpty());
        assertEquals(new Long(6), map.get(2).lowerEndpoint());
        assertEquals(new Long(10), map.get(2).upperEndpoint());
    }

    @Test
    public void divide4NumberInto5Groups() {
        // when
        Multimap<Integer, Integer> map = Utils.divideArrIntoGroups(new int[]{1, 2, 3, 4}, 5);

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
        Multimap<Integer, Integer> map = Utils.divideArrIntoGroups(new int[]{1, 2, 3, 4, 5, 6, 7, 8}, 5);

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
        Multimap<Integer, Integer> map = Utils.divideArrIntoGroups(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, 4);

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

    @Test
    public void divide100SizeRangeInto4Groups() {
        //given
        long startInclude = 100L;
        long endExclude = 200L;
        int[] arr = new int[]{3, 4, 5, 6};

        // when
        Map<Integer, Range<Long>> arrIndex2Range = Utils.divideIntoArrGroups(startInclude, endExclude, arr);

        // then
        assertEquals(4, arrIndex2Range.size());
        assertTrue(arrIndex2Range.containsKey(3));
        assertEquals(new Long(100), arrIndex2Range.get(3).lowerEndpoint());
        assertEquals(new Long(125), arrIndex2Range.get(3).upperEndpoint());
        assertTrue(arrIndex2Range.containsKey(4));
        assertEquals(new Long(125), arrIndex2Range.get(4).lowerEndpoint());
        assertEquals(new Long(150), arrIndex2Range.get(4).upperEndpoint());
        assertTrue(arrIndex2Range.containsKey(5));
        assertEquals(new Long(150), arrIndex2Range.get(5).lowerEndpoint());
        assertEquals(new Long(175), arrIndex2Range.get(5).upperEndpoint());
        assertTrue(arrIndex2Range.containsKey(6));
        assertEquals(new Long(175), arrIndex2Range.get(6).lowerEndpoint());
        assertEquals(new Long(200), arrIndex2Range.get(6).upperEndpoint());
    }

    @Test
    public void divide1BillionInto5Groups() {
        // given
        long many = 100_0000_0000L;
        int few = 5;
        // when
        Map<Integer, Range<Long>> map = Utils.divideIntoGroups(many, few);
        // then
        assertTrue(map.containsKey(0));
        assertEquals(new Long(0L), map.get(0).lowerEndpoint());
        assertEquals(new Long(20_0000_0000L), map.get(0).upperEndpoint());

        assertTrue(map.containsKey(1));
        assertEquals(new Long(20_0000_0000L), map.get(1).lowerEndpoint());
        assertEquals(new Long(40_0000_0000L), map.get(1).upperEndpoint());

        assertTrue(map.containsKey(2));
        assertEquals(new Long(40_0000_0000L), map.get(2).lowerEndpoint());
        assertEquals(new Long(60_0000_0000L), map.get(2).upperEndpoint());

        assertTrue(map.containsKey(3));
        assertEquals(new Long(60_0000_0000L), map.get(3).lowerEndpoint());
        assertEquals(new Long(80_0000_0000L), map.get(3).upperEndpoint());

        assertTrue(map.containsKey(4));
        assertEquals(new Long(80_0000_0000L), map.get(4).lowerEndpoint());
        assertEquals(new Long(100_0000_0000L), map.get(4).upperEndpoint());
    }

}