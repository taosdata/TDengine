package com.taosdata.tsync.utils;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class UtilsTest {

    @Test
    public void divide10IntoGroupsOf3() {
        //when
        Map<Long, Range<Long>> map = Utils.divideIntoGroupsOfN(0l, 10l, 3);

        // then
        Assert.assertEquals(4, map.size());

        Assert.assertEquals(true, map.containsKey(0l));
        Assert.assertEquals(new Long(0), map.get(0l).lowerEndpoint());
        Assert.assertEquals(new Long(3), map.get(0l).upperEndpoint());

        Assert.assertEquals(true, map.containsKey(1l));
        Assert.assertEquals(new Long(3), map.get(1l).lowerEndpoint());
        Assert.assertEquals(new Long(6), map.get(1l).upperEndpoint());

        Assert.assertEquals(true, map.containsKey(2l));
        Assert.assertEquals(new Long(6), map.get(2l).lowerEndpoint());
        Assert.assertEquals(new Long(9), map.get(2l).upperEndpoint());

        Assert.assertEquals(true, map.containsKey(3l));
        Assert.assertEquals(new Long(9), map.get(3l).lowerEndpoint());
        Assert.assertEquals(new Long(10), map.get(3l).upperEndpoint());
    }

    @Test
    public void divide3NumberInto5Groups() {
        // when
        Map<Long, Range<Long>> map = Utils.divideIntoGroups(3l, 5l);

        // then
        Assert.assertEquals(false, map.get(0l).isEmpty());
        Assert.assertEquals(new Long(0), map.get(0l).lowerEndpoint());
        Assert.assertEquals(new Long(1), map.get(0l).upperEndpoint());

        Assert.assertEquals(false, map.get(1l).isEmpty());
        Assert.assertEquals(new Long(1), map.get(1l).lowerEndpoint());
        Assert.assertEquals(new Long(2), map.get(1l).upperEndpoint());

        Assert.assertEquals(false, map.get(2l).isEmpty());
        Assert.assertEquals(new Long(2), map.get(2l).lowerEndpoint());
        Assert.assertEquals(new Long(3), map.get(2l).upperEndpoint());

        Assert.assertEquals(true, map.get(3l).isEmpty());
        Assert.assertEquals(new Long(3), map.get(3l).lowerEndpoint());
        Assert.assertEquals(new Long(3), map.get(3l).upperEndpoint());

        Assert.assertEquals(true, map.get(4l).isEmpty());
        Assert.assertEquals(new Long(3), map.get(4l).lowerEndpoint());
        Assert.assertEquals(new Long(3), map.get(4l).upperEndpoint());
    }

    @Test
    public void divide10NumberIntoSuccessive4Groups() {
        // when
        Map<Long, Range<Long>> map = Utils.divideIntoGroups(10l, 4l);

        // then
        Assert.assertEquals(false, map.get(0l).isEmpty());
        Assert.assertEquals(new Long(0), map.get(0l).lowerEndpoint());
        Assert.assertEquals(new Long(3), map.get(0l).upperEndpoint());

        Assert.assertEquals(false, map.get(1l).isEmpty());
        Assert.assertEquals(new Long(3), map.get(1l).lowerEndpoint());
        Assert.assertEquals(new Long(6), map.get(1l).upperEndpoint());

        Assert.assertEquals(false, map.get(2l).isEmpty());
        Assert.assertEquals(new Long(6), map.get(2l).lowerEndpoint());
        Assert.assertEquals(new Long(9), map.get(2l).upperEndpoint());

        Assert.assertEquals(false, map.get(3l).isEmpty());
        Assert.assertEquals(new Long(9), map.get(3l).lowerEndpoint());
        Assert.assertEquals(new Long(10), map.get(3l).upperEndpoint());
    }

    @Test
    public void divide11NumberIntoSuccessive4Groups() {
        // when
        Map<Integer, Range<Long>> map = Utils.divideIntoGroups(11, 4);

        // then
        Assert.assertEquals(false, map.get(0).isEmpty());
        Assert.assertEquals(new Long(0), map.get(0).lowerEndpoint());
        Assert.assertEquals(new Long(3), map.get(0).upperEndpoint());

        Assert.assertEquals(false, map.get(1).isEmpty());
        Assert.assertEquals(new Long(3), map.get(1).lowerEndpoint());
        Assert.assertEquals(new Long(6), map.get(1).upperEndpoint());

        Assert.assertEquals(false, map.get(2).isEmpty());
        Assert.assertEquals(new Long(6), map.get(2).lowerEndpoint());
        Assert.assertEquals(new Long(9), map.get(2).upperEndpoint());

        Assert.assertEquals(false, map.get(3).isEmpty());
        Assert.assertEquals(new Long(9), map.get(3).lowerEndpoint());
        Assert.assertEquals(new Long(11), map.get(3).upperEndpoint());
    }

    @Test
    public void divide9NumberIntoSuccessive4Groups() {
        // when
        Map<Integer, Range<Long>> map = Utils.divideIntoGroups(9, 4);

        // then
        Assert.assertEquals(false, map.get(0).isEmpty());
        Assert.assertEquals(new Long(0), map.get(0).lowerEndpoint());
        Assert.assertEquals(new Long(2), map.get(0).upperEndpoint());

        Assert.assertEquals(false, map.get(1).isEmpty());
        Assert.assertEquals(new Long(2), map.get(1).lowerEndpoint());
        Assert.assertEquals(new Long(4), map.get(1).upperEndpoint());

        Assert.assertEquals(false, map.get(2).isEmpty());
        Assert.assertEquals(new Long(4), map.get(2).lowerEndpoint());
        Assert.assertEquals(new Long(6), map.get(2).upperEndpoint());

        Assert.assertEquals(false, map.get(3).isEmpty());
        Assert.assertEquals(new Long(6), map.get(3).lowerEndpoint());
        Assert.assertEquals(new Long(9), map.get(3).upperEndpoint());
    }

    @Test
    public void divide10NumberIntoSuccessive3Groups() {
        // when
        Map<Integer, Range<Long>> map = Utils.divideIntoGroups(10, 3);

        // then
        Assert.assertEquals(false, map.get(0).isEmpty());
        Assert.assertEquals(new Long(0), map.get(0).lowerEndpoint());
        Assert.assertEquals(new Long(3), map.get(0).upperEndpoint());

        Assert.assertEquals(false, map.get(1).isEmpty());
        Assert.assertEquals(new Long(3), map.get(1).lowerEndpoint());
        Assert.assertEquals(new Long(6), map.get(1).upperEndpoint());

        Assert.assertEquals(false, map.get(2).isEmpty());
        Assert.assertEquals(new Long(6), map.get(2).lowerEndpoint());
        Assert.assertEquals(new Long(10), map.get(2).upperEndpoint());
    }

    @Test
    public void divide4NumberInto5Groups() {
        // when
        Multimap<Integer, Integer> map = Utils.divideArrIntoGroups(new int[]{1, 2, 3, 4}, 5);

        // then
        Assert.assertEquals(true, map.containsKey(0));
        Assert.assertEquals(1, map.get(0).size());
        Assert.assertEquals(true, map.get(0).contains(1));

        Assert.assertEquals(true, map.containsKey(1));
        Assert.assertEquals(1, map.get(1).size());
        Assert.assertEquals(true, map.get(1).contains(2));

        Assert.assertEquals(true, map.containsKey(2));
        Assert.assertEquals(1, map.get(2).size());
        Assert.assertEquals(true, map.get(2).contains(3));

        Assert.assertEquals(true, map.containsKey(3));
        Assert.assertEquals(1, map.get(3).size());
        Assert.assertEquals(true, map.get(3).contains(4));

        Assert.assertEquals(false, map.containsKey(4));
    }

    @Test
    public void divide8NumberInto5Groups() {
        // when
        Multimap<Integer, Integer> map = Utils.divideArrIntoGroups(new int[]{1, 2, 3, 4, 5, 6, 7, 8}, 5);

        // then
        Assert.assertEquals(true, map.containsKey(0));
        Assert.assertEquals(2, map.get(0).size());
        Assert.assertEquals(true, map.get(0).contains(1));
        Assert.assertEquals(true, map.get(0).contains(2));

        Assert.assertEquals(true, map.containsKey(1));
        Assert.assertEquals(2, map.get(1).size());
        Assert.assertEquals(true, map.get(1).contains(3));
        Assert.assertEquals(true, map.get(1).contains(4));

        Assert.assertEquals(true, map.containsKey(2));
        Assert.assertEquals(2, map.get(2).size());
        Assert.assertEquals(true, map.get(2).contains(5));
        Assert.assertEquals(true, map.get(2).contains(6));

        Assert.assertEquals(true, map.containsKey(3));
        Assert.assertEquals(2, map.get(3).size());
        Assert.assertEquals(true, map.get(3).contains(7));
        Assert.assertEquals(true, map.get(3).contains(8));

        Assert.assertEquals(false, map.containsKey(4));

//        Assert.assertEquals(true, map.get(4).contains(5));
    }

    @Test
    public void divide9NumberInto4Groups() {
        // when
        Multimap<Integer, Integer> map = Utils.divideArrIntoGroups(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, 4);

        // then
        Assert.assertEquals(true, map.containsKey(0));
        Assert.assertEquals(2, map.get(0).size());
        Assert.assertEquals(true, map.get(0).contains(1));
        Assert.assertEquals(true, map.get(0).contains(2));

        Assert.assertEquals(true, map.containsKey(1));
        Assert.assertEquals(2, map.get(1).size());
        Assert.assertEquals(true, map.get(1).contains(3));
        Assert.assertEquals(true, map.get(1).contains(4));

        Assert.assertEquals(true, map.containsKey(2));
        Assert.assertEquals(2, map.get(2).size());
        Assert.assertEquals(true, map.get(2).contains(5));
        Assert.assertEquals(true, map.get(2).contains(6));

        Assert.assertEquals(true, map.containsKey(3));
        Assert.assertEquals(3, map.get(3).size());
        Assert.assertEquals(true, map.get(3).contains(7));
        Assert.assertEquals(true, map.get(3).contains(8));
        Assert.assertEquals(true, map.get(3).contains(9));

    }
}