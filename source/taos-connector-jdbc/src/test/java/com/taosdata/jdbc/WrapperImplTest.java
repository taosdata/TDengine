package com.taosdata.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class WrapperImplTest {

    static class TestWrapperImpl extends WrapperImpl {
        // Test subclass
    }

    @Test
    public void testUnwrapSuccess() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        TestWrapperImpl unwrapped = wrapper.unwrap(TestWrapperImpl.class);
        Assert.assertNotNull(unwrapped);
        Assert.assertSame(wrapper, unwrapped);
    }

    @Test
    public void testUnwrapToSameClass() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        WrapperImpl unwrapped = wrapper.unwrap(WrapperImpl.class);
        Assert.assertNotNull(unwrapped);
        Assert.assertSame(wrapper, unwrapped);
    }

    @Test(expected = SQLException.class)
    public void testUnwrapFailure() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        wrapper.unwrap(String.class);
    }

    @Test
    public void testUnwrapToObject() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Object unwrapped = wrapper.unwrap(Object.class);
        Assert.assertNotNull(unwrapped);
        Assert.assertSame(wrapper, unwrapped);
    }

    @Test
    public void testIsWrapperForTrue() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertTrue(wrapper.isWrapperFor(TestWrapperImpl.class));
    }

    @Test
    public void testIsWrapperForParentClass() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertTrue(wrapper.isWrapperFor(WrapperImpl.class));
    }

    @Test
    public void testIsWrapperForObject() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertTrue(wrapper.isWrapperFor(Object.class));
    }

    @Test
    public void testIsWrapperForFalse() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertFalse(wrapper.isWrapperFor(String.class));
    }

    @Test
    public void testIsWrapperForDifferentInterface() throws SQLException {
        TestWrapperImpl wrapper = new TestWrapperImpl();
        Assert.assertFalse(wrapper.isWrapperFor(Runnable.class));
    }
}
