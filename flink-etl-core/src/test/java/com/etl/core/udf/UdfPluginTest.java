package com.etl.core.udf;

import com.etl.core.udf.scalar.HashUdf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UdfPluginTest {

    @Test
    void testHashUdfIdentifier() {
        HashUdf hashUdf = new HashUdf();
        assertEquals("hash_code", hashUdf.identifier());
    }

    @Test
    void testHashUdfCreateFunction() {
        HashUdf hashUdf = new HashUdf();
        assertNotNull(hashUdf.createFunction());
    }

    @Test
    void testHashFunctionEval() {
        HashUdf.HashFunction function = new HashUdf.HashFunction();

        // 测试 null 输入
        assertEquals(0, function.eval(null));

        // 测试正常输入
        assertEquals("hello".hashCode(), function.eval("hello"));
        assertEquals(Integer.valueOf(123).hashCode(), function.eval(123));
    }
}