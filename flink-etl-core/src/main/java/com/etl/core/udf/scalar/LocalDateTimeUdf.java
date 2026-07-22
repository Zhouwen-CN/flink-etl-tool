package com.etl.core.udf.scalar;

import com.etl.core.spi.UdfPlugin;
import com.google.auto.service.AutoService;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.time.LocalDateTime;

/**
 * 返回 LocalDateTime.now() flink
 * 剞劂 now() 函数返回的是 Instant，插入 oracle date 类型报错问题
 */
@AutoService(UdfPlugin.class)
public class LocalDateTimeUdf implements UdfPlugin {

    @Override
    public String identifier() {
        return "current_datetime";
    }

    @Override
    public UserDefinedFunction createFunction() {
        return new LocalDateTimeFunction();
    }

    /**
     * 实际的 Flink ScalarFunction 实现
     */
    public static class LocalDateTimeFunction extends ScalarFunction {

        @DataTypeHint("TIMESTAMP(3)")
        public Object eval() {
            return LocalDateTime.now();
        }

        @Override
        public String toString() {
            return "current_datetime()";
        }
    }
}