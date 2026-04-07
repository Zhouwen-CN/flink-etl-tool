package com.etl.core.udf.scalar;

import com.etl.core.spi.UdfPlugin;
import com.google.auto.service.AutoService;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * Hash 函数示例
 * 返回输入值的哈希码
 */
@AutoService(UdfPlugin.class)
public class HashUdf implements UdfPlugin {

    @Override
    public String identifier() {
        return "hash_code";
    }

    @Override
    public UserDefinedFunction createFunction() {
        return new HashFunction();
    }

    /**
     * 实际的 Flink ScalarFunction 实现
     */
    public static class HashFunction extends ScalarFunction {

        /**
         * 计算输入对象的哈希码
         *
         * @param input 输入对象，可以为 null
         * @return 哈希码，null 输入返回 0
         */
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object input) {
            if (input == null) {
                return 0;
            }
            return input.hashCode();
        }

        @Override
        public String toString() {
            return "hash_code()";
        }
    }
}