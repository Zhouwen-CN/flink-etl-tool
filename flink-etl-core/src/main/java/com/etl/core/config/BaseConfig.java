package com.etl.core.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置基类
 *
 * <p>按配置值的结构分成三类读取方法：
 * <ul>
 *   <li>{@link #get(String, Class)}：单个基本类型值</li>
 *   <li>{@link #getList(String, Class)}：列表，逐个转换元素类型</li>
 *   <li>{@link #getMap(String, Class)}：映射，逐个转换 value 类型</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class BaseConfig implements Serializable {

    private Map<String, Object> config;

    /**
     * 类型转换
     *
     * @param key   配置键，用于报错信息
     * @param value 原始值
     * @param clazz 期望类型
     * @param <T>   期望类型参数
     * @return 转换后的值
     */
    @SuppressWarnings("unchecked")
    private static <T> T convert(String key, Object value, Class<T> clazz) {
        if (clazz == Object.class) {
            return (T) value;
        }
        if (clazz == String.class) {
            return (T) String.valueOf(value);
        }
        if (clazz == Integer.class) {
            return (T) Integer.valueOf(toInt(key, value));
        }
        if (clazz == Long.class) {
            return (T) Long.valueOf(toLong(key, value));
        }
        if (clazz == Double.class) {
            return (T) Double.valueOf(toDouble(key, value));
        }
        if (clazz == Boolean.class) {
            return (T) Boolean.valueOf(Boolean.parseBoolean(String.valueOf(value)));
        }
        throw new IllegalArgumentException("不支持的类型: " + clazz.getName());
    }

    /**
     * 转换为 int：支持 Integer/Long 等数值型（含整数值的浮点型）与数字字符串
     */
    private static int toInt(String key, Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        try {
            if (value instanceof Number) {
                // 数值型：必须是整数且不溢出 int 范围
                return new BigDecimal(value.toString()).intValueExact();
            }
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "配置项 '" + key + "' 的值 '" + value + "' 无法转换为整数", e);
        }
    }

    /**
     * 转换为 long：支持数值型（含整数值的浮点型）与数字字符串
     */
    private static long toLong(String key, Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        try {
            if (value instanceof Number) {
                // 数值型：必须是整数且不溢出 long 范围
                return new BigDecimal(value.toString()).longValueExact();
            }
            return Long.parseLong(String.valueOf(value).trim());
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    "配置项 '" + key + "' 的值 '" + value + "' 无法转换为长整数", e);
        }
    }

    /**
     * 转换为 double：支持数值型与数字字符串
     */
    private static double toDouble(String key, Object value) {
        if (value instanceof Double) {
            return (Double) value;
        }
        try {
            if (value instanceof Number) {
                return new BigDecimal(value.toString()).doubleValue();
            }
            return Double.parseDouble(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "配置项 '" + key + "' 的值 '" + value + "' 无法转换为小数", e);
        }
    }

    /**
     * 校验类型是否受支持
     */
    private static void checkTypeSupported(Class<?> clazz) {
        if (clazz != Object.class && clazz != String.class && clazz != Integer.class
                && clazz != Long.class && clazz != Double.class && clazz != Boolean.class) {
            throw new IllegalArgumentException("不支持的类型: " + clazz.getName());
        }
    }

    /**
     * 类型的可读名称，用于报错信息
     */
    private static String typeName(Class<?> clazz) {
        if (clazz == String.class) {
            return "字符串";
        }
        if (clazz == Integer.class) {
            return "整数";
        }
        if (clazz == Long.class) {
            return "长整数";
        }
        if (clazz == Double.class) {
            return "小数";
        }
        if (clazz == Boolean.class) {
            return "布尔值";
        }
        return "原始类型";
    }

    /**
     * 根据期望类型获取单个配置值
     *
     * <p>支持的类型：String、Integer、Long、Double、Boolean、Object。
     * 不支持的类型（包括 List、Map）抛 IllegalArgumentException。
     * 当 config 为 null、key 不存在或值为 null 时返回 null。
     *
     * <p>需要读取列表或映射时，请使用 {@link #getList(String, Class)}
     * 或 {@link #getMap(String, Class)}，它们会把元素/value 转换成指定的类型。
     *
     * @param key   配置键
     * @param clazz 期望的返回类型
     * @param <T>   返回类型参数
     * @return 转换后的配置值；不存在时返回 null
     */
    public <T> T get(String key, Class<T> clazz) {
        Object value = this.get(key);
        if (value == null) {
            return null;
        }
        return convert(key, value, clazz);
    }

    /**
     * 根据期望类型获取单个配置值，未设置时返回默认值
     *
     * <p>类型转换异常仍然抛出，不会被默认值掩盖。
     */
    public <T> T get(String key, Class<T> clazz, T defaultValue) {
        T value = get(key, clazz);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取列表类型的配置值，并把每个元素转换为指定类型
     *
     * <p>元素转换规则与 {@link #get(String, Class)} 完全一致：
     * <ul>
     *   <li>{@code String}：统一转成字符串</li>
     *   <li>{@code Integer}/{@code Long}：支持数值型（含整数值的浮点型）与数字字符串</li>
     *   <li>{@code Double}：支持数值型与数字字符串</li>
     *   <li>{@code Boolean}：支持布尔值与布尔字符串</li>
     *   <li>{@code Object}：保持原始值不变</li>
     * </ul>
     * 元素为 null 或元素无法转换时抛 IllegalArgumentException。
     *
     * @param key      配置键
     * @param itemType 元素类型
     * @param <T>      元素类型参数
     * @return 转换后的列表；key 不存在或值为 null 时返回 null
     */
    public <T> List<T> getList(String key, Class<T> itemType) {
        Object value = this.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?>)) {
            throw new IllegalArgumentException("配置项 '" + key + "' 不是列表类型");
        }
        checkTypeSupported(itemType);

        List<?> rawList = (List<?>) value;
        List<T> result = new ArrayList<>(rawList.size());
        int index = 0;
        for (Object item : rawList) {
            if (item == null) {
                throw new IllegalArgumentException(
                        "配置项 '" + key + "' 的第 " + (index + 1) + " 个元素不能为空");
            }
            try {
                result.add(convert(key, item, itemType));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("配置项 '" + key + "' 的第 " + (index + 1)
                        + " 个元素的值 '" + item + "' 无法转换为" + typeName(itemType), e);
            }
            index++;
        }
        return result;
    }

    /**
     * 获取映射类型的配置值，并把每个 value 转换为指定类型
     *
     * <p>key 统一转成字符串，value 的转换规则与 {@link #get(String, Class)} 一致。
     * value 为 null 时原样保留（不参与转换），value 无法转换时抛 IllegalArgumentException。
     *
     * @param key       配置键
     * @param valueType value 类型
     * @param <T>       value 类型参数
     * @return 转换后的映射；key 不存在或值为 null 时返回 null
     */
    public <T> Map<String, T> getMap(String key, Class<T> valueType) {
        Object value = this.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("配置项 '" + key + "' 不是映射类型");
        }
        checkTypeSupported(valueType);

        Map<String, T> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            String mapKey = String.valueOf(entry.getKey());
            Object mapValue = entry.getValue();
            if (mapValue == null) {
                // null 值原样保留，由调用方决定如何处理
                result.put(mapKey, null);
                continue;
            }
            try {
                result.put(mapKey, convert(key, mapValue, valueType));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("配置项 '" + key + "' 的键 '" + mapKey
                        + "' 的值 '" + mapValue + "' 无法转换为" + typeName(valueType), e);
            }
        }
        return result;
    }

    /**
     * 直接获取原始对象（不做类型转换）
     */
    public Object get(String key) {
        return config != null ? config.get(key) : null;
    }

    /**
     * 检查配置项是否存在
     */
    public boolean contains(String key) {
        return config != null && config.containsKey(key);
    }
}
