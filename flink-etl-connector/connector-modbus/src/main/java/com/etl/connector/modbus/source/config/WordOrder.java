package com.etl.connector.modbus.source.config;

/**
 * Modbus 字序枚举
 * <p>
 * 描述 wordSize=2 时两个寄存器组合为 32bit 的字序。
 * 以 12.5f（位模式 0x41480000，字节 A=0x41、B=0x48、C=0x00、D=0x00）为例：
 * <ul>
 *   <li>ABCD：高字在前（标准大端），寄存器 [0x4148, 0x0000]</li>
 *   <li>CDAB：低字在前（威胜等国产表常见），寄存器 [0x0000, 0x4148]</li>
 * </ul>
 */
public enum WordOrder {

    /**
     * 高字在前（标准大端）
     */
    ABCD,

    /** 低字在前（威胜等国产表常见） */
    CDAB;

    /**
     * 按字序将两个寄存器组合为 32bit 整数
     *
     * @param firstReg  首寄存器值
     * @param secondReg 次寄存器值
     * @return 组合后的 32bit 值
     */
    public int combine(short firstReg, short secondReg) {
        if (this == CDAB) {
            // 低字在前：第二个寄存器为高 16 位
            return (secondReg << 16) | (firstReg & 0xFFFF);
        }
        // ABCD：高字在前（大端），第一个寄存器为高 16 位
        return (firstReg << 16) | (secondReg & 0xFFFF);
    }

    /**
     * 解析字序配置，大小写不敏感
     *
     * @param value 配置值
     * @return 对应的枚举值，无法识别时返回 null
     */
    public static WordOrder parse(String value) {
        for (WordOrder order : values()) {
            if (order.name().equalsIgnoreCase(value)) {
                return order;
            }
        }
        return null;
    }
}
