package com.etl.connector.modbus.source.exception;

import lombok.Getter;

import java.io.IOException;

/**
 * Modbus 协议层异常响应
 * <p>
 * 表示设备对请求做出了完整、合法的响应，但以异常码拒绝了该请求
 * （如异常码 02 Illegal Data Address，表示请求的地址在设备上不存在）。
 *
 * <p>
 * 该异常与连接层故障（超时、断连、流不同步）区分开：
 * <ul>
 *   <li>协议层异常：设备通信正常，仅该地址不可读，可跳过该点位继续读取其余点位</li>
 *   <li>连接层故障：数据流已不可信，必须让作业失败并由 Flink 从 checkpoint 恢复</li>
 * </ul>
 */
@Getter
public class ModbusProtocolException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Modbus 异常码
     * -- GETTER --
     * 获取 Modbus 异常码
     *
     */
    private final int exceptionCode;

    public ModbusProtocolException(int exceptionCode, String message) {
        super(message);
        this.exceptionCode = exceptionCode;
    }

}
