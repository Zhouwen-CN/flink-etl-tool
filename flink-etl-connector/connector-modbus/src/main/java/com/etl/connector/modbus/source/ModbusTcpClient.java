package com.etl.connector.modbus.source;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 轻量 Modbus TCP 客户端
 * <p>
 * 使用 Java 原生 Socket 实现 Modbus TCP 协议，仅支持功能码 0x03（Read Holding Registers）。
 * 协议编解码逻辑拆为包级静态方法 {@link #buildReadRequest} 与 {@link #parseReadResponse}，
 * 便于脱离 socket 进行单元测试。
 */
@Slf4j
public class ModbusTcpClient implements Closeable {

    /** 读取保持寄存器功能码 */
    private static final int FUNCTION_READ_HOLDING_REGISTERS = 0x03;
    /** 异常响应标志位：功能码 | 0x80 */
    private static final int EXCEPTION_FLAG = 0x80;
    /** MBAP 头长度 */
    private static final int MBAP_HEADER_LENGTH = 7;

    private final String ip;
    private final int port;
    private final int deviceId;
    private final int timeoutMs;

    private Socket socket;
    private InputStream in;
    private OutputStream out;
    /** 事务标识符，每次请求自增（0-65535 回绕） */
    private int transactionId = 0;

    public ModbusTcpClient(String ip, int port, int deviceId, int timeoutMs) {
        this.ip = ip;
        this.port = port;
        this.deviceId = deviceId;
        this.timeoutMs = timeoutMs;
    }

    /**
     * 建立 TCP 连接，设置连接超时与读取超时（共用 timeoutMs）
     */
    public void connect() throws IOException {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(ip, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            in = socket.getInputStream();
            out = socket.getOutputStream();
            log.info("Modbus TCP 连接成功: {}:{}", ip, port);
        } catch (IOException e) {
            close();
            throw new IOException("Modbus TCP 连接失败: " + ip + ":" + port, e);
        }
    }

    /**
     * 读取保持寄存器（功能码 0x03）
     *
     * @param address  起始地址
     * @param quantity 寄存器数量
     * @return 寄存器值数组
     */
    public short[] readHoldingRegisters(int address, int quantity) throws IOException {
        transactionId = (transactionId + 1) & 0xFFFF;
        byte[] request = buildReadRequest(transactionId, deviceId, address, quantity);
        out.write(request);
        out.flush();
        return parseReadResponse(in, transactionId, quantity);
    }

    /**
     * 构建 Read Holding Registers 请求帧（MBAP 7 字节 + PDU 5 字节 = 12 字节）
     */
    static byte[] buildReadRequest(int transactionId, int deviceId, int address, int quantity) {
        byte[] frame = new byte[12];
        // MBAP
        frame[0] = (byte) ((transactionId >> 8) & 0xFF);
        frame[1] = (byte) (transactionId & 0xFF);
        frame[2] = 0x00; // protocol id high
        frame[3] = 0x00; // protocol id low
        frame[4] = 0x00; // length high
        frame[5] = 0x06; // length low = 6
        frame[6] = (byte) (deviceId & 0xFF);
        // PDU
        frame[7] = (byte) FUNCTION_READ_HOLDING_REGISTERS;
        frame[8] = (byte) ((address >> 8) & 0xFF);
        frame[9] = (byte) (address & 0xFF);
        frame[10] = (byte) ((quantity >> 8) & 0xFF);
        frame[11] = (byte) (quantity & 0xFF);
        return frame;
    }

    /**
     * 解析 Read Holding Registers 响应帧
     *
     * @param in                 输入流
     * @param expectedTxId       期望的 Transaction ID
     * @param expectedQuantity   期望的寄存器数量
     * @return 寄存器值数组
     */
    static short[] parseReadResponse(InputStream in, int expectedTxId, int expectedQuantity)
            throws IOException {
        // 1. 读满 MBAP 头（7 字节）
        byte[] header = new byte[MBAP_HEADER_LENGTH];
        readFully(in, header, MBAP_HEADER_LENGTH);

        int txId = ((header[0] & 0xFF) << 8) | (header[1] & 0xFF);
        if (txId != expectedTxId) {
            throw new IOException(String.format(
                    "Modbus 响应 Transaction ID 不匹配: 期望 %d, 实际 %d", expectedTxId, txId));
        }

        int length = ((header[4] & 0xFF) << 8) | (header[5] & 0xFF);
        // length 包含 unitId(1) + 后续 PDU；已读掉 unitId，故 PDU 剩余 = length - 1
        int pduLength = length - 1;
        if (pduLength < 2) {
            throw new IOException("Modbus 响应长度字段非法: " + length);
        }

        // 2. 读满 PDU 剩余字节
        byte[] pdu = new byte[pduLength];
        readFully(in, pdu, pduLength);

        int functionCode = pdu[0] & 0xFF;
        // 3. 异常响应判断
        if ((functionCode & EXCEPTION_FLAG) != 0) {
            int exceptionCode = pdu[1] & 0xFF;
            throw new IOException("Modbus 异常响应: 功能码=0x" +
                    Integer.toHexString(functionCode) + ", 异常码=" + exceptionCode);
        }

        // 4. 正常响应：pdu[1] = byteCount, 后续为寄存器数据
        int byteCount = pdu[1] & 0xFF;
        int expectedByteCount = expectedQuantity * 2;
        if (byteCount != expectedByteCount) {
            throw new IOException(String.format(
                    "Modbus 响应字节数不符: 期望 %d, 实际 %d", expectedByteCount, byteCount));
        }
        if (pdu.length < 2 + byteCount) {
            throw new IOException(String.format(
                    "Modbus 响应数据不足: 期望 %d 字节, 实际 %d", byteCount, pdu.length - 2));
        }

        short[] registers = new short[expectedQuantity];
        for (int i = 0; i < expectedQuantity; i++) {
            int hi = pdu[2 + i * 2] & 0xFF;
            int lo = pdu[2 + i * 2 + 1] & 0xFF;
            registers[i] = (short) ((hi << 8) | lo);
        }
        return registers;
    }

    /**
     * 从流中读满 len 字节，不足则抛异常
     */
    private static void readFully(InputStream in, byte[] buffer, int len) throws IOException {
        int offset = 0;
        while (offset < len) {
            int read = in.read(buffer, offset, len - offset);
            if (read < 0) {
                throw new IOException(String.format(
                        "Modbus 响应流提前结束: 期望 %d 字节, 实际读取 %d", len, offset));
            }
            offset += read;
        }
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.close();
                log.info("Modbus TCP 连接已释放");
            } catch (IOException e) {
                log.warn("关闭 Modbus TCP 连接异常", e);
            } finally {
                socket = null;
                in = null;
                out = null;
            }
        }
    }
}
