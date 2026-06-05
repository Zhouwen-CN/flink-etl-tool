package com.etl.connector.modbus.source;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class ModbusTcpClientTest {

    /**
     * 验证请求帧格式：MBAP(7) + PDU(5) = 12 字节
     * transactionId=1, deviceId=1, address=0x0010, quantity=0x0002
     */
    @Test
    void testBuildReadRequest() {
        byte[] frame = ModbusTcpClient.buildReadRequest(1, 1, 0x0010, 0x0002);

        assertEquals(12, frame.length);
        // Transaction ID = 1
        assertEquals(0x00, frame[0] & 0xFF);
        assertEquals(0x01, frame[1] & 0xFF);
        // Protocol ID = 0
        assertEquals(0x00, frame[2] & 0xFF);
        assertEquals(0x00, frame[3] & 0xFF);
        // Length = 6
        assertEquals(0x00, frame[4] & 0xFF);
        assertEquals(0x06, frame[5] & 0xFF);
        // Unit ID = 1
        assertEquals(0x01, frame[6] & 0xFF);
        // Function Code = 0x03
        assertEquals(0x03, frame[7] & 0xFF);
        // Start Address = 0x0010
        assertEquals(0x00, frame[8] & 0xFF);
        assertEquals(0x10, frame[9] & 0xFF);
        // Quantity = 0x0002
        assertEquals(0x00, frame[10] & 0xFF);
        assertEquals(0x02, frame[11] & 0xFF);
    }

    /**
     * 验证正常响应解析：读取 2 个寄存器 [0x000A, 0x000B]
     */
    @Test
    void testParseReadResponse_normal() throws IOException {
        byte[] response = {
                0x00, 0x01,       // transaction id
                0x00, 0x00,       // protocol id
                0x00, 0x07,       // length = 7
                0x01,             // unit id
                0x03,             // function code
                0x04,             // byte count
                0x00, 0x0A,       // register 0 = 10
                0x00, 0x0B        // register 1 = 11
        };
        short[] data = ModbusTcpClient.parseReadResponse(
                new ByteArrayInputStream(response), 1, 2);

        assertEquals(2, data.length);
        assertEquals(10, data[0]);
        assertEquals(11, data[1]);
    }

    /**
     * 验证异常响应：function code = 0x83，异常码 0x02
     */
    @Test
    void testParseReadResponse_exception() {
        byte[] response = {
                0x00, 0x01,       // transaction id
                0x00, 0x00,       // protocol id
                0x00, 0x03,       // length = 3 (unit + fc + exceptionCode)
                0x01,             // unit id
                (byte) 0x83,      // function code = 0x03 | 0x80
                0x02              // exception code = ILLEGAL DATA ADDRESS
        };
        IOException ex = assertThrows(IOException.class, () ->
                ModbusTcpClient.parseReadResponse(
                        new ByteArrayInputStream(response), 1, 2));
        assertTrue(ex.getMessage().contains("异常"));
    }

    /**
     * 验证 Transaction ID 不匹配抛异常
     */
    @Test
    void testParseReadResponse_transactionIdMismatch() {
        byte[] response = {
                0x00, 0x02,       // transaction id = 2 (期望 1)
                0x00, 0x00,
                0x00, 0x05,       // length
                0x01,
                0x03,
                0x02,
                0x00, 0x0A
        };
        IOException ex = assertThrows(IOException.class, () ->
                ModbusTcpClient.parseReadResponse(
                        new ByteArrayInputStream(response), 1, 1));
        assertTrue(ex.getMessage().contains("Transaction"));
    }

    /**
     * 验证响应字节不足（流提前结束）抛异常
     */
    @Test
    void testParseReadResponse_insufficientData() {
        byte[] response = {
                0x00, 0x01,
                0x00, 0x00,
                0x00, 0x07,       // 声称还有 7 字节，实际不足
                0x01,
                0x03,
                0x04,
                0x00, 0x0A        // 只给了 1 个寄存器，缺 1 个
        };
        assertThrows(IOException.class, () ->
                ModbusTcpClient.parseReadResponse(
                        new ByteArrayInputStream(response), 1, 2));
    }
}
