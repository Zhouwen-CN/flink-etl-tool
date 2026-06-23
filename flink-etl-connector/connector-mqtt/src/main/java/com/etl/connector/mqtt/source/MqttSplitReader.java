package com.etl.connector.mqtt.source;

import com.etl.connector.mqtt.source.config.MqttSourceConfig;
import com.etl.core.schema.EtlSchema;
import com.etl.core.schema.JsonToRowConverter;
import com.etl.core.source.AbstractSplitReader;
import com.etl.core.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.Row;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 分片读取器
 * 使用 Paho 客户端订阅 topic，阻塞读取消息
 */
@Slf4j
public class MqttSplitReader extends AbstractSplitReader<Row, MqttSplit> {

    private static final int QOS = 1;
    private static final int QUEUE_CAPACITY = 1024;
    private static final long FETCH_TIMEOUT_MS = 100;

    /**
     * 消息阻塞队列
     */
    private final BlockingQueue<Row> messageQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);


    /**
     * MQTT 客户端
     */
    private MqttClient mqttClient;

    /**
     * 当前分片
     */
    private MqttSplit currentSplit;

    /**
     * 阻塞式提取数据
     *
     * @return 包含分片 ID 的记录集合
     * @throws IOException 读取异常
     */
    @Override
    public RecordsWithSplitIds<Row> fetch() throws IOException {
        RecordsBySplits.Builder<Row> builder = new RecordsBySplits.Builder<>();

        if (currentSplit == null) {
            MqttSplit split = pendingSplits.poll();
            if (split == null) {
                return builder.build();
            }
            currentSplit = split;
            connectAndSubscribe();
        }

        // 尝试从队列获取消息
        Row record;
        try {
            record = messageQueue.poll(FETCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return builder.build();
        }

        if (record != null) {
            // 有消息，添加到结果
            builder.add(currentSplit.getSplitId(), record);
        }

        return builder.build();
    }

    /**
     * 连接 MQTT broker 并订阅 topic
     */
    private void connectAndSubscribe() {
        MqttSourceConfig config = currentSplit.getConfig();
        try {
            // 创建 MQTT 客户端
            mqttClient = new MqttClient(config.getBroker(), config.getClientId());

            // 设置连接选项
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setKeepAliveInterval(60);

            // 设置认证信息（可选）
            if (config.getUsername() != null && !config.getUsername().isEmpty()) {
                options.setUserName(config.getUsername());
            }
            if (config.getPassword() != null && !config.getPassword().isEmpty()) {
                options.setPassword(config.getPassword().toCharArray());
            }

            // 设置回调
            mqttClient.setCallback(new MqttCallbackImpl(config.getSchema(), messageQueue));

            // 连接
            mqttClient.connect(options);

            // 订阅 topic
            String topic = config.getTopic();
            mqttClient.subscribe(topic, QOS);
            log.info("MQTT 客户端已连接: broker={}, clientId={}, topic={}, QoS: {}",
                    config.getBroker(), config.getClientId(), topic, QOS);
        } catch (MqttException e) {
            log.error("MQTT 连接失败: {}", e.getMessage(), e);
            throw new RuntimeException("MQTT 连接失败: " + e.getMessage(), e);
        }
    }

    /**
     * 唤醒阻塞的提取操作
     */
    public void wakeUp() {
        // 唤醒阻塞的 fetch 操作
    }

    /**
     * 关闭读取器，释放资源
     *
     * @throws Exception 关闭异常
     */
    public void close() throws Exception {
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                mqttClient.disconnect();
                mqttClient.close();
                log.info("MQTT 客户端已关闭");
            } catch (MqttException e) {
                log.warn("关闭 MQTT 客户端异常: {}", e.getMessage());
            }
        }
    }

    public static class MqttCallbackImpl implements MqttCallback {

        private final EtlSchema schema;
        private final BlockingQueue<Row> messageQueue;

        public MqttCallbackImpl(EtlSchema schema, BlockingQueue<Row> messageQueue) {
            this.schema = schema;
            this.messageQueue = messageQueue;
        }


        @Override
        public void connectionLost(Throwable cause) {
            log.warn("MQTT 连接丢失: {}", cause.getMessage());
            // Paho 会自动重连（automaticReconnect=true）
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            try {
                byte[] payload = message.getPayload();
                if (payload == null || payload.length == 0) {
                    log.warn("收到空消息，topic: {}", topic);
                    return;
                }

                String jsonContent = new String(payload, StandardCharsets.UTF_8);
                log.debug("收到 MQTT 消息: topic={}, payload={}", topic, jsonContent);

                JsonNode jsonNode = JsonUtil.readTree(jsonContent);

                // 转换为 Row
                List<Row> rows = JsonToRowConverter.convertJsonToRows(jsonNode, schema);
                for (Row row : rows) {
                    if (!messageQueue.offer(row)) {
                        log.warn("消息队列已满，丢弃消息");
                    }
                }

            } catch (Exception e) {
                log.error("处理 MQTT 消息失败: {}", e.getMessage(), e);
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            // 不处理发布确认
        }
    }
}