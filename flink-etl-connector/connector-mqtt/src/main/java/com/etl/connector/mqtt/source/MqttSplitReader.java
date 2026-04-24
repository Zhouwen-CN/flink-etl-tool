package com.etl.connector.mqtt.source;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 分片读取器
 * 使用 Paho 客户端订阅 topic，阻塞读取消息
 */
@Slf4j
public class MqttSplitReader implements MqttCallback {

    private static final int QOS = 1;
    private static final int QUEUE_CAPACITY = 1000;
    private static final long FETCH_TIMEOUT_MS = 100;

    private final Queue<MqttSplit> pendingSplits = new ArrayDeque<>();
    private final Set<String> finishedSplits = new HashSet<>();

    /** 消息阻塞队列 */
    private final BlockingQueue<MqttMessageRecord> messageQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    /** MQTT 客户端 */
    private MqttClient mqttClient;

    /** 当前订阅的 topic */
    private String currentTopic;

    /** 是否已连接 */
    private volatile boolean connected = false;

    /**
     * 阻塞式提取数据
     *
     * @return 包含分片 ID 的记录集合
     * @throws IOException 读取异常
     */
    public RecordsWithSplitIds<MqttMessageRecord> fetch() throws IOException {
        RecordsBySplits.Builder<MqttMessageRecord> builder = new RecordsBySplits.Builder<>();

        // 尝试从队列获取消息
        MqttMessageRecord record;
        try {
            record = messageQueue.poll(FETCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return builder.build();
        }

        if (record != null) {
            // 有消息，添加到结果
            builder.add(record.getSplitId(), record);
        }

        // 检查是否有已完成的分片
        builder.addFinishedSplits(finishedSplits);

        return builder.build();
    }

    /**
     * 处理分片变动
     *
     * @param splitsChanges 分片变动
     */
    public void handleSplitsChanges(SplitsChange<MqttSplit> splitsChanges) {
        for (MqttSplit split : splitsChanges.splits()) {
            pendingSplits.add(split);
            log.debug("接收到 MQTT 分片: {}", split);
        }

        // 处理第一个分片（启动连接）
        if (!connected && !pendingSplits.isEmpty()) {
            MqttSplit split = pendingSplits.poll();
            connectAndSubscribe(split);
        }
    }

    /**
     * 连接 MQTT broker 并订阅 topic
     */
    private void connectAndSubscribe(MqttSplit split) {
        MqttSourceConfig config = split.getConfig();

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
            mqttClient.setCallback(this);

            // 连接
            mqttClient.connect(options);
            connected = true;
            currentTopic = config.getTopic();

            log.info("MQTT 客户端已连接: broker={}, clientId={}, topic={}",
                    config.getBroker(), config.getClientId(), currentTopic);

            // 订阅 topic
            mqttClient.subscribe(currentTopic, QOS);
            log.info("已订阅 topic: {}, QoS: {}", currentTopic, QOS);

        } catch (MqttException e) {
            log.error("MQTT 连接失败: {}", e.getMessage(), e);
            throw new RuntimeException("MQTT 连接失败: " + e.getMessage(), e);
        }
    }

    // ===== MqttCallback 实现 =====

    @Override
    public void connectionLost(Throwable cause) {
        log.warn("MQTT 连接丢失: {}", cause.getMessage());
        connected = false;
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

            // 射入队列
            MqttMessageRecord record = new MqttMessageRecord("mqtt-split-0", jsonContent);
            messageQueue.put(record);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("消息入队被中断");
        } catch (Exception e) {
            log.error("处理 MQTT 消息失败: {}", e.getMessage(), e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // 不处理发布确认
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
                mqttClient.unsubscribe(currentTopic);
                mqttClient.disconnect();
                mqttClient.close();
                log.info("MQTT 客户端已关闭");
            } catch (MqttException e) {
                log.warn("关闭 MQTT 客户端异常: {}", e.getMessage());
            }
        }
        connected = false;
    }
}