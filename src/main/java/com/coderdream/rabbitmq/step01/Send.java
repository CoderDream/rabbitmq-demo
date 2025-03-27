package com.coderdream.rabbitmq.step01;

import com.coderdream.rabbitmq.util.CdConstants;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 消息发送者，向名为 "hello" 的队列发送消息。
 */
@Slf4j // 使用 Lombok 的 @Slf4j 注解，自动生成 log 实例
public class Send {

    private final static String QUEUE_NAME = "hello"; // 队列名称

    public static void main(String[] args) {
        Connection connection = null;
        Channel channel = null;
        try {
            // 1. 创建连接工厂
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(CdConstants.RABBITMQ_SERVER_IP); // 设置 RabbitMQ 服务器地址

            // 2. 建立到 RabbitMQ 服务器的连接
            connection = factory.newConnection();
            log.info("连接到 RabbitMQ 服务器: {}", CdConstants.RABBITMQ_SERVER_IP);

            // 3. 创建信道
            channel = connection.createChannel();
            log.info("创建信道");

            // 4. 声明队列 (如果队列不存在，则创建)
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            log.info("声明队列: {}", QUEUE_NAME);

            // 5. 定义消息内容
            String message = "Hello World!";
            log.info("发送消息: {}", message);

            // 6. 发送消息
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
            log.info("消息已发送到队列: {}", QUEUE_NAME);

        } catch (IOException | TimeoutException e) {
            log.error("发生异常", e); // 记录异常信息
        } finally {
            // 7. 关闭连接和信道 (在 finally 块中确保连接和信道被关闭)
            try {
                if (channel != null && channel.isOpen()) {
                    channel.close();
                    log.info("关闭信道");
                }
                if (connection != null && connection.isOpen()) {
                    connection.close();
                    log.info("关闭连接");
                }
            } catch (IOException | TimeoutException e) {
                log.error("关闭连接或信道时发生异常", e);
            }
        }
    }
}
