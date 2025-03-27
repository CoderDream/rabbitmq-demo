package com.coderdream.rabbitmq.step01;

import com.coderdream.rabbitmq.util.CdConstants;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * 消息接收者，从名为 "hello" 的队列中接收消息。
 */
@Slf4j // 使用 Lombok 的 @Slf4j 注解，自动生成 log 实例
public class Receive {

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
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            // 5. 创建消费者 (使用 DeliverCallback 处理接收到的消息)
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                log.info(" [x] Received '" + message + "'");
            };

            // 6. 创建消费者取消回调函数 (当消费者被取消时执行)
            CancelCallback cancelCallback = consumerTag -> {
                log.info(" [x] Consumer cancelled");
            };

            // 7. 消费消息
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback); // autoAck is true
            log.info("开始消费消息，队列：{}，自动确认：true", QUEUE_NAME);

            // 为了防止程序过早退出，可以添加一个无限循环
            while (true) {
                Thread.sleep(1000); // 每隔 1 秒检查一次
            }


        } catch (IOException | TimeoutException | InterruptedException e) {
            log.error("发生异常", e); // 记录异常信息
        } finally {
            // 8. 关闭连接和信道 (在 finally 块中确保连接和信道被关闭)
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
