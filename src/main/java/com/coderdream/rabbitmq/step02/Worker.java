package com.coderdream.rabbitmq.step02;

import com.coderdream.rabbitmq.util.CdConstants;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class Worker {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(CdConstants.RABBITMQ_SERVER_IP);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明此队列并且持久化
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1); // 告诉RabbitMQ同一时间给一个消息给消费者

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println(" [x] Received '" + message + "'");
            try {
              try {
                doWork(message);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // 手动确认消息
            }
        };

        CancelCallback cancelCallback = consumerTag -> {
            System.out.println(" [x] Consumer cancelled");
        };

        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, cancelCallback); // autoAck 设置为 false
    }

    private static void doWork(String task) throws InterruptedException {
        for (char ch : task.toCharArray()) {
            if (ch == '.')
                Thread.sleep(1000); // 这里是假装我们很忙
        }
    }
}
