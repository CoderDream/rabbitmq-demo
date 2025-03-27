//package com.coderdream.rabbitmq.kafka;
//
//import org.apache.kafka.clients.admin.*;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.KafkaFuture;
//import org.apache.kafka.common.config.ConfigResource;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.time.Duration;
//import java.util.*;
//import java.util.concurrent.ExecutionException;
//
//public class KafkaUtils {
//
//    private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
//
//    private final String bootstrapServers;
//    private final String groupId;
//    private final Properties producerProps;
//    private final Properties consumerProps;
//    private final Properties adminClientProps;
//
//    private KafkaProducer<String, String> producer;
//    private KafkaConsumer<String, String> consumer;
//    private AdminClient adminClient;
//
//    public KafkaUtils(String bootstrapServers, String groupId) {
//        this.bootstrapServers = bootstrapServers;
//        this.groupId = groupId;
//
//        // Producer 配置
//        producerProps = new Properties();
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        // Consumer 配置
//        consumerProps = new Properties();
//        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早的消息开始消费
//
//        // AdminClient 配置
//        adminClientProps = new Properties();
//        adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//    }
//
//    // ------------------------- 初始化方法 -------------------------
//
//    /**
//     * 初始化 Kafka Producer
//     */
//    public void initProducer() {
//        if (producer == null) {
//            producer = new KafkaProducer<>(producerProps);
//            logger.info("Kafka Producer 初始化完成");
//        }
//    }
//
//    /**
//     * 初始化 Kafka Consumer
//     */
//    public void initConsumer() {
//        if (consumer == null) {
//            consumer = new KafkaConsumer<>(consumerProps);
//            logger.info("Kafka Consumer 初始化完成，GroupID: {}", groupId);
//        }
//    }
//
//    /**
//     * 初始化 Kafka AdminClient
//     */
//    public void initAdminClient() {
//        if (adminClient == null) {
//            adminClient = AdminClient.create(adminClientProps);
//            logger.info("Kafka AdminClient 初始化完成");
//        }
//    }
//
//
//    // ------------------------- Topic 操作 -------------------------
//
//    /**
//     * 创建 Topic
//     * @param topicName Topic 名称
//     * @param numPartitions 分区数量
//     * @param replicationFactor 副本因子
//     * @throws ExecutionException
//     * @throws InterruptedException
//     */
//    public void createTopic(String topicName, int numPartitions, short replicationFactor) throws ExecutionException, InterruptedException {
//        initAdminClient();
//        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
//        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
//        result.all().get();
//        logger.info("Topic 创建成功: {}", topicName);
//    }
//
//    /**
//     * 删除 Topic
//     * @param topicName Topic 名称
//     * @throws ExecutionException
//     * @throws InterruptedException
//     */
//    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
//        initAdminClient();
//        DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
//        result.all().get();
//        logger.info("Topic 删除成功: {}", topicName);
//    }
//
//    /**
//     * 获取 Topic 信息
//     * @param topicName Topic 名称
//     * @return TopicDescription 对象
//     * @throws ExecutionException
//     * @throws InterruptedException
//     */
//    public TopicDescription getTopicDescription(String topicName) throws ExecutionException, InterruptedException {
//        initAdminClient();
//        DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topicName));
//        Map<String, KafkaFuture<TopicDescription>> values = result.values();
//        KafkaFuture<TopicDescription> future = values.get(topicName);
//        if (future != null) {
//            return future.get();
//        } else {
//            return null; // 或者抛出异常，取决于你的业务逻辑
//        }
//    }
//
//    /**
//     * 获取所有 Topic 名称
//     * @return Topic 名称列表
//     * @throws ExecutionException
//     * @throws InterruptedException
//     */
//    public List<String> listTopics() throws ExecutionException, InterruptedException {
//        initAdminClient();
//        ListTopicsResult result = adminClient.listTopics();
//        return new ArrayList<>(result.names().get());
//    }
//
//    /**
//     * 修改 Topic 配置
//     * @param topicName Topic 名称
//     * @param configName 配置名称
//     * @param configValue 配置值
//     * @throws ExecutionException
//     * @throws InterruptedException
//     */
//    public void alterTopicConfig(String topicName, String configName, String configValue) throws ExecutionException, InterruptedException {
//        initAdminClient();
//        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
//        Config config = new Config(Collections.singletonList(new ConfigEntry(configName, configValue)));
//
//        AlterConfigsResult result = adminClient.alterConfigs(Collections.singletonMap(resource, config));
//        result.all().get();
//        logger.info("Topic {} 配置 {} 修改为 {}", topicName, configName, configValue);
//    }
//
//
//    // ------------------------- 消息发送和消费 -------------------------
//
//    /**
//     * 发送消息
//     * @param topicName Topic 名称
//     * @param key 消息 Key
//     * @param value 消息 Value
//     * @throws ExecutionException
//     * @throws InterruptedException
//     */
//    public void sendMessage(String topicName, String key, String value) throws ExecutionException, InterruptedException {
//        initProducer();
//        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
//        producer.send(record).get();
//        logger.info("发送消息到 Topic: {}, Key: {}, Value: {}", topicName, key, value);
//    }
//
//    /**
//     * 消费消息
//     * @param topicName Topic 名称
//     * @return 消息列表
//     */
//    public List<String> consumeMessage(String topicName) {
//        initConsumer();
//        consumer.subscribe(Collections.singletonList(topicName));
//        List<String> messages = new ArrayList<>();
//        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
//        records.forEach(record -> {
//            messages.add(String.format("Key: %s, Value: %s, Partition: %s, Offset:%s%n", record.key(), record.value(), record.partition(), record.offset()));
//        });
//        consumer.commitSync(); // Commit offset
//        return messages;
//    }
//
//    // ------------------------- 其他 -------------------------
//
//    /**
//     * 关闭 Producer
//     */
//    public void closeProducer() {
//        if (producer != null) {
//            producer.close();
//            logger.info("Kafka Producer 关闭");
//        }
//    }
//
//    /**
//     * 关闭 Consumer
//     */
//    public void closeConsumer() {
//        if (consumer != null) {
//            consumer.close();
//            logger.info("Kafka Consumer 关闭");
//        }
//    }
//
//    /**
//     * 关闭 AdminClient
//     */
//    public void closeAdminClient() {
//        if (adminClient != null) {
//            adminClient.close();
//            logger.info("Kafka AdminClient 关闭");
//        }
//    }
//
//    /**
//     * 关闭所有客户端
//     */
//    public void close() {
//        closeProducer();
//        closeConsumer();
//        closeAdminClient();
//    }
//
//    // ------------------------- Main 方法示例 -------------------------
//    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
//        String bootstrapServers = "192.168.3.165:9092"; // 替换为你的 Kafka Broker 地址
//        String groupId = "test-group";
//
//        KafkaUtils kafkaUtils = new KafkaUtils(bootstrapServers, groupId);
//
//        String topicName = "test-topic";
//        int numPartitions = 1;
//        short replicationFactor = 1;
//
//        // 创建 Topic
//        kafkaUtils.createTopic(topicName, numPartitions, replicationFactor);
//
//        // 发送消息
//        kafkaUtils.sendMessage(topicName, "key1", "value1");
//        kafkaUtils.sendMessage(topicName, "key2", "value2");
//
//        // 消费消息
//        List<String> messages = kafkaUtils.consumeMessage(topicName);
//        messages.forEach(System.out::println);
//
//        // 获取 Topic 信息
//        TopicDescription topicDescription = kafkaUtils.getTopicDescription(topicName);
//        System.out.println("Topic Description: " + topicDescription);
//
//        // 列出所有 Topic
//        List<String> topics = kafkaUtils.listTopics();
//        System.out.println("All Topics: " + topics);
//
//        //修改topic配置
//        kafkaUtils.alterTopicConfig(topicName, "retention.ms", String.valueOf(60 * 60 * 1000));
//
//        // 删除 Topic
//        kafkaUtils.deleteTopic(topicName);
//
//        // 关闭所有客户端
//        kafkaUtils.close();
//    }
//
//    // 内部类，用于表示配置条目
//    static class ConfigEntry extends org.apache.kafka.common.config.ConfigEntry {
//
//        public ConfigEntry(String name, String value) {
//            super(name, value);
//        }
//    }
//}
