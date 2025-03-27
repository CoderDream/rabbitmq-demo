package com.coderdream.rabbitmq.kafka;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaUtilTest {

    private static final String TEST_GROUP_ID = "test-group-" + System.currentTimeMillis();
    private static final String TEST_MESSAGE = "Hello Kafka!";
    private static final long TIMEOUT_MS = 5000L;

    @BeforeAll
    static void setUp() {
        KafkaUtil.sendMessage(TEST_MESSAGE);
    }

    @AfterAll
    static void tearDown() {
    }

    @Test
    void testSendMessage() {
        assertDoesNotThrow(() -> KafkaUtil.sendMessage("Test message from JUnit"));
    }

    @Test
    void testConsumeMessage() {
        String consumedMessage = KafkaUtil.consumeMessage(TEST_GROUP_ID, TIMEOUT_MS, "earliest");
        assertNotNull(consumedMessage, "Should consume a message");
        assertEquals(TEST_MESSAGE, consumedMessage, "Consumed message should match sent message");
    }

    @Test
    void testConsumeNoMessage() {
        String consumedMessage = KafkaUtil.consumeMessage(TEST_GROUP_ID + "-new", 1000L, "latest");
        assertNull(consumedMessage, "Should return null when no new message is available");
    }
}
