package com.indivaragroup.training.kafka.configuration.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Kafka Consumer to receive messages from Kafka brokers.
 * <p>
 * This class configures Kafka consumers with String deserializers for both keys and values.
 * JSON deserialization to domain objects is performed manually in consumer services using
 * ObjectMapper, providing flexibility and avoiding deprecated Spring Kafka JSON deserializers.
 * </p>
 *
 * <p><b>Configuration Highlights:</b></p>
 * <ul>
 *   <li><b>Deserialization:</b> String-based deserialization with manual JSON parsing</li>
 *   <li><b>Concurrency:</b> Parallel message processing with 3 concurrent consumers per topic</li>
 *   <li><b>Reliability:</b> Automatic offset management with batch acknowledgment</li>
 *   <li><b>Scalability:</b> Consumer group coordination for horizontal scaling</li>
 * </ul>
 *
 * <p><b>Consumer Groups:</b></p>
 * <p>
 * All consumers share the same consumer group ID, enabling load balancing across multiple
 * consumer instances. Each partition is assigned to only one consumer within a group,
 * ensuring messages are processed exactly once per consumer group.
 * </p>
 *
 * <p><b>Offset Management:</b></p>
 * <ul>
 *   <li><b>Auto-commit enabled:</b> Offsets are automatically committed periodically</li>
 *   <li><b>Batch acknowledgment:</b> Offsets committed after processing a batch of records</li>
 *   <li><b>Earliest offset reset:</b> Starts from the beginning if no offset exists</li>
 * </ul>
 *
 * <p><b>Best Practices Applied:</b></p>
 * <ul>
 *   <li>Concurrency for parallel processing and improved throughput</li>
 *   <li>Session timeout and heartbeat configuration for reliable group coordination</li>
 *   <li>Batch processing for efficient offset management</li>
 *   <li>Configurable poll records for memory management</li>
 * </ul>
 *
 * @author Supriyandi La Awe
 * @version 1.0.0
 * @since 2025-11-22
 * @see EnableKafka
 * @see ConsumerFactory
 * @see ConcurrentKafkaListenerContainerFactory
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfiguration {

    /**
     * Kafka broker server addresses.
     * <p>
     * This value is injected from the application configuration file using the key:
     * {@code spring.kafka.bootstrap-servers}
     * </p>
     *
     * <p><b>Format:</b> {@code host1:port1,host2:port2,...}</p>
     * <p><b>Example:</b> {@code localhost:9092} or {@code kafka-broker1:9092,kafka-broker2:9092}</p>
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Consumer group identifier for coordinating message consumption.
     * <p>
     * This value is injected from the application configuration file using the key:
     * {@code spring.kafka.consumer.group-id}
     * </p>
     *
     * <p><b>Purpose:</b></p>
     * <ul>
     *   <li>Groups multiple consumer instances for load balancing</li>
     *   <li>Ensures each partition is consumed by only one consumer in the group</li>
     *   <li>Enables horizontal scaling by adding more consumer instances</li>
     *   <li>Maintains offset tracking per consumer group</li>
     * </ul>
     *
     * <p><b>Example:</b> {@code wallet-transaction-group}</p>
     */
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    /**
     * Creates and configures a ConsumerFactory bean for Kafka message consumption.
     * <p>
     * This factory is configured with String deserializers for both keys and values,
     * requiring manual JSON deserialization in the consumer service layer. This approach
     * provides better control over deserialization and error handling while avoiding
     * deprecated Spring Kafka JSON deserializers.
     * </p>
     *
     * <p><b>Configuration Parameters:</b></p>
     * <ul>
     *   <li><b>BOOTSTRAP_SERVERS:</b> Kafka broker addresses for initial connection</li>
     *   <li><b>GROUP_ID:</b> Consumer group identifier for coordination and load balancing</li>
     *   <li><b>KEY_DESERIALIZER:</b> StringDeserializer for message keys</li>
     *   <li><b>VALUE_DESERIALIZER:</b> StringDeserializer for message values (JSON strings)</li>
     * </ul>
     *
     * <p><b>Offset Management Configuration:</b></p>
     * <ul>
     *   <li><b>ENABLE_AUTO_COMMIT = true:</b> Automatically commit offsets at regular intervals.
     *       Simplifies offset management but may lead to duplicate processing on failure.
     *       Trade-off: Convenience vs exactly-once semantics.</li>
     *   <li><b>AUTO_OFFSET_RESET = "earliest":</b> Start consuming from the beginning of the topic
     *       when no committed offset exists. Ensures no messages are missed for new consumer groups.
     *       Alternatives: "latest" (skip old messages), "none" (throw exception).</li>
     * </ul>
     *
     * <p><b>Performance Configuration:</b></p>
     * <ul>
     *   <li><b>MAX_POLL_RECORDS = 500:</b> Maximum number of records returned in a single poll.
     *       Lower values reduce memory usage but may decrease throughput.
     *       Higher values improve throughput but increase memory footprint.
     *       Tune based on message size and processing time.</li>
     * </ul>
     *
     * <p><b>Health and Coordination Configuration:</b></p>
     * <ul>
     *   <li><b>SESSION_TIMEOUT = 30000ms (30 sec):</b> Maximum time between heartbeats before
     *       consumer is considered dead and rebalancing is triggered. Must be greater than
     *       heartbeat interval. Increase for slow processing scenarios.</li>
     *   <li><b>HEARTBEAT_INTERVAL = 10000ms (10 sec):</b> How often to send heartbeats to
     *       the coordinator. Should be 1/3 of session timeout. Ensures timely detection
     *       of consumer failures.</li>
     * </ul>
     *
     * <p><b>Rebalancing Behavior:</b></p>
     * <p>
     * When consumers join or leave the group, Kafka triggers a rebalance to redistribute
     * partitions. During rebalancing:
     * </p>
     * <ul>
     *   <li>Message consumption is paused</li>
     *   <li>Partitions are reassigned among available consumers</li>
     *   <li>Consumers resume from last committed offset</li>
     * </ul>
     *
     * <p><b>Production Recommendations:</b></p>
     * <ul>
     *   <li>Monitor consumer lag to ensure consumers keep up with producers</li>
     *   <li>Adjust max.poll.records based on message size and processing complexity</li>
     *   <li>Consider manual offset management for exactly-once semantics</li>
     *   <li>Tune session timeout based on expected processing time per batch</li>
     * </ul>
     *
     * @return ConsumerFactory configured with String deserializers and production-ready settings
     * @see ConsumerConfig
     * @see StringDeserializer
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Best Practice Configuration for Offset Management
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Best Practice Configuration for Performance and Coordination
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Creates a ConcurrentKafkaListenerContainerFactory for Kafka message listeners.
     * <p>
     * This factory creates listener containers that manage the lifecycle of Kafka consumers
     * and coordinate message consumption. It supports concurrent message processing through
     * multiple consumer threads, significantly improving throughput for I/O-bound operations.
     * </p>
     *
     * <p><b>Concurrency Model:</b></p>
     * <p>
     * With concurrency set to 3, the factory creates 3 separate consumer instances per
     * {@code @KafkaListener} method. Each consumer handles one or more partitions, enabling
     * parallel message processing within the same consumer group.
     * </p>
     *
     * <p><b>Configuration Details:</b></p>
     * <ul>
     *   <li><b>Concurrency = 3:</b> Creates 3 concurrent consumer threads per listener.
     *       <ul>
     *         <li>Optimal for topics with 3+ partitions (one consumer per partition)</li>
     *         <li>Increases throughput by processing messages in parallel</li>
     *         <li>Useful for I/O-bound operations (database writes, API calls)</li>
     *         <li>Should not exceed number of partitions in the topic</li>
     *       </ul>
     *   </li>
     *   <li><b>Poll Timeout = 3000ms (3 sec):</b> Maximum time to wait for messages in a poll.
     *       <ul>
     *         <li>Shorter timeout = more responsive to consumer shutdown</li>
     *         <li>Longer timeout = fewer empty polls, less overhead</li>
     *         <li>Balance between responsiveness and efficiency</li>
     *       </ul>
     *   </li>
     *   <li><b>Ack Mode = BATCH:</b> Commits offsets after processing entire batch.
     *       <ul>
     *         <li>More efficient than acknowledging each record individually</li>
     *         <li>Reduces number of offset commit operations</li>
     *         <li>May result in duplicate processing if consumer fails mid-batch</li>
     *         <li>Good balance between performance and reliability</li>
     *       </ul>
     *   </li>
     * </ul>
     *
     * <p><b>Acknowledgment Modes Comparison:</b></p>
     * <table border="1">
     *   <tr>
     *     <th>Mode</th>
     *     <th>Commits</th>
     *     <th>Performance</th>
     *     <th>Duplicate Risk</th>
     *   </tr>
     *   <tr>
     *     <td>RECORD</td>
     *     <td>After each record</td>
     *     <td>Slowest</td>
     *     <td>Lowest</td>
     *   </tr>
     *   <tr>
     *     <td>BATCH</td>
     *     <td>After batch</td>
     *     <td>Fast</td>
     *     <td>Moderate</td>
     *   </tr>
     *   <tr>
     *     <td>MANUAL</td>
     *     <td>Explicit in code</td>
     *     <td>Variable</td>
     *     <td>Configurable</td>
     *   </tr>
     * </table>
     *
     * <p><b>Usage Example:</b></p>
     * <pre>
     * {@code
     * @Service
     * public class TransactionConsumer {
     *     @KafkaListener(
     *         topics = "transaction-events",
     *         groupId = "wallet-transaction-group",
     *         containerFactory = "kafkaListenerContainerFactory"
     *     )
     *     public void consume(String message) {
     *         // This method will be invoked by 3 concurrent consumer threads
     *         // Processing messages from different partitions in parallel
     *     }
     * }
     * }
     * </pre>
     *
     * <p><b>Scaling Guidelines:</b></p>
     * <ul>
     *   <li><b>Horizontal Scaling:</b> Add more application instances (each with concurrency=3)</li>
     *   <li><b>Vertical Scaling:</b> Increase concurrency value (up to partition count)</li>
     *   <li><b>Partition Strategy:</b> Create topics with partitions ≥ (instances × concurrency)</li>
     *   <li><b>Resource Consideration:</b> Each concurrent consumer uses memory and CPU</li>
     * </ul>
     *
     * <p><b>Production Recommendations:</b></p>
     * <ul>
     *   <li>Set concurrency equal to the number of partitions for optimal load distribution</li>
     *   <li>Monitor consumer lag and adjust concurrency if consumers fall behind</li>
     *   <li>Use MANUAL_IMMEDIATE ack mode for exactly-once processing requirements</li>
     *   <li>Implement error handling and dead letter queue for failed messages</li>
     *   <li>Consider batch listener for bulk processing scenarios</li>
     * </ul>
     *
     * @return ConcurrentKafkaListenerContainerFactory configured for concurrent message processing
     * @see ConcurrentKafkaListenerContainerFactory
     * @see ContainerProperties.AckMode
     * @see org.springframework.kafka.annotation.KafkaListener
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Best Practice Configuration for Concurrency and Acknowledgment
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        return factory;
    }
}
