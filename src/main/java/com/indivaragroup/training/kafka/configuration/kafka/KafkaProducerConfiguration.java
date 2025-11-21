package com.indivaragroup.training.kafka.configuration.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Kafka Producer to send messages to Kafka brokers.
 * <p>
 * This class configures the Kafka producer with String serializers for both keys and values.
 * Domain objects are serialized to JSON strings using ObjectMapper in the producer service
 * before being sent to Kafka topics. This approach provides flexibility and avoids deprecated
 * Spring Kafka JSON serializers.
 * </p>
 *
 * <p><b>Configuration Highlights:</b></p>
 * <ul>
 *   <li><b>Serialization:</b> String-based serialization for keys and values</li>
 *   <li><b>Idempotence:</b> Enabled to prevent duplicate messages</li>
 *   <li><b>Reliability:</b> Configured with acknowledgment from all replicas</li>
 *   <li><b>Performance:</b> Optimized with compression and batching</li>
 * </ul>
 *
 * <p><b>Producer Guarantees:</b></p>
 * <ul>
 *   <li><b>At-least-once delivery:</b> Messages are guaranteed to be delivered at least once</li>
 *   <li><b>Ordering:</b> Messages with the same key maintain order within a partition</li>
 *   <li><b>Durability:</b> Messages are persisted to all in-sync replicas before acknowledgment</li>
 * </ul>
 *
 * <p><b>Best Practices Applied:</b></p>
 * <ul>
 *   <li>Idempotent producer to eliminate duplicates on retry</li>
 *   <li>LZ4 compression for efficient network usage</li>
 *   <li>Batching for improved throughput</li>
 *   <li>Configurable retry mechanism for transient failures</li>
 * </ul>
 *
 * @author Supriyandi La Awe
 * @version 1.0.0
 * @since 2025-11-22
 * @see KafkaTemplate
 * @see ProducerFactory
 */
@Configuration
public class KafkaProducerConfiguration {

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
     * Creates and configures a ProducerFactory bean for Kafka message production.
     * <p>
     * This factory is configured with String serializers for both keys and values,
     * requiring manual JSON serialization in the producer service layer. This approach
     * provides better control over serialization and avoids issues with deprecated
     * Spring Kafka JSON serializers.
     * </p>
     *
     * <p><b>Configuration Parameters:</b></p>
     * <ul>
     *   <li><b>BOOTSTRAP_SERVERS:</b> Kafka broker addresses for initial connection</li>
     *   <li><b>KEY_SERIALIZER:</b> StringSerializer for message keys (typically entity IDs)</li>
     *   <li><b>VALUE_SERIALIZER:</b> StringSerializer for message values (JSON strings)</li>
     * </ul>
     *
     * <p><b>Reliability Configuration:</b></p>
     * <ul>
     *   <li><b>ACKS_CONFIG = "all":</b> Wait for acknowledgment from all in-sync replicas.
     *       Provides the strongest durability guarantee but may increase latency.</li>
     *   <li><b>RETRIES_CONFIG = 10:</b> Retry failed sends up to 10 times.
     *       Helps handle transient network issues and broker unavailability.</li>
     *   <li><b>ENABLE_IDEMPOTENCE = true:</b> Ensures exactly-once semantics by preventing
     *       duplicate messages on retry. Requires acks=all and retries > 0.</li>
     *   <li><b>MAX_IN_FLIGHT_REQUESTS = 5:</b> Maximum number of unacknowledged requests.
     *       With idempotence enabled, can be up to 5 while maintaining order.</li>
     * </ul>
     *
     * <p><b>Performance Configuration:</b></p>
     * <ul>
     *   <li><b>DELIVERY_TIMEOUT = 120000ms (2 min):</b> Maximum time for message delivery
     *       including retries. Should be greater than request.timeout.ms + linger.ms.</li>
     *   <li><b>LINGER_MS = 10ms:</b> Wait up to 10ms before sending a batch.
     *       Increases throughput by allowing more messages to batch together.</li>
     *   <li><b>COMPRESSION_TYPE = "lz4":</b> Fast compression algorithm that reduces
     *       network bandwidth usage with minimal CPU overhead.</li>
     *   <li><b>BATCH_SIZE = 32768 bytes (32KB):</b> Maximum batch size.
     *       Larger batches improve throughput but may increase latency.</li>
     * </ul>
     *
     * <p><b>Trade-offs:</b></p>
     * <ul>
     *   <li><b>Throughput vs Latency:</b> Batching and linger increase throughput but add latency</li>
     *   <li><b>Reliability vs Performance:</b> acks=all provides durability but reduces throughput</li>
     *   <li><b>CPU vs Network:</b> Compression reduces network usage but increases CPU load</li>
     * </ul>
     *
     * <p><b>Production Recommendations:</b></p>
     * <ul>
     *   <li>Monitor producer metrics to tune batch size and linger time</li>
     *   <li>Adjust retry count based on your SLA requirements</li>
     *   <li>Consider increasing delivery timeout for high-latency networks</li>
     *   <li>Test compression types (lz4, snappy, gzip) to find optimal balance</li>
     * </ul>
     *
     * @return ProducerFactory configured with String serializers and production-ready settings
     * @see ProducerConfig
     * @see StringSerializer
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Best Practice Configuration for Reliability
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 10);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Best Practice Configuration for Performance
        configProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Creates a KafkaTemplate bean for sending messages to Kafka topics.
     * <p>
     * KafkaTemplate is a high-level abstraction that simplifies Kafka message production.
     * It provides synchronous and asynchronous methods for sending messages with support
     * for callbacks and error handling.
     * </p>
     *
     * <p><b>Usage Example:</b></p>
     * <pre>
     * {@code
     * @Service
     * @RequiredArgsConstructor
     * public class TransactionProducer {
     *     private final KafkaTemplate<String, String> kafkaTemplate;
     *     private final ObjectMapper objectMapper;
     *
     *     public void sendEvent(TransactionEntityDTO transaction) {
     *         String json = objectMapper.writeValueAsString(transaction);
     *         kafkaTemplate.send("transaction-events",
     *                           transaction.getId().toString(),
     *                           json);
     *     }
     * }
     * }
     * </pre>
     *
     * <p><b>Key Features:</b></p>
     * <ul>
     *   <li><b>Asynchronous Sending:</b> Returns CompletableFuture for non-blocking operations</li>
     *   <li><b>Transaction Support:</b> Can be used with Spring's @Transactional annotation</li>
     *   <li><b>Error Handling:</b> Provides callback mechanisms for success and failure scenarios</li>
     *   <li><b>Metrics:</b> Integrates with Spring Boot metrics and monitoring</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> KafkaTemplate is thread-safe and can be shared across
     * multiple producer services.</p>
     *
     * @return KafkaTemplate configured for String key and value types, ready for message production
     * @see KafkaTemplate
     * @see #producerFactory()
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
