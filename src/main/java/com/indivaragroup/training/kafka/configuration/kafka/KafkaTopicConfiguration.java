package com.indivaragroup.training.kafka.configuration.kafka;

import com.indivaragroup.training.kafka.configuration.kafka.consumer.TransactionConsumer;
import com.indivaragroup.training.kafka.configuration.kafka.consumer.WalletConsumer;
import com.indivaragroup.training.kafka.configuration.kafka.producer.TransactionProducer;
import com.indivaragroup.training.kafka.configuration.kafka.producer.WalletProducer;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Configuration class for defining and creating Kafka topics used in the application.
 * <p>
 * This class is responsible for automatically creating Kafka topic beans during application startup.
 * Each topic is configured with a specified number of partitions and replicas to support
 * scalability and fault tolerance in the event-driven architecture.
 * </p>
 *
 * <p>
 * The topics defined here follow a clear naming convention:
 * <ul>
 *   <li><b>Event topics</b>: Used for publishing domain events (e.g., transaction-events, wallet-events)</li>
 *   <li><b>Command topics</b>: Used for requesting actions (e.g., create-transaction, create-wallet)</li>
 * </ul>
 * </p>
 *
 * <p><b>Partition Strategy:</b></p>
 * <ul>
 *   <li>Each topic is configured with 3 partitions to enable parallel processing</li>
 *   <li>Multiple consumers can process messages concurrently from different partitions</li>
 *   <li>Messages with the same key (e.g., transaction ID) will always go to the same partition</li>
 * </ul>
 *
 * <p><b>Replication Factor:</b></p>
 * <ul>
 *   <li>Configured with 1 replica (suitable for development/testing)</li>
 *   <li>For production environments, increase replicas to 2 or 3 for fault tolerance</li>
 * </ul>
 *
 * @author Supriyandi La Awe
 * @version 1.0.0
 * @since 2025-11-22
 * @see TransactionProducer
 * @see TransactionConsumer
 * @see WalletProducer
 * @see WalletConsumer
 */
@Configuration
public class KafkaTopicConfiguration {

    /**
     * Name of the Kafka topic for transaction events.
     * <p>
     * This topic name is retrieved from the application configuration file (application.yml)
     * using the key: {@code kafka.topic.transaction-events}
     * </p>
     *
     * <p><b>Usage:</b> Used by {@link TransactionProducer} to publish transaction events
     * and by {@link TransactionConsumer} to consume and process these events.</p>
     */
    @Value("${kafka.topic.transaction-events}")
    private String transactionEventsTopic;

    /**
     * Name of the Kafka topic for wallet events.
     * <p>
     * This topic name is retrieved from the application configuration file (application.yml)
     * using the key: {@code kafka.topic.wallet-events}
     * </p>
     *
     * <p><b>Usage:</b> Used by {@link WalletProducer} to publish wallet balance change events
     * and by {@link WalletConsumer} to consume and process these events.</p>
     */
    @Value("${kafka.topic.wallet-events}")
    private String walletEventsTopic;

    /**
     * Name of the Kafka topic for transaction creation commands.
     * <p>
     * This topic name is retrieved from the application configuration file (application.yml)
     * using the key: {@code kafka.topic.create-transaction}
     * </p>
     *
     * <p><b>Usage:</b> Used as a command channel for requesting transaction creation
     * from external services or client applications.</p>
     */
    @Value("${kafka.topic.create-transaction}")
    private String createTransactionTopic;

    /**
     * Name of the Kafka topic for wallet creation commands.
     * <p>
     * This topic name is retrieved from the application configuration file (application.yml)
     * using the key: {@code kafka.topic.create-wallet}
     * </p>
     *
     * <p><b>Usage:</b> Used as a command channel for requesting wallet creation
     * from external services or client applications.</p>
     */
    @Value("${kafka.topic.create-wallet}")
    private String createWalletTopic;

    /**
     * Creates a Kafka topic bean for transaction events.
     * <p>
     * This topic is used by producers to send transaction events and by consumers to
     * receive and process events related to transaction creation or updates. The topic
     * enables asynchronous communication between transaction service and wallet service.
     * </p>
     *
     * <p><b>Topic Configuration:</b></p>
     * <ul>
     *   <li><b>Partitions:</b> 3 - Enables parallel processing by multiple consumers</li>
     *   <li><b>Replicas:</b> 1 - Single replica for development (increase for production)</li>
     * </ul>
     *
     * <p><b>Message Flow:</b></p>
     * <pre>
     * TransactionService → TransactionProducer → transaction-events topic → TransactionConsumer
     * </pre>
     *
     * <p><b>Use Cases:</b></p>
     * <ul>
     *   <li>Publishing new transaction events after database persistence</li>
     *   <li>Triggering wallet balance updates based on transaction type (DEBIT/CREDIT)</li>
     *   <li>Enabling audit trail and transaction monitoring</li>
     * </ul>
     *
     * @return NewTopic bean configured for transaction-events with 3 partitions and 1 replica
     * @see TransactionProducer#sendTransactionEvent(com.indivaragroup.training.kafka.dto.entity.TransactionEntityDTO)
     * @see TransactionConsumer#consumeTransactionEvent(String, String, int, long)
     */
    @Bean
    public NewTopic transactionEventsTopic() {
        return TopicBuilder.name(transactionEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Creates a Kafka topic bean for wallet events.
     * <p>
     * This topic is used for publishing and consuming events related to wallet changes,
     * including balance updates, currency changes, and wallet status modifications.
     * It enables real-time notifications and synchronization across microservices.
     * </p>
     *
     * <p><b>Topic Configuration:</b></p>
     * <ul>
     *   <li><b>Partitions:</b> 3 - Allows concurrent processing of wallet events</li>
     *   <li><b>Replicas:</b> 1 - Single replica for development (increase for production)</li>
     * </ul>
     *
     * <p><b>Message Flow:</b></p>
     * <pre>
     * TransactionConsumer → WalletProducer → wallet-events topic → WalletConsumer
     * </pre>
     *
     * <p><b>Use Cases:</b></p>
     * <ul>
     *   <li>Publishing wallet balance changes after transaction processing</li>
     *   <li>Triggering user notifications about balance updates</li>
     *   <li>Synchronizing wallet data across multiple services</li>
     *   <li>Maintaining audit logs for compliance and tracking</li>
     * </ul>
     *
     * @return NewTopic bean configured for wallet-events with 3 partitions and 1 replica
     * @see WalletProducer#sendWalletEvent(com.indivaragroup.training.kafka.dto.entity.WalletEntityDTO)
     * @see WalletConsumer#consumeWalletEvent(String, String, int, long)
     */
    @Bean
    public NewTopic walletEventsTopic() {
        return TopicBuilder.name(walletEventsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Creates a Kafka topic bean for transaction creation commands.
     * <p>
     * This topic serves as a command channel for receiving transaction creation requests
     * from external services or client applications. It follows the Command Query Responsibility
     * Segregation (CQRS) pattern where commands are separated from events.
     * </p>
     *
     * <p><b>Topic Configuration:</b></p>
     * <ul>
     *   <li><b>Partitions:</b> 3 - Distributes command processing load</li>
     *   <li><b>Replicas:</b> 1 - Single replica for development (increase for production)</li>
     * </ul>
     *
     * <p><b>Command vs Event:</b></p>
     * <ul>
     *   <li><b>Command:</b> Request to perform an action (create-transaction)</li>
     *   <li><b>Event:</b> Notification that something happened (transaction-events)</li>
     * </ul>
     *
     * <p><b>Use Cases:</b></p>
     * <ul>
     *   <li>Receiving transaction creation requests from API gateway</li>
     *   <li>Integrating with external payment systems</li>
     *   <li>Supporting batch transaction processing</li>
     * </ul>
     *
     * @return NewTopic bean configured for create-transaction with 3 partitions and 1 replica
     */
    @Bean
    public NewTopic createTransactionTopic() {
        return TopicBuilder.name(createTransactionTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Creates a Kafka topic bean for wallet creation commands.
     * <p>
     * This topic serves as a command channel for receiving wallet creation requests
     * from external services or client applications. It enables asynchronous wallet
     * provisioning and supports integration with user registration flows.
     * </p>
     *
     * <p><b>Topic Configuration:</b></p>
     * <ul>
     *   <li><b>Partitions:</b> 3 - Enables parallel wallet creation processing</li>
     *   <li><b>Replicas:</b> 1 - Single replica for development (increase for production)</li>
     * </ul>
     *
     * <p><b>Use Cases:</b></p>
     * <ul>
     *   <li>Creating wallets during user registration</li>
     *   <li>Provisioning multi-currency wallets</li>
     *   <li>Integrating with identity management systems</li>
     *   <li>Supporting bulk wallet creation for onboarding</li>
     * </ul>
     *
     * @return NewTopic bean configured for create-wallet with 3 partitions and 1 replica
     */
    @Bean
    public NewTopic createWalletTopic() {
        return TopicBuilder.name(createWalletTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
