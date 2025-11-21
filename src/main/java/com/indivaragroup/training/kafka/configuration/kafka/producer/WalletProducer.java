package com.indivaragroup.training.kafka.configuration.kafka.producer;

import com.indivaragroup.training.kafka.dto.entity.WalletEntityDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

/**
 * Producer service for sending wallet events to Kafka.
 * <p>
 * This service is responsible for serializing {@link WalletEntityDTO} to JSON format
 * and publishing it to the wallet-events Kafka topic. It enables real-time notifications
 * and data synchronization across microservices when wallet balances change.
 * </p>
 *
 * <p><b>Event Publishing Flow:</b></p>
 * <pre>
 * TransactionConsumer → WalletProducer → Kafka Topic → WalletConsumer → Notification Service
 * </pre>
 *
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Asynchronous message sending for non-blocking operations</li>
 *   <li>JSON serialization with Jackson ObjectMapper</li>
 *   <li>Wallet ID as message key for partition ordering</li>
 *   <li>Callback-based success/failure handling</li>
 *   <li>Comprehensive logging for audit and debugging</li>
 * </ul>
 *
 * <p><b>Use Cases:</b></p>
 * <ul>
 *   <li>Publishing wallet balance updates after transaction processing</li>
 *   <li>Triggering user notifications about balance changes</li>
 *   <li>Enabling real-time wallet data synchronization</li>
 *   <li>Supporting audit trail and compliance logging</li>
 * </ul>
 *
 * @author Kafka Training Team
 * @version 1.0.0
 * @since 2025-11-22
 * @see WalletEntityDTO
 * @see com.indivaragroup.training.kafka.configuration.kafka.consumer.WalletConsumer
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class WalletProducer {

    /**
     * KafkaTemplate for sending messages to Kafka topics.
     * Configured with String serializers for both key and value.
     */
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Jackson ObjectMapper for serializing domain objects to JSON strings.
     * Handles all Java object to JSON conversions.
     */
    private final ObjectMapper objectMapper;

    /**
     * Name of the Kafka topic for wallet events.
     * Injected from application properties with key: {@code kafka.topic.wallet-events}
     */
    @Value("${kafka.topic.wallet-events}")
    private String walletTopic;

    /**
     * Sends a wallet event to the Kafka topic asynchronously.
     * <p>
     * This method serializes the wallet entity to JSON and publishes it to Kafka,
     * enabling downstream services to react to wallet balance changes. The wallet ID
     * is used as the message key to ensure ordering of events for the same wallet.
     * </p>
     *
     * <p><b>Operation Sequence:</b></p>
     * <ol>
     *   <li>Serialize {@link WalletEntityDTO} to JSON string</li>
     *   <li>Queue message to Kafka with wallet ID as key</li>
     *   <li>Return immediately (non-blocking)</li>
     *   <li>Log success or failure via callback</li>
     * </ol>
     *
     * <p><b>Message Partitioning:</b></p>
     * <p>
     * Using wallet ID as the key ensures:
     * </p>
     * <ul>
     *   <li>All events for the same wallet go to the same partition</li>
     *   <li>Events for a specific wallet are processed in order</li>
     *   <li>Different wallets can be processed in parallel</li>
     * </ul>
     *
     * <p><b>Error Scenarios:</b></p>
     * <ul>
     *   <li><b>JSON Serialization Failure:</b> Throws RuntimeException immediately</li>
     *   <li><b>Kafka Send Failure:</b> Logged via callback, retries handled by producer config</li>
     *   <li><b>Topic Not Found:</b> Logged as error, message is lost</li>
     * </ul>
     *
     * <p><b>Production Considerations:</b></p>
     * <ul>
     *   <li>Monitor send latency and failure rates</li>
     *   <li>Implement dead letter queue for failed messages</li>
     *   <li>Consider idempotent consumers to handle duplicates</li>
     *   <li>Set up alerts for persistent send failures</li>
     * </ul>
     *
     * @param walletEntityDTO the wallet data to be sent as an event
     * @throws RuntimeException if JSON serialization fails or message cannot be queued
     * @see WalletEntityDTO
     * @see KafkaTemplate#send(String, Object, Object)
     */
    public void sendWalletEvent(WalletEntityDTO walletEntityDTO) {
        try {
            // Serialize domain object to JSON string
            String jsonMessage = objectMapper.writeValueAsString(walletEntityDTO);

            log.info("Sending wallet event to Kafka topic: {}", walletTopic);
            log.info("Event data: {}", walletEntityDTO);

            // Send message asynchronously with wallet ID as key
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(
                            walletTopic,
                            walletEntityDTO.getWalletEntityDTOId().toString(),
                            jsonMessage
                    );

            // Register callback for send result
            future.whenComplete((result, throwable) -> {
                if (throwable == null) {
                    log.info("Wallet event sent successfully to topic: {} with offset: {}",
                            walletTopic,
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to send wallet event to topic: {}", walletTopic, throwable);
                }
            });

        } catch (Exception exception) {
            log.error("Error serializing wallet event to JSON", exception);
            throw new RuntimeException("Failed to send wallet event", exception);
        }
    }
}
