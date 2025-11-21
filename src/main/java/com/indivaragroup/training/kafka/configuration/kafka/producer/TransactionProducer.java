package com.indivaragroup.training.kafka.configuration.kafka.producer;

import com.indivaragroup.training.kafka.dto.entity.TransactionEntityDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

/**
 * Producer service for sending transaction events to Kafka.
 * <p>
 * This service is responsible for serializing {@link TransactionEntityDTO} to JSON format
 * and publishing it to the transaction-events Kafka topic. It enables asynchronous,
 * event-driven communication between the transaction service and wallet service.
 * </p>
 *
 * <p><b>Event Publishing Flow:</b></p>
 * <pre>
 * TransactionService → TransactionProducer → Kafka Topic → TransactionConsumer → Wallet Update
 * </pre>
 *
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Asynchronous message sending using CompletableFuture</li>
 *   <li>JSON serialization with Jackson ObjectMapper</li>
 *   <li>Transaction ID as message key for partition ordering</li>
 *   <li>Comprehensive logging for monitoring and debugging</li>
 *   <li>Error handling with runtime exception propagation</li>
 * </ul>
 *
 * <p><b>Message Structure:</b></p>
 * <ul>
 *   <li><b>Key:</b> Transaction ID (UUID) - ensures messages for same transaction go to same partition</li>
 *   <li><b>Value:</b> Complete TransactionEntityDTO serialized as JSON string</li>
 *   <li><b>Topic:</b> transaction-events (configurable via application properties)</li>
 * </ul>
 *
 * @author Kafka Training Team
 * @version 1.0.0
 * @since 2025-11-22
 * @see TransactionEntityDTO
 * @see com.indivaragroup.training.kafka.configuration.kafka.consumer.TransactionConsumer
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionProducer {

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
     * Name of the Kafka topic for transaction events.
     * Injected from application properties with key: {@code kafka.topic.transaction-events}
     */
    @Value("${kafka.topic.transaction-events}")
    private String transactionTopic;

    /**
     * Sends a transaction event to the Kafka topic asynchronously.
     * <p>
     * This method performs the following operations:
     * </p>
     * <ol>
     *   <li>Serializes the {@link TransactionEntityDTO} to a JSON string</li>
     *   <li>Sends the message to Kafka with transaction ID as the key</li>
     *   <li>Registers a callback to log success or failure</li>
     *   <li>Throws RuntimeException if serialization or sending fails</li>
     * </ol>
     *
     * <p><b>Message Key Strategy:</b></p>
     * <p>
     * Uses transaction ID as the message key to ensure:
     * </p>
     * <ul>
     *   <li>All events for the same transaction go to the same partition</li>
     *   <li>Maintains order of events for a specific transaction</li>
     *   <li>Enables parallel processing of different transactions</li>
     * </ul>
     *
     * <p><b>Asynchronous Behavior:</b></p>
     * <p>
     * This method returns immediately after queuing the message. The actual send operation
     * happens asynchronously. Success or failure is reported via the CompletableFuture callback.
     * </p>
     *
     * <p><b>Error Handling:</b></p>
     * <ul>
     *   <li><b>Serialization Error:</b> Throws RuntimeException immediately</li>
     *   <li><b>Send Failure:</b> Logged via callback, may retry based on producer configuration</li>
     *   <li><b>Callback Exception:</b> Does not affect application flow</li>
     * </ul>
     *
     * <p><b>Usage Example:</b></p>
     * <pre>
     * {@code
     * TransactionEntityDTO transaction = TransactionEntityDTO.builder()
     *     .transactionEntityDTOId(UUID.randomUUID())
     *     .transactionEntityDTOType("DEBIT")
     *     .transactionEntityDTOAmount(100000L)
     *     .transactionEntityDTOStatus("PENDING")
     *     .build();
     *
     * transactionProducer.sendTransactionEvent(transaction);
     * // Returns immediately, actual send happens asynchronously
     * }
     * </pre>
     *
     * <p><b>Monitoring:</b></p>
     * <ul>
     *   <li>Success: Logs topic name and message offset</li>
     *   <li>Failure: Logs error details with exception stack trace</li>
     *   <li>Monitor Kafka producer metrics for throughput and errors</li>
     * </ul>
     *
     * @param transactionEntityDTO the transaction data to be sent as an event
     * @throws RuntimeException if JSON serialization fails or message cannot be queued
     * @see TransactionEntityDTO
     * @see KafkaTemplate#send(String, Object, Object)
     */
    public void sendTransactionEvent(TransactionEntityDTO transactionEntityDTO) {
        try {
            // Serialize domain object to JSON string
            String jsonMessage = objectMapper.writeValueAsString(transactionEntityDTO);

            log.info("Sending transaction event to Kafka topic: {}", transactionTopic);
            log.info("Event data: {}", transactionEntityDTO);

            // Send message asynchronously with transaction ID as key
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(
                            transactionTopic,
                            transactionEntityDTO.getTransactionEntityDTOId().toString(),
                            jsonMessage
                    );

            // Register callback for send result
            future.whenComplete((result, throwable) -> {
                if (throwable == null) {
                    log.info("Transaction event sent successfully to topic: {} with offset: {}",
                            transactionTopic,
                            result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to send transaction event to topic: {}", transactionTopic, throwable);
                }
            });

        } catch (Exception exception) {
            log.error("Error serializing transaction event to JSON", exception);
            throw new RuntimeException("Failed to send transaction event", exception);
        }
    }
}
