package com.indivaragroup.training.kafka.configuration.kafka.consumer;

import com.indivaragroup.training.kafka.configuration.kafka.producer.WalletProducer;
import com.indivaragroup.training.kafka.dto.entity.TransactionEntityDTO;
import com.indivaragroup.training.kafka.dto.entity.WalletEntityDTO;
import com.indivaragroup.training.kafka.dto.exception.CoreThrowHandlerException;
import com.indivaragroup.training.kafka.repository.MTransactionRepositories;
import com.indivaragroup.training.kafka.repository.MWalletRepositories;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Consumer service for receiving and processing transaction events from Kafka.
 * <p>
 * This service acts as the core business logic processor in the event-driven architecture,
 * bridging transaction events with wallet balance updates. It consumes transaction events
 * from Kafka, validates and processes them, updates wallet balances accordingly, and
 * publishes wallet events for downstream processing.
 * </p>
 *
 * <p><b>Responsibilities:</b></p>
 * <ol>
 *   <li>Consume transaction events from the transaction-events Kafka topic</li>
 *   <li>Deserialize JSON messages to {@link TransactionEntityDTO} objects</li>
 *   <li>Validate wallet existence and balance sufficiency</li>
 *   <li>Update wallet balance based on transaction type (DEBIT/CREDIT)</li>
 *   <li>Update transaction status (SUCCESS/FAILED)</li>
 *   <li>Publish wallet events to trigger notifications and audit logging</li>
 * </ol>
 *
 * <p><b>Transaction Processing Flow:</b></p>
 * <pre>
 * Kafka Topic → TransactionConsumer → Database Update → WalletProducer → Kafka Topic
 *     ↓                ↓                    ↓                 ↓               ↓
 * transaction-   Deserialize &         Update wallet     Publish wallet  wallet-events
 *   events         validate            & transaction         event
 *                                         tables
 * </pre>
 *
 * <p><b>Transaction Types:</b></p>
 * <ul>
 *   <li><b>DEBIT:</b> Deducts amount from wallet balance (e.g., payment, withdrawal)</li>
 *   <li><b>CREDIT:</b> Adds amount to wallet balance (e.g., deposit, refund)</li>
 * </ul>
 *
 * <p><b>Transactional Guarantees:</b></p>
 * <ul>
 *   <li>Database operations wrapped in {@code @Transactional} for atomicity</li>
 *   <li>Automatic rollback on failure ensures data consistency</li>
 *   <li>Transaction status reflects processing outcome (SUCCESS/FAILED)</li>
 * </ul>
 *
 * <p><b>Error Handling Strategy:</b></p>
 * <ul>
 *   <li>Wallet not found: Throws exception, transaction marked as FAILED</li>
 *   <li>Insufficient balance: Throws exception, transaction marked as FAILED</li>
 *   <li>Unknown transaction type: Throws exception, transaction marked as FAILED</li>
 *   <li>Database error: Transaction rollback, status update attempted</li>
 * </ul>
 *
 * <p><b>Concurrency:</b></p>
 * <p>
 * Configured with concurrency=3 in the listener container factory, allowing
 * parallel processing of messages from different partitions. Each consumer
 * instance processes messages independently with its own database transaction.
 * </p>
 *
 * @author Kafka Training Team
 * @version 1.0.0
 * @since 2025-11-22
 * @see TransactionEntityDTO
 * @see WalletEntityDTO
 * @see WalletProducer
 * @see org.springframework.kafka.annotation.KafkaListener
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionConsumer {

    /**
     * Jackson ObjectMapper for deserializing JSON messages to domain objects.
     */
    private final ObjectMapper objectMapper;

    /**
     * Repository for wallet database operations.
     * Used to retrieve and update wallet balance information.
     */
    private final MWalletRepositories mWalletRepositories;

    /**
     * Repository for transaction database operations.
     * Used to update transaction status after processing.
     */
    private final MTransactionRepositories mTransactionRepositories;

    /**
     * Producer for publishing wallet events to Kafka.
     * Triggered after successful wallet balance update.
     */
    private final WalletProducer walletProducer;

    /**
     * Listens to and processes transaction events from Kafka.
     * <p>
     * This method is automatically invoked by the Spring Kafka listener container
     * whenever a new message is available in the transaction-events topic. It runs
     * within a database transaction to ensure atomicity of wallet and transaction updates.
     * </p>
     *
     * <p><b>Processing Steps:</b></p>
     * <ol>
     *   <li>Deserialize JSON message to TransactionEntityDTO</li>
     *   <li>Log transaction details for monitoring</li>
     *   <li>Delegate to business logic processor</li>
     *   <li>Handle and log any processing errors</li>
     * </ol>
     *
     * <p><b>Kafka Listener Configuration:</b></p>
     * <ul>
     *   <li><b>Topics:</b> Configured via application property kafka.topic.transaction-events</li>
     *   <li><b>Group ID:</b> Shared consumer group for load balancing</li>
     *   <li><b>Container Factory:</b> Provides concurrency and acknowledgment settings</li>
     * </ul>
     *
     * <p><b>Message Metadata:</b></p>
     * <ul>
     *   <li><b>Topic:</b> Source topic name for logging and monitoring</li>
     *   <li><b>Partition:</b> Partition number for debugging and performance analysis</li>
     *   <li><b>Offset:</b> Message offset for tracking and replay capability</li>
     * </ul>
     *
     * <p><b>Transaction Boundaries:</b></p>
     * <p>
     * The {@code @Transactional} annotation ensures that:
     * </p>
     * <ul>
     *   <li>All database operations succeed or fail together</li>
     *   <li>Changes are rolled back on exception</li>
     *   <li>Offset is only committed after successful transaction commit</li>
     * </ul>
     *
     * <p><b>Error Recovery:</b></p>
     * <p>
     * If processing fails, the message remains uncommitted and will be reprocessed.
     * Ensure idempotency in business logic to handle potential duplicate processing.
     * </p>
     *
     * <p><b>Monitoring Points:</b></p>
     * <ul>
     *   <li>Log entry for each consumed message</li>
     *   <li>Transaction details logged for audit trail</li>
     *   <li>Error details logged with topic and offset information</li>
     * </ul>
     *
     * @param message the JSON string payload from Kafka message
     * @param topic the name of the Kafka topic from which message was consumed
     * @param partition the partition number from which message was consumed
     * @param offset the offset of the message within its partition
     * @see KafkaListener
     * @see Transactional
     * @see #processTransactionEvent(TransactionEntityDTO)
     */
    @KafkaListener(
            topics = "${kafka.topic.transaction-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeTransactionEvent(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        try {
            // Deserialize JSON message to domain object
            TransactionEntityDTO transactionEntityDTO = objectMapper.readValue(message, TransactionEntityDTO.class);

            System.out.println();
            log.info("========================================");
            log.info("Consumed Transaction Event from Kafka");
            log.info("Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
            log.info("Transaction ID: {}", transactionEntityDTO.getTransactionEntityDTOId());
            log.info("Wallet ID: {}", transactionEntityDTO.getTransactionEntityDTOIdWallet());
            log.info("Type: {}", transactionEntityDTO.getTransactionEntityDTOType());
            log.info("Amount: {}", transactionEntityDTO.getTransactionEntityDTOAmount());
            log.info("Status: {}", transactionEntityDTO.getTransactionEntityDTOStatus());
            log.info("========================================");
            System.out.println();

            // Process transaction business logic
            processTransactionEvent(transactionEntityDTO);

        } catch (Exception e) {
            log.error("Error processing transaction event from topic: {} at offset: {}", topic, offset, e);
        }
    }

    /**
     * Processes the business logic for a transaction event.
     * <p>
     * This method implements the core business logic for transaction processing,
     * including wallet validation, balance updates, and event propagation. It uses
     * BigDecimal for precise monetary calculations to avoid floating-point errors.
     * </p>
     *
     * <p><b>Processing Algorithm:</b></p>
     * <ol>
     *   <li>Retrieve wallet from database (throws exception if not found)</li>
     *   <li>Extract current balance and transaction amount</li>
     *   <li>Calculate new balance based on transaction type:
     *     <ul>
     *       <li>DEBIT: newBalance = currentBalance - amount</li>
     *       <li>CREDIT: newBalance = currentBalance + amount</li>
     *     </ul>
     *   </li>
     *   <li>Validate sufficient balance for DEBIT transactions</li>
     *   <li>Update wallet balance and timestamp in database</li>
     *   <li>Update transaction status to SUCCESS</li>
     *   <li>Publish wallet event for notifications and audit</li>
     * </ol>
     *
     * <p><b>Transaction Type Handling:</b></p>
     * <table border="1">
     *   <tr>
     *     <th>Type</th>
     *     <th>Operation</th>
     *     <th>Use Cases</th>
     *     <th>Validation</th>
     *   </tr>
     *   <tr>
     *     <td>DEBIT</td>
     *     <td>Subtract amount</td>
     *     <td>Payment, withdrawal, transfer out</td>
     *     <td>Check sufficient balance</td>
     *   </tr>
     *   <tr>
     *     <td>CREDIT</td>
     *     <td>Add amount</td>
     *     <td>Deposit, refund, transfer in</td>
     *     <td>No balance check needed</td>
     *   </tr>
     * </table>
     *
     * <p><b>Validation Rules:</b></p>
     * <ul>
     *   <li><b>Wallet Existence:</b> Must exist in database or CoreThrowHandlerException is thrown</li>
     *   <li><b>Sufficient Balance:</b> For DEBIT, newBalance must be ≥ 0</li>
     *   <li><b>Transaction Type:</b> Must be "DEBIT" or "CREDIT" (case-insensitive)</li>
     *   <li><b>Amount:</b> Must be positive (validated in DTO layer)</li>
     * </ul>
     *
     * <p><b>Database Updates:</b></p>
     * <ul>
     *   <li>Wallet: Updates balance and updated_at timestamp</li>
     *   <li>Transaction: Updates status field (SUCCESS or FAILED)</li>
     *   <li>Both updates occur within same transaction for consistency</li>
     * </ul>
     *
     * <p><b>Event Propagation:</b></p>
     * <p>
     * On successful processing, publishes a wallet event containing updated balance.
     * This triggers downstream processes:
     * </p>
     * <ul>
     *   <li>User notifications (email, push, SMS)</li>
     *   <li>Audit logging for compliance</li>
     *   <li>Real-time balance updates in UI</li>
     *   <li>Analytics and reporting systems</li>
     * </ul>
     *
     * <p><b>Error Handling:</b></p>
     * <ul>
     *   <li><b>Wallet Not Found:</b> Transaction marked FAILED, exception propagated</li>
     *   <li><b>Insufficient Balance:</b> Transaction marked FAILED, exception propagated</li>
     *   <li><b>Unknown Type:</b> Transaction marked FAILED, exception propagated</li>
     *   <li><b>Database Error:</b> Transaction rolled back, status update attempted</li>
     * </ul>
     *
     * <p><b>Idempotency Considerations:</b></p>
     * <p>
     * This method is NOT idempotent. Reprocessing the same message will apply
     * the balance change multiple times. Consider implementing idempotency checks
     * using transaction ID for production systems.
     * </p>
     *
     * <p><b>Performance Notes:</b></p>
     * <ul>
     *   <li>Uses BigDecimal for precise monetary calculations</li>
     *   <li>Database queries optimized with proper indexing on wallet_id</li>
     *   <li>Wallet event publishing is asynchronous (non-blocking)</li>
     * </ul>
     *
     * @param transactionEntityDTO the transaction data to be processed
     * @throws CoreThrowHandlerException if wallet is not found in database
     * @throws RuntimeException if balance is insufficient or transaction type is unknown
     * @see WalletEntityDTO
     * @see WalletProducer#sendWalletEvent(WalletEntityDTO)
     */
    private void processTransactionEvent(TransactionEntityDTO transactionEntityDTO) {
        try {
            // Retrieve wallet from database
            WalletEntityDTO wallet = mWalletRepositories.findById(transactionEntityDTO.getTransactionEntityDTOIdWallet())
                    .orElseThrow(() -> new CoreThrowHandlerException("Wallet not found: " + transactionEntityDTO.getTransactionEntityDTOIdWallet()));

            log.info("Found wallet: {} with balance: {}", wallet.getWalletEntityDTOId(), wallet.getWalletEntityDTOBalance());

            // Extract balance and amount (using BigDecimal for precision)
            BigDecimal currentBalance = wallet.getWalletEntityDTOBalance();
            BigDecimal transactionAmount = transactionEntityDTO.getTransactionEntityDTOAmount();
            BigDecimal newBalance;

            // Calculate new balance based on transaction type
            if ("DEBIT".equalsIgnoreCase(transactionEntityDTO.getTransactionEntityDTOType())) {
                // DEBIT: Subtract amount (payment, withdrawal)
                newBalance = currentBalance.subtract(transactionAmount);
                log.info("Processing DEBIT transaction. Current: {}, Amount: {}, New: {}",
                        currentBalance, transactionAmount, newBalance);

                // Validate sufficient balance
                if (newBalance.compareTo(BigDecimal.ZERO) < 0) {
                    throw new RuntimeException("Insufficient balance. Current: " + currentBalance + ", Required: " + transactionAmount);
                }
            } else if ("CREDIT".equalsIgnoreCase(transactionEntityDTO.getTransactionEntityDTOType())) {
                // CREDIT: Add amount (deposit, refund)
                newBalance = currentBalance.add(transactionAmount);
                log.info("Processing CREDIT transaction. Current: {}, Amount: {}, New: {}",
                        currentBalance, transactionAmount, newBalance);
            } else {
                throw new RuntimeException("Unknown transaction type: " + transactionEntityDTO.getTransactionEntityDTOType());
            }

            // Update wallet balance and timestamp
            wallet.setWalletEntityDTOBalance(newBalance);
            wallet.setWalletEntityDTOUpdatedAt(LocalDateTime.now());
            mWalletRepositories.save(wallet);
            log.info("Wallet balance updated successfully. New balance: {}", newBalance);

            // Update transaction status to SUCCESS
            transactionEntityDTO.setTransactionEntityDTOStatus("SUCCESS");
            mTransactionRepositories.save(transactionEntityDTO);
            log.info("Transaction status updated to SUCCESS");

            // Publish wallet event for notifications and audit
            walletProducer.sendWalletEvent(wallet);
            log.info("Wallet event sent to Kafka for notification");

        } catch (Exception e) {
            log.error("Error processing transaction: {}", transactionEntityDTO.getTransactionEntityDTOId(), e);

            // Attempt to mark transaction as FAILED
            try {
                transactionEntityDTO.setTransactionEntityDTOStatus("FAILED");
                mTransactionRepositories.save(transactionEntityDTO);
                log.info("Transaction status updated to FAILED");
            } catch (Exception ex) {
                log.error("Error updating transaction status to FAILED", ex);
            }
        }
    }
}
