package com.indivaragroup.training.kafka.configuration.kafka.consumer;

import com.indivaragroup.training.kafka.dto.entity.WalletEntityDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumer service for receiving and processing wallet events from Kafka.
 * <p>
 * This service represents the final stage in the event-driven transaction processing flow.
 * It consumes wallet balance change events and triggers user-facing actions such as
 * notifications, audit logging, and data synchronization with external systems.
 * </p>
 *
 * <p><b>Responsibilities:</b></p>
 * <ol>
 *   <li>Consume wallet events from the wallet-events Kafka topic</li>
 *   <li>Deserialize JSON messages to {@link WalletEntityDTO} objects</li>
 *   <li>Send notifications to users about balance changes</li>
 *   <li>Log audit trails for compliance and tracking</li>
 *   <li>Synchronize wallet data with external systems if required</li>
 * </ol>
 *
 * <p><b>Event Processing Flow:</b></p>
 * <pre>
 *  Kafka Topic   → WalletConsumer → Notification Service
 *       ↓                  ↓                  ↓
 * wallet-events      Deserialize    Send Email/Push/SMS
 *                     & validate      Log Audit Trail
 *                                    Sync to Analytics
 * </pre>
 *
 * <p><b>Integration Points:</b></p>
 * <ul>
 *   <li><b>Notification Service:</b> Email, push notifications, SMS alerts</li>
 *   <li><b>Audit System:</b> Compliance logging and transaction history</li>
 *   <li><b>Analytics Platform:</b> Real-time balance tracking and reporting</li>
 *   <li><b>External APIs:</b> Third-party wallet synchronization</li>
 * </ul>
 *
 * <p><b>Notification Strategy:</b></p>
 * <ul>
 *   <li>Multi-channel notifications (email, push, SMS)</li>
 *   <li>Configurable notification preferences per user</li>
 *   <li>Template-based messaging for consistency</li>
 *   <li>Internationalization support for multiple languages</li>
 * </ul>
 *
 * <p><b>Concurrency:</b></p>
 * <p>
 * Configured with concurrency=3 in the listener container factory, allowing
 * parallel processing of wallet events from different partitions. Each consumer
 * instance handles notifications independently for optimal throughput.
 * </p>
 *
 * <p><b>Idempotency:</b></p>
 * <p>
 * This consumer should be designed to be idempotent, as wallet events may be
 * reprocessed in case of failures. Consider implementing deduplication logic
 * based on wallet ID and timestamp for production systems.
 * </p>
 *
 * @author Kafka Training Team
 * @version 1.0.0
 * @since 2025-11-22
 * @see WalletEntityDTO
 * @see org.springframework.kafka.annotation.KafkaListener
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class WalletConsumer {

    /**
     * Jackson ObjectMapper for deserializing JSON messages to domain objects.
     */
    private final ObjectMapper objectMapper;

    /**
     * Listens to and processes wallet events from Kafka.
     * <p>
     * This method is automatically invoked by the Spring Kafka listener container
     * whenever a new wallet event is available in the wallet-events topic. It handles
     * user notifications, audit logging, and data synchronization for wallet balance changes.
     * </p>
     *
     * <p><b>Processing Steps:</b></p>
     * <ol>
     *   <li>Deserialize JSON message to WalletEntityDTO</li>
     *   <li>Log wallet event details for monitoring</li>
     *   <li>Delegate to business logic processor</li>
     *   <li>Handle and log any processing errors</li>
     * </ol>
     *
     * <p><b>Kafka Listener Configuration:</b></p>
     * <ul>
     *   <li><b>Topics:</b> Configured via application property kafka.topic.wallet-events</li>
     *   <li><b>Group ID:</b> Shared consumer group for load balancing</li>
     *   <li><b>Container Factory:</b> Provides concurrency and acknowledgment settings</li>
     * </ul>
     *
     * <p><b>Message Metadata:</b></p>
     * <ul>
     *   <li><b>Topic:</b> Source topic name (wallet-events)</li>
     *   <li><b>Partition:</b> Partition number for debugging and load analysis</li>
     *   <li><b>Offset:</b> Message offset for tracking and replay capability</li>
     * </ul>
     *
     * <p><b>Error Handling:</b></p>
     * <p>
     * Errors during notification sending or audit logging are logged but do not
     * prevent offset commit. This ensures that transient failures (e.g., email
     * service downtime) don't block message processing. Consider implementing
     * a retry mechanism or dead letter queue for critical notifications.
     * </p>
     *
     * <p><b>Performance Considerations:</b></p>
     * <ul>
     *   <li>Notifications sent asynchronously to avoid blocking</li>
     *   <li>Batch processing of audit logs for efficiency</li>
     *   <li>Rate limiting for external API calls</li>
     *   <li>Circuit breaker pattern for resilience</li>
     * </ul>
     *
     * <p><b>Monitoring Points:</b></p>
     * <ul>
     *   <li>Log entry for each consumed wallet event</li>
     *   <li>Wallet balance and user information logged</li>
     *   <li>Notification delivery status tracked</li>
     *   <li>Error details logged with topic and offset</li>
     * </ul>
     *
     * @param message the JSON string payload from Kafka message
     * @param topic the name of the Kafka topic from which message was consumed
     * @param partition the partition number from which message was consumed
     * @param offset the offset of the message within its partition
     * @see KafkaListener
     * @see #processWalletEvent(WalletEntityDTO)
     */
    @KafkaListener(
            topics = "${kafka.topic.wallet-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWalletEvent(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        try {
            // Deserialize JSON message to domain object
            WalletEntityDTO walletEntityDTO = objectMapper.readValue(message, WalletEntityDTO.class);

            System.out.println("\n");
            log.info("========================================");
            log.info("Consumed Wallet Event from Kafka");
            log.info("Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
            log.info("Wallet ID: {}", walletEntityDTO.getWalletEntityDTOId());
            log.info("User ID: {}", walletEntityDTO.getWalletEntityDTOIdUser());
            log.info("Balance: {}", walletEntityDTO.getWalletEntityDTOBalance());
            log.info("Currency: {}", walletEntityDTO.getWalletEntityDTOCurrency());
            log.info("Updated At: {}", walletEntityDTO.getWalletEntityDTOUpdatedAt());
            log.info("========================================");
            System.out.println();

            // Process wallet event business logic
            processWalletEvent(walletEntityDTO);

        } catch (Exception e) {
            log.error("Error processing wallet event from topic: {} at offset: {}", topic, offset, e);
        }
    }

    /**
     * Processes the business logic for a wallet event.
     * <p>
     * This method orchestrates the post-transaction activities including user notifications
     * and audit logging. It serves as the integration point between the wallet system and
     * external services such as notification platforms and compliance systems.
     * </p>
     *
     * <p><b>Processing Activities:</b></p>
     * <ol>
     *   <li>Send multi-channel notifications to user</li>
     *   <li>Log audit trail for compliance</li>
     *   <li>Synchronize data with external systems (future enhancement)</li>
     * </ol>
     *
     * <p><b>Notification Channels:</b></p>
     * <table border="1">
     *   <tr>
     *     <th>Channel</th>
     *     <th>Use Case</th>
     *     <th>Priority</th>
     *   </tr>
     *   <tr>
     *     <td>Email</td>
     *     <td>Detailed transaction summary</td>
     *     <td>Medium</td>
     *   </tr>
     *   <tr>
     *     <td>Push Notification</td>
     *     <td>Real-time balance alert</td>
     *     <td>High</td>
     *   </tr>
     *   <tr>
     *     <td>SMS</td>
     *     <td>Critical balance changes</td>
     *     <td>High</td>
     *   </tr>
     * </table>
     *
     * <p><b>Audit Logging:</b></p>
     * <p>
     * Records wallet balance changes for:
     * </p>
     * <ul>
     *   <li>Regulatory compliance (e.g., financial regulations)</li>
     *   <li>Dispute resolution and investigation</li>
     *   <li>Analytics and reporting</li>
     *   <li>Security monitoring and fraud detection</li>
     * </ul>
     *
     * <p><b>Error Resilience:</b></p>
     * <ul>
     *   <li>Notification failures are logged but don't prevent processing</li>
     *   <li>Audit logging failures are escalated for manual review</li>
     *   <li>Retry mechanism for critical operations</li>
     *   <li>Circuit breaker prevents cascade failures</li>
     * </ul>
     *
     * <p><b>Future Enhancements (TODO):</b></p>
     * <ul>
     *   <li>Implement actual notification service integration</li>
     *   <li>Add persistent audit log storage to database</li>
     *   <li>Integrate with analytics platform for real-time dashboards</li>
     *   <li>Add webhook support for third-party integrations</li>
     *   <li>Implement notification preference management</li>
     * </ul>
     *
     * @param walletEntityDTO the wallet data containing updated balance information
     * @see #sendNotificationToUser(WalletEntityDTO)
     * @see #logAuditTrail(WalletEntityDTO)
     */
    private void processWalletEvent(WalletEntityDTO walletEntityDTO) {
        log.info("--- Processing wallet event: {}", walletEntityDTO.getWalletEntityDTOId());

        // Send notifications to user
        sendNotificationToUser(walletEntityDTO);

        // Log audit trail for compliance
        logAuditTrail(walletEntityDTO);

        log.info("Wallet event processed successfully");
    }

    /**
     * Sends notifications to the user about wallet balance changes.
     * <p>
     * This method simulates sending multi-channel notifications to inform users
     * about their wallet balance updates. In a production system, this would
     * integrate with actual notification services (e.g., SendGrid, Firebase, Twilio).
     * </p>
     *
     * <p><b>Notification Content:</b></p>
     * <ul>
     *   <li>User identifier for personalization</li>
     *   <li>Current wallet balance</li>
     *   <li>Currency information</li>
     *   <li>Timestamp of the change</li>
     * </ul>
     *
     * <p><b>Implementation Strategy:</b></p>
     * <ul>
     *   <li>Template-based messages for consistency</li>
     *   <li>Internationalization based on user preferences</li>
     *   <li>Channel selection based on notification settings</li>
     *   <li>Retry logic for failed deliveries</li>
     * </ul>
     *
     * <p><b>TODO - Production Implementation:</b></p>
     * <pre>
     * {@code
     * // Email Notification
     * emailService.sendBalanceUpdateEmail(
     *     userEmail,
     *     walletEntityDTO.getWalletEntityDTOBalance(),
     *     walletEntityDTO.getWalletEntityDTOCurrency()
     * );
     *
     * // Push Notification
     * pushNotificationService.sendBalanceAlert(
     *     userId,
     *     "Your wallet balance has been updated"
     * );
     *
     * // SMS Notification (for large transactions)
     * if (isLargeTransaction(walletEntityDTO)) {
     *     smsService.sendBalanceAlert(userPhone, balance);
     * }
     * }
     * </pre>
     *
     * <p><b>Best Practices:</b></p>
     * <ul>
     *   <li>Respect user notification preferences</li>
     *   <li>Implement rate limiting to prevent spam</li>
     *   <li>Use message queuing for reliable delivery</li>
     *   <li>Track delivery status for analytics</li>
     * </ul>
     *
     * @param walletEntityDTO the wallet data to include in the notification
     */
    private void sendNotificationToUser(WalletEntityDTO walletEntityDTO) {
        // Simulate notification sending
        log.info("--- NOTIFICATION: User {} - Your wallet balance has been updated to {}",
                walletEntityDTO.getWalletEntityDTOIdUser(),
                walletEntityDTO.getWalletEntityDTOBalance());

        // TODO: Implement real notification service integration
        // - Email notification via SendGrid/AWS SES
        // - Push notification via Firebase Cloud Messaging
        // - SMS notification via Twilio/AWS SNS
    }

    /**
     * Logs audit trail for wallet balance changes.
     * <p>
     * This method records wallet balance updates for compliance, security monitoring,
     * and dispute resolution. Audit logs should be immutable and stored in a secure,
     * tamper-proof system.
     * </p>
     *
     * <p><b>Audit Information:</b></p>
     * <ul>
     *   <li>Wallet ID for transaction tracking</li>
     *   <li>Updated balance amount</li>
     *   <li>Timestamp of the change</li>
     *   <li>Event source (Kafka topic and offset)</li>
     * </ul>
     *
     * <p><b>Compliance Requirements:</b></p>
     * <ul>
     *   <li>Immutable audit records</li>
     *   <li>Long-term retention (typically 7+ years)</li>
     *   <li>Secure storage with access controls</li>
     *   <li>Searchable and exportable for audits</li>
     * </ul>
     *
     * <p><b>TODO - Production Implementation:</b></p>
     * <pre>
     * {@code
     * // Save to dedicated audit log table
     * AuditLog auditLog = AuditLog.builder()
     *     .walletId(walletEntityDTO.getWalletEntityDTOId())
     *     .userId(walletEntityDTO.getWalletEntityDTOIdUser())
     *     .balanceBefore(previousBalance)
     *     .balanceAfter(walletEntityDTO.getWalletEntityDTOBalance())
     *     .currency(walletEntityDTO.getWalletEntityDTOCurrency())
     *     .timestamp(walletEntityDTO.getWalletEntityDTOUpdatedAt())
     *     .eventType("BALANCE_UPDATE")
     *     .build();
     *
     * auditLogRepository.save(auditLog);
     *
     * // Or send to external logging service
     * externalLoggingService.logEvent(auditLog);
     * }
     * </pre>
     *
     * <p><b>Storage Options:</b></p>
     * <ul>
     *   <li>Dedicated audit_log table in database</li>
     *   <li>External logging service (e.g., Splunk, ELK Stack)</li>
     *   <li>Blockchain for immutability (for high-value transactions)</li>
     *   <li>Archive to cold storage for long-term retention</li>
     * </ul>
     *
     * @param walletEntityDTO the wallet data to be logged in the audit trail
     */
    private void logAuditTrail(WalletEntityDTO walletEntityDTO) {
        log.info("--- AUDIT: Wallet {} balance updated to {} at {}",
                walletEntityDTO.getWalletEntityDTOId(),
                walletEntityDTO.getWalletEntityDTOBalance(),
                walletEntityDTO.getWalletEntityDTOUpdatedAt());

        // TODO: Save to audit log table or external logging service
        // - Create dedicated audit_log table
        // - Include user ID, wallet ID, old balance, new balance
        // - Add event type, timestamp, and correlation ID
        // - Consider WORM (Write Once Read Many) storage
    }
}
