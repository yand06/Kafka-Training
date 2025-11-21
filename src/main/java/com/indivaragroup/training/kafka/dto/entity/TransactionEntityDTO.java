package com.indivaragroup.training.kafka.dto.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Table(name = "m_transaction")
public class TransactionEntityDTO {
    @Id
    @Column(name = "transaction_id")
    private UUID transactionEntityDTOId;

    @Column(name = "id_wallet")
    private UUID transactionEntityDTOIdWallet;

    @Column(name = "transaction_type")
    private String transactionEntityDTOType;

    @Column(name = "transaction_status")
    private String transactionEntityDTOStatus;

    @Column(name = "transaction_created_at")
    private LocalDateTime transactionEntityDTOCreatedAt;
}
