package com.indivaragroup.training.kafka.dto.entity;

import com.indivaragroup.training.kafka.utility.ColumnNameUtility;
import com.indivaragroup.training.kafka.utility.TableNameUtility;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Table(name = TableNameUtility.TABLE_TRANSACTION)
public class TransactionEntityDTO {
    @Id
    @Column(name = ColumnNameUtility.COLUMN_TRANSACTION_ID)
    private UUID transactionEntityDTOId;

    @Column(name = ColumnNameUtility.COLUMN_TRANSACTION_ID_WALLET)
    private UUID transactionEntityDTOIdWallet;

    @Column(name = ColumnNameUtility.COLUMN_TRANSACTION_TYPE)
    private String transactionEntityDTOType;

    @Column(name = ColumnNameUtility.COLUMN_TRANSACTION_AMOUNT)
    private BigDecimal transactionEntityDTOAmount;

    @Column(name = ColumnNameUtility.COLUMN_TRANSACTION_STATUS)
    private String transactionEntityDTOStatus;

    @Column(name = ColumnNameUtility.COLUMN_TRANSACTION_CREATED_AT)
    private LocalDateTime transactionEntityDTOCreatedAt;
}
