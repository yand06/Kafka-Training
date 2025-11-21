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
@Table(name = TableNameUtility.TABLE_WALLET)
public class WalletEntityDTO {
    @Id
    @Column(name = ColumnNameUtility.COLUMN_WALLET_ID)
    private UUID walletEntityDTOId;

    @Column(name = ColumnNameUtility.COLUMN_WALLET_ID_USER)
    private UUID walletEntityDTOIdUser;

    @Column(name = ColumnNameUtility.COLUMN_WALLET_BALANCE)
    private BigDecimal walletEntityDTOBalance;

    @Column(name = ColumnNameUtility.COLUMN_WALLET_CURRENCY)
    private String walletEntityDTOCurrency;

    @Column(name = ColumnNameUtility.COLUMN_WALLET_UPDATED_AT)
    private LocalDateTime walletEntityDTOUpdatedAt;
}
