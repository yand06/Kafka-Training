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
@Table(name = "m_wallet")
public class WalletEntityDTO {
    @Id
    @Column(name = "wallet_id")
    private UUID walletEntityDTOId;

    @Column(name = "id_user")
    private UUID walletEntityDTOIdUser;

    @Column(name = "wallet_balance")
    private String walletEntityDTOBalance;

    @Column(name = "wallet_currency")
    private String walletEntityDTOCurrency;

    @Column(name = "wallet_updated_at")
    private LocalDateTime walletEntityDTOUpdatedAt;
}
