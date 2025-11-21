package com.indivaragroup.training.kafka.dto.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
public class TransactionResponse {

    @JsonProperty("transactionId")
    private UUID transactionEntityDTOId;

    @JsonProperty("walletId")
    private UUID transactionEntityDTOIdWallet;

    @JsonProperty("transactionType")
    private String transactionEntityDTOType;

    @JsonProperty("transactionStatus")
    private String transactionEntityDTOStatus;

    @JsonProperty("transactionCreatedAt")
    private LocalDateTime transactionEntityDTOCreatedAt;

}
