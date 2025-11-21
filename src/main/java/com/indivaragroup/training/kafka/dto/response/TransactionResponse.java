package com.indivaragroup.training.kafka.dto.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
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

    @JsonProperty("transactionAmount")
    private BigDecimal transactionEntityDTOAmount;

    @JsonProperty("transactionStatus")
    private String transactionEntityDTOStatus;

    @JsonProperty("transactionCreatedAt")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime transactionEntityDTOCreatedAt;

}
