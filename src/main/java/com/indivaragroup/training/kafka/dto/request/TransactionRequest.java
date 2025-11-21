package com.indivaragroup.training.kafka.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TransactionRequest {

    @NotBlank(message = "Transaction type required!!!")
    @JsonProperty("transactionType")
    private String transactionEntityDTOType;

    @NotBlank(message = "Transaction amount required!!!")
    @Positive(message = "Transaction amount must be greater than 0!!!")
    @JsonProperty("transactionAmount")
    private BigDecimal transactionEntityDTOAmount;

}
