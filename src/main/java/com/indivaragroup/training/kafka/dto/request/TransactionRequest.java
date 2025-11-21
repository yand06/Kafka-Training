package com.indivaragroup.training.kafka.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class TransactionRequest {

    @NotBlank(message = "Transaction type required!!!")
    @JsonProperty("transactionType")
    private String transactionEntityDTOType;

}
