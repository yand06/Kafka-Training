package com.indivaragroup.training.kafka.service.interfacing.module;

import com.indivaragroup.training.kafka.dto.request.TransactionRequest;
import com.indivaragroup.training.kafka.dto.response.RestApiResponse;
import com.indivaragroup.training.kafka.dto.response.TransactionResponse;

import java.util.UUID;

public interface TransactionService {

    RestApiResponse<TransactionResponse> createTransaction(UUID idWallet, TransactionRequest transactionRequest);

}
