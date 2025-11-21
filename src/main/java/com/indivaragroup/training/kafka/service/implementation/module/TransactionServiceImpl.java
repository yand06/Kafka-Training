package com.indivaragroup.training.kafka.service.implementation.module;

import com.indivaragroup.training.kafka.dto.entity.TransactionEntityDTO;
import com.indivaragroup.training.kafka.dto.request.TransactionRequest;
import com.indivaragroup.training.kafka.dto.response.RestApiResponse;
import com.indivaragroup.training.kafka.dto.response.TransactionResponse;
import com.indivaragroup.training.kafka.repository.MTransactionRepositories;
import com.indivaragroup.training.kafka.service.interfacing.module.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionServiceImpl implements TransactionService {

    private final MTransactionRepositories mTransactionRepositories;

    @Override
    public RestApiResponse<TransactionResponse> createTransaction(UUID idWallet, TransactionRequest transactionRequest) {
        TransactionEntityDTO transactionEntityDTO = TransactionEntityDTO.builder()
                .transactionEntityDTOId(UUID.randomUUID())
                .transactionEntityDTOType(transactionRequest.getTransactionEntityDTOType())
                .transactionEntityDTOStatus("PENDING")
                .transactionEntityDTOCreatedAt(LocalDateTime.now())
                .transactionEntityDTOIdWallet(idWallet)
                .build();

        mTransactionRepositories.save(transactionEntityDTO);

        TransactionResponse transactionResponse = TransactionResponse.builder()
                .transactionEntityDTOId(transactionEntityDTO.getTransactionEntityDTOId())
                .transactionEntityDTOType(transactionEntityDTO.getTransactionEntityDTOType())
                .transactionEntityDTOStatus(transactionEntityDTO.getTransactionEntityDTOStatus())
                .transactionEntityDTOCreatedAt(transactionEntityDTO.getTransactionEntityDTOCreatedAt())
                .transactionEntityDTOIdWallet(transactionEntityDTO.getTransactionEntityDTOIdWallet())
                .build();

        return RestApiResponse.<TransactionResponse>builder()
                .restApiResponseCode(HttpStatus.OK.value())
                .restApiResponseMessage("Transaction Created")
                .restApiResponseResults(transactionResponse)
                .restApiResponseError(null)
                .build();
    }
}
