package com.indivaragroup.training.kafka.controller.module;

import com.indivaragroup.training.kafka.dto.request.TransactionRequest;
import com.indivaragroup.training.kafka.dto.response.RestApiResponse;
import com.indivaragroup.training.kafka.dto.response.TransactionResponse;
import com.indivaragroup.training.kafka.service.interfacing.module.TransactionService;
import com.indivaragroup.training.kafka.utility.RestApiPathUtility;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping(RestApiPathUtility.API_PATH + RestApiPathUtility.API_PATH_VERSION + RestApiPathUtility.API_PATH_TRANSACTION)
public class TransactionController {

    private final TransactionService transactionService;

    @PostMapping(RestApiPathUtility.API_PATH_TRANSACTION_CREATE + RestApiPathUtility.API_PATH_WALLET_ID)
    public ResponseEntity<RestApiResponse<TransactionResponse>> createTransaction(
            @PathVariable UUID idWallet,
            @Valid @RequestBody TransactionRequest transactionRequest
    ) {
        return ResponseEntity.ok(transactionService.createTransaction(idWallet, transactionRequest));
    }

}
