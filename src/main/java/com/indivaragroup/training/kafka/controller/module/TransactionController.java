package com.indivaragroup.training.kafka.controller.module;

import com.indivaragroup.training.kafka.dto.request.TransactionRequest;
import com.indivaragroup.training.kafka.dto.response.RestApiResponse;
import com.indivaragroup.training.kafka.dto.response.TransactionResponse;
import com.indivaragroup.training.kafka.service.interfacing.module.TransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/transaction")
public class TransactionController {

    private final TransactionService transactionService;

    @PostMapping("/create-transaction/{idWallet}")
    public ResponseEntity<RestApiResponse<TransactionResponse>> createTransaction(
            @PathVariable UUID idWallet,
            @Valid @RequestBody TransactionRequest transactionRequest
    ) {
        return ResponseEntity.ok(transactionService.createTransaction(idWallet, transactionRequest));
    }

}
