package com.indivaragroup.training.kafka.repository;

import com.indivaragroup.training.kafka.dto.entity.TransactionEntityDTO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface MTransactionRepositories extends JpaRepository<TransactionEntityDTO, UUID> {
}
