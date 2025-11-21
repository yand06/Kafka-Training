package com.indivaragroup.training.kafka.repository;

import com.indivaragroup.training.kafka.dto.entity.WalletEntityDTO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface MWalletRepositories extends JpaRepository<WalletEntityDTO, UUID> {
}
