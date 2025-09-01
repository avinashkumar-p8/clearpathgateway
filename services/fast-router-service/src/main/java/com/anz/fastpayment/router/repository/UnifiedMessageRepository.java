package com.anz.fastpayment.router.repository;

import com.anz.fastpayment.router.model.UnifiedMessage;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UnifiedMessageRepository extends SpannerRepository<UnifiedMessage, String> {}
