package com.anz.fastpayment.router.repository;

import com.anz.fastpayment.router.model.DedupeKey;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;

public interface DedupeKeyRepository extends SpannerRepository<DedupeKey, String> {}






