package com.anz.fastpayment.router.repository;

import com.anz.fastpayment.router.model.RouterEvent;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RouterEventRepository extends SpannerRepository<RouterEvent, String> {}
