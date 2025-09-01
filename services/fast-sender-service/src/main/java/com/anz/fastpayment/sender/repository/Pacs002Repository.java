package com.anz.fastpayment.sender.repository;

import com.anz.fastpayment.sender.model.Pacs002Entity;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface Pacs002Repository extends SpannerRepository<Pacs002Entity, String> {}
