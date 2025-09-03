package com.anz.fastpayment.inward.scheme.validation.repository;

import com.anz.fastpayment.inward.scheme.validation.entity.MessageUniqueId;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import com.google.cloud.spring.data.spanner.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository interface for MessageUniqueId entity operations
 * Provides data access methods for idempotency checks and caching support
 */
@Repository
public interface MessageUniqueIdRepository extends SpannerRepository<MessageUniqueId, Long> {
    
    /**
     * Check if MUID exists with active status
     * Used for idempotency validation
     */
    @Query("SELECT COUNT(*) > 0 FROM message_unique_ids WHERE muid = @muid AND is_active = true")
    boolean existsByMuidAndActive(@Param("muid") String muid);
    
    /**
     * Find MUID by muid with active status
     * Used for idempotency operations
     */
    @Query("SELECT * FROM message_unique_ids WHERE muid = @muid AND is_active = true")
    Optional<MessageUniqueId> findByMuidAndActive(@Param("muid") String muid);
    
    /**
     * Find MUID by muid (basic lookup)
     * Used for general operations
     */
    @Query("SELECT * FROM message_unique_ids WHERE muid = @muid")
    Optional<MessageUniqueId> findByMuid(@Param("muid") String muid);
    
    /**
     * Check if MUID exists in specific topic and partition with active status
     * Used for enhanced idempotency validation
     */
    @Query("SELECT COUNT(*) > 0 FROM message_unique_ids WHERE muid = @muid AND message_topic = @topic AND message_partition = @partition AND is_active = true")
    boolean existsByMuidAndTopicAndPartition(@Param("muid") String muid, @Param("topic") String topic, @Param("partition") Integer partition);
    
    /**
     * Find MUID by muid, topic, and partition with active status
     * Used for enhanced idempotency operations
     */
    @Query("SELECT * FROM message_unique_ids WHERE muid = @muid AND message_topic = @topic AND message_partition = @partition AND is_active = true")
    Optional<MessageUniqueId> findByMuidAndTopicAndPartition(@Param("muid") String muid, @Param("topic") String topic, @Param("partition") Integer partition);
}
