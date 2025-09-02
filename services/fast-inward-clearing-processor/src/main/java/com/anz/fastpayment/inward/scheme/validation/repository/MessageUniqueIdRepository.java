package com.anz.fastpayment.inward.scheme.validation.repository;

import com.anz.fastpayment.inward.scheme.validation.entity.MessageUniqueId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository interface for MessageUniqueId entity operations
 * Provides data access methods for idempotency checks and caching support
 */
@Repository
public interface MessageUniqueIdRepository extends JpaRepository<MessageUniqueId, Long> {
    
    /**
     * Check if MUID exists with active status
     * Used for idempotency validation
     */
    @Query("SELECT COUNT(m) > 0 FROM MessageUniqueId m WHERE m.muid = :muid AND m.isActive = true")
    boolean existsByMuidAndActive(@Param("muid") String muid);
    
    /**
     * Find MUID by muid with active status
     * Used for idempotency operations
     */
    @Query("SELECT m FROM MessageUniqueId m WHERE m.muid = :muid AND m.isActive = true")
    Optional<MessageUniqueId> findByMuidAndActive(@Param("muid") String muid);
    
    /**
     * Find MUID by muid (basic lookup)
     * Used for general operations
     */
    Optional<MessageUniqueId> findByMuid(String muid);
    
    /**
     * Check if MUID exists in specific topic and partition with active status
     * Used for enhanced idempotency validation
     */
    @Query("SELECT COUNT(m) > 0 FROM MessageUniqueId m WHERE m.muid = :muid AND m.topic = :topic AND m.partition = :partition AND m.isActive = true")
    boolean existsByMuidAndTopicAndPartition(@Param("muid") String muid, @Param("topic") String topic, @Param("partition") Integer partition);
    
    /**
     * Find MUID by muid, topic, and partition with active status
     * Used for enhanced idempotency operations
     */
    @Query("SELECT m FROM MessageUniqueId m WHERE m.muid = :muid AND m.topic = :topic AND m.partition = :partition AND m.isActive = true")
    Optional<MessageUniqueId> findByMuidAndTopicAndPartition(@Param("muid") String muid, @Param("topic") String topic, @Param("partition") Integer partition);
}
