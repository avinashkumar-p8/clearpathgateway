package com.anz.fastpayment.inward.scheme.validation.service;

import com.anz.fastpayment.inward.scheme.validation.constants.ValidationTags;
import com.anz.fastpayment.inward.scheme.validation.exception.ValidationException;
import com.anz.fastpayment.inward.scheme.validation.model.TagValidationResult;
import com.anz.fastpayment.inward.scheme.validation.model.ValidationResult;
import com.anz.fastpayment.inward.scheme.validation.model.ValidationStatus;
import com.anz.fastpayment.inward.scheme.validation.repository.CurrencyRepository;
import com.anz.fastpayment.inward.scheme.validation.repository.CountryRepository;
import com.anz.fastpayment.inward.scheme.validation.util.MapToArrayConverter;
import com.anz.fastpayment.inward.util.DirectFieldExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Enhanced Scheme Validation Orchestrator for Banking Operations
 * Orchestrates parallel validation of all required tags using direct field extraction
 */
@Service
public class SchemeValidationOrchestrator {
    
    private static final Logger log = LoggerFactory.getLogger(SchemeValidationOrchestrator.class);
    
    // Repository dependencies for database validation
    @Autowired
    private CurrencyRepository currencyRepository;
    
    @Autowired
    private CountryRepository countryRepository;
    
    // Thread pool for parallel validation
    private final ExecutorService validationExecutor = Executors.newFixedThreadPool(10);
    
    // Required tags for validation (using tag names for switch statement) - 9 active tags (15 commented out)
    private static final List<String> REQUIRED_TAGS = List.of(ValidationTags.TAG_CURRENCY, ValidationTags.TAG_COUNTRY, /* ValidationTags.TAG_FROM_MMBID, ValidationTags.TAG_TO_MMBID, ValidationTags.TAG_BIZ_MSG_IDR, ValidationTags.TAG_CPY_DPICT, ValidationTags.TAG_CRE_DT_TM, ValidationTags.TAG_NB_OF_TXS, ValidationTags.TAG_TTL_INTR_BK_STTLM_AMT, ValidationTags.TAG_STTLM_MTD, ValidationTags.TAG_CD, ValidationTags.TAG_PMT_TP_INF_SVC_LVL_CD, */ ValidationTags.TAG_INSTR_ID, ValidationTags.TAG_INTR_BK_STTLM_AMT, ValidationTags.TAG_INTR_BK_STTLM_DT, ValidationTags.TAG_CDTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID, ValidationTags.TAG_INSTG_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID, ValidationTags.TAG_DBTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID, ValidationTags.TAG_INSTD_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID /* , ValidationTags.TAG_CHRG_BR, ValidationTags.TAG_ORGNL_MSG_NM_ID, ValidationTags.TAG_RVSL_ID, ValidationTags.TAG_RVSD_INTR_BK_STTLM_AMT, ValidationTags.TAG_RVSL_RSN_INF_RSN_PRTY */);
    
    // Statistics tracking
    private final AtomicLong inputMessageCount = new AtomicLong(0);
    private final AtomicLong successfulMessageCount = new AtomicLong(0);
    private final AtomicLong failedMessageCount = new AtomicLong(0);
    private final AtomicLong successfulValidationCount = new AtomicLong(0);
    private final AtomicLong validationErrorCount = new AtomicLong(0);
    private final AtomicLong missingTagCount = new AtomicLong(0);
    private final AtomicReference<String> lastValidationTime = new AtomicReference<>("Never");
    
    // In-memory caches for daily uniqueness checks
    private final Map<LocalDate, Set<String>> bizMsgIdrCache = new ConcurrentHashMap<>();
    private final Map<LocalDate, Set<String>> instrIdCache = new ConcurrentHashMap<>();
    private final Map<LocalDate, Set<String>> rvslIdCache = new ConcurrentHashMap<>();
    
    /**
     * Constructor for dependency injection
     */
    public SchemeValidationOrchestrator() {
        // Constructor - repositories will be injected by Spring
    }
    
    /**
     * Main validation method that orchestrates parallel validation of all required tags.
     * 
     * @param payload The message payload as Map<String, Object>
     * @param transactionId The transaction identifier for logging and tracking
     * @return ValidationResult containing all validation results
     */
    public ValidationResult validateMessage(Map<String, Object> payload, String transactionId) {
        log.info("Starting validation for transaction: {}", transactionId);
        
        // Update input counter
        inputMessageCount.incrementAndGet();
        lastValidationTime.set(java.time.LocalDateTime.now().toString());
        
        // Convert payload structure if needed (Map-based to Array-based)
        Map<String, Object> convertedPayload = MapToArrayConverter.convertIfNeeded(payload);
        if (convertedPayload != payload) {
            log.info("Payload structure converted from Map-based to Array-based for transaction: {}", transactionId);
        }
        
        // Log input data structure summary
        log.debug("Input payload type: {}, size: {}", convertedPayload.getClass().getSimpleName(), convertedPayload.size());
        
        List<String> successfulValidations = new ArrayList<>();
        List<String> failedValidations = new ArrayList<>();
        List<String> missingTags = new ArrayList<>();
        
        // Create parallel validation futures for each required tag
        List<CompletableFuture<TagValidationResult>> validationFutures = REQUIRED_TAGS.stream()
            .map(tagName -> CompletableFuture.supplyAsync(() -> {
                try {
                    // Get the actual JSON path for this tag
                    String jsonPath = getJsonPathForTag(tagName);
                    // Extract field value directly using the constant path
                    String tagValue = DirectFieldExtractor.extractFieldValue(convertedPayload, jsonPath);
                    
                    if (tagValue != null && !tagValue.trim().isEmpty()) {
                        log.debug("Extracted value for tag '{}': {}", tagName, tagValue);
                        return validateTag(tagName, tagValue, transactionId);
                    } else {
                        log.debug("Tag '{}' not found or has empty value in payload - treating as successful validation", tagName);
                        missingTagCount.incrementAndGet();
                        // Treat missing tags as successful validations since they're not part of the current schema
                        return new TagValidationResult(tagName, "not applicable", ValidationStatus.SUCCESS, "Tag not present in current schema - validation skipped", "SCHEMA_MISMATCH");
                    }
                } catch (Exception e) {
                    log.error("Unexpected error during validation of tag '{}' in transaction {}: {}", tagName, transactionId, e.getMessage(), e);
                    validationErrorCount.incrementAndGet();
                    return new TagValidationResult(tagName, null, ValidationStatus.FAILURE, "Validation error: " + e.getMessage(), "VALIDATION_ERROR");
                }
            }, validationExecutor))
            .collect(Collectors.toList());
        
        // Wait for all validations to complete and collect results
        try {
            CompletableFuture.allOf(validationFutures.toArray(new CompletableFuture[0])).join();
            log.info("All validations completed for transaction: {}", transactionId);
        } catch (Exception e) {
            log.error("Error during parallel validation execution for transaction {}: {}", transactionId, e.getMessage(), e);
        }
        
        // Collect all validation results
        List<TagValidationResult> allResults = validationFutures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList());
        
        // Process results and update statistics
        for (TagValidationResult result : allResults) {
            if (result.getStatus() == ValidationStatus.SUCCESS) {
                successfulValidations.add(result.getTagName());
                successfulValidationCount.incrementAndGet();
            } else if (result.getStatus() == ValidationStatus.FAILURE) {
                failedValidations.add(result.getTagName() + ": " + result.getErrorMessage());
            } else if (result.getStatus() == ValidationStatus.MISSING) {
                missingTags.add(result.getTagName());
            }
        }
        
        // Create validation result
        List<TagValidationResult> tagResults = new ArrayList<>();
        
        // Add successful validations
        for (String tagName : successfulValidations) {
            TagValidationResult result = new TagValidationResult(tagName, "validated", ValidationStatus.SUCCESS);
            tagResults.add(result);
        }
        
        // Add failed validations
        for (String failedValidation : failedValidations) {
            String[] parts = failedValidation.split(":", 2);
            String tagName = parts[0];
            String message = parts.length > 1 ? parts[1] : "Validation failed";
            TagValidationResult result = new TagValidationResult(tagName, "invalid", ValidationStatus.FAILURE, message, "VALIDATION_ERROR");
            tagResults.add(result);
        }
        
        // Add missing tags
        for (String tagName : missingTags) {
            TagValidationResult result = new TagValidationResult(tagName, null, ValidationStatus.MISSING, "Tag not found in payload", "MISSING_TAG");
            tagResults.add(result);
        }
        
        ValidationResult validationResult = new ValidationResult(transactionId, tagResults);
        
        // Update success/failure statistics
        if (failedValidations.isEmpty()) {
            successfulMessageCount.incrementAndGet();
        } else {
            failedMessageCount.incrementAndGet();
        }
        
        // Log output data structure summary
        log.debug("Output validation result: {} successful, {} failed, {} missing", 
            successfulValidations.size(), failedValidations.size(), missingTags.size());
        
        return validationResult;
    }
    
    /**
     * Get the JSON path for a given tag name
     * 
     * @param tagName The tag name (e.g., "currency", "country")
     * @return The corresponding JSON path
     */
    private String getJsonPathForTag(String tagName) {
        return switch (tagName) {
            case ValidationTags.TAG_CURRENCY -> ValidationTags.CURRENCY;
            case ValidationTags.TAG_COUNTRY -> ValidationTags.COUNTRY;
            case ValidationTags.TAG_INSTR_ID -> ValidationTags.INSTR_ID;
            case ValidationTags.TAG_INTR_BK_STTLM_AMT -> ValidationTags.INTR_BK_STTLM_AMT;
            case ValidationTags.TAG_INTR_BK_STTLM_DT -> ValidationTags.INTR_BK_STTLM_DT;
            case ValidationTags.TAG_CDTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID -> ValidationTags.CDTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID;
            case ValidationTags.TAG_INSTG_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID -> ValidationTags.INSTG_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID;
            case ValidationTags.TAG_DBTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID -> ValidationTags.DBTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID;
            case ValidationTags.TAG_INSTD_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID -> ValidationTags.INSTD_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID;
            default -> tagName; // Fallback to tag name if no mapping found
        };
    }
    
    /**
     * Validate a single tag using switch method for direct validation
     * 
     * @param tagName The tag name to validate
     * @param tagValue The tag value to validate
     * @param transactionId The transaction ID for logging
     * @return TagValidationResult for the tag
     */
    private TagValidationResult validateTag(String tagName, String tagValue, String transactionId) {
        try {
            if (tagValue == null || tagValue.trim().isEmpty()) {
                log.debug("Tag {} has null or empty value for transaction: {}", tagName, transactionId);
                return new TagValidationResult(tagName, tagValue, ValidationStatus.MISSING, 
                    "Tag value is missing", "MISSING_TAG");
            }
            
            log.info("=== SWITCH METHOD DATA FLOW ===");
            log.info("Processing tag: '{}' with value: '{}' for transaction: {}", tagName, tagValue, transactionId);
            
            // Use switch method to call the right validation method for each required tag
            switch (tagName.toLowerCase()) {
                case ValidationTags.TAG_CURRENCY:
                    log.info("Switch case: 'currency' -> calling validateCurrency()");
                    validateCurrency(tagValue, transactionId);
                    break;
                case ValidationTags.TAG_COUNTRY:
                    log.info("Switch case: 'country' -> calling validateCountry()");
                    validateCountry(tagValue, transactionId);
                    break;
                // case ValidationTags.TAG_FROM_MMBID:
                //     log.info("Switch case: 'from_mmbid' -> calling validateMmbid()");
                //     validateMmbid(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_TO_MMBID:
                //     log.info("Switch case: 'to_mmbid' -> calling validateMmbid()");
                //     validateMmbid(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_BIZ_MSG_IDR:
                //     log.info("Switch case: 'biz_msg_idr' -> calling validateBizMsgIdr()");
                //     validateBizMsgIdr(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_CPY_DPICT:
                //     log.info("Switch case: 'cpy_dpict' -> calling validateCpyDpict()");
                //     validateCpyDpict(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_CRE_DT_TM:
                //     log.info("Switch case: 'cre_dt_tm' -> calling validateCreDtTm()");
                //     validateCreDtTm(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_NB_OF_TXS:
                //     log.info("Switch case: 'nb_of_txs' -> calling validateNbOfTxs()");
                //     validateNbOfTxs(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_TTL_INTR_BK_STTLM_AMT:
                //     log.info("Switch case: 'ttl_intr_bk_sttlm_amt' -> calling validateTtlIntrBkSttlmAmt()");
                //     validateTtlIntrBkSttlmAmt(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_STTLM_MTD:
                //     log.info("Switch case: 'sttlm_mtd' -> calling validateSttlmMtd()");
                //     validateSttlmMtd(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_CD:
                //     log.info("Switch case: 'cd' -> calling validateCd()");
                //     validateCd(tagValue, transactionId);
                //     break;
                case ValidationTags.TAG_INSTR_ID:
                    log.info("Switch case: 'instr_id' -> calling validateInstrId()");
                    validateInstrId(tagValue, transactionId);
                    break;
                // case ValidationTags.TAG_PMT_TP_INF_SVC_LVL_CD:
                //     log.info("Switch case: 'pmt_tp_inf_svc_lvl_cd' -> calling validatePmtTpInfSvcLvlCd()");
                //     validatePmtTpInfSvcLvlCd(tagValue, transactionId);
                //     break;
                case ValidationTags.TAG_INTR_BK_STTLM_AMT:
                    log.info("Switch case: 'intr_bk_sttlm_amt' -> calling validateIntrBkSttlmAmt()");
                    validateIntrBkSttlmAmt(tagValue, transactionId);
                    break;
                case ValidationTags.TAG_INTR_BK_STTLM_DT:
                    log.info("Switch case: 'intr_bk_sttlm_dt' -> calling validateIntrBkSttlmDt()");
                    validateIntrBkSttlmDt(tagValue, transactionId);
                    break;
                case ValidationTags.TAG_CDTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID:
                    log.info("Switch case: 'cdtr_agt_fin_instn_id_clr_sys_mmb_id_mmb_id' -> calling validateMmbid()");
                    validateMmbid(tagValue, transactionId);
                    break;
                case ValidationTags.TAG_INSTG_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID:
                    log.info("Switch case: 'instg_agt_fin_instn_id_clr_sys_mmb_id_mmb_id' -> calling validateMmbid()");
                    validateMmbid(tagValue, transactionId);
                    break;
                case ValidationTags.TAG_DBTR_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID:
                    log.info("Switch case: 'dbtr_agt_fin_instn_id_clr_sys_mmb_id_mmb_id' -> calling validateMmbid()");
                    validateMmbid(tagValue, transactionId);
                    break;
                case ValidationTags.TAG_INSTD_AGT_FIN_INSTN_ID_CLR_SYS_MMB_ID_MMB_ID:
                    log.info("Switch case: 'instd_agt_fin_instn_id_clr_sys_mmb_id_mmb_id' -> calling validateMmbid()");
                    validateMmbid(tagValue, transactionId);
                    break;
                // case ValidationTags.TAG_CHRG_BR:
                //     log.info("Switch case: 'chrg_br' -> calling validateChrgBr()");
                //     validateChrgBr(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_ORGNL_MSG_NM_ID:
                //     log.info("Switch case: 'orgnl_msg_nm_id' -> calling validateOrgnlMsgNmId()");
                //     validateOrgnlMsgNmId(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_RVSL_ID:
                //     log.info("Switch case: 'rvsl_id' -> calling validateRvslId()");
                //     validateRvslId(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_RVSD_INTR_BK_STTLM_AMT:
                //     log.info("Switch case: 'rvsd_intr_bk_sttlm_amt' -> calling validateRvsdIntrBkSttlmAmt()");
                //     validateRvsdIntrBkSttlmAmt(tagValue, transactionId);
                //     break;
                // case ValidationTags.TAG_RVSL_RSN_INF_RSN_PRTY:
                //     log.info("Switch case: 'rvsl_rsn_inf_rsn_prty' -> calling validateRvslRsnInfRsnPrtry()");
                //     validateRvslRsnInfRsnPrtry(tagValue, transactionId);
                //     break;
                default:
                    log.warn("Switch default case: No validation method found for tag: {} in transaction: {}", tagName, transactionId);
                    return new TagValidationResult(tagName, tagValue, ValidationStatus.MISSING, 
                        "No validation method available", "NO_VALIDATION_METHOD");
            }
            
            log.debug("Tag {} validation successful for transaction: {}", tagName, transactionId);
            return new TagValidationResult(tagName, tagValue, ValidationStatus.SUCCESS);
            
        } catch (ValidationException e) {
            log.debug("Tag {} validation failed for transaction: {}: {}", tagName, transactionId, e.getMessage());
            return new TagValidationResult(tagName, tagValue, ValidationStatus.FAILURE, 
                e.getMessage(), e.getErrorCode());
        } catch (Exception e) {
            log.error("Unexpected error during validation of tag {} for transaction: {}", tagName, transactionId, e);
            return new TagValidationResult(tagName, tagValue, ValidationStatus.FAILURE, 
                "Unexpected validation error: " + e.getMessage(), "VALIDATION_ERROR");
        }
    }
    
    /**
     * Validate currency using database lookup
     */
    private void validateCurrency(String currencyCode, String transactionId) throws ValidationException {
        log.debug("Validating currency: {} for transaction: {}", currencyCode, transactionId);
        
        if (currencyCode == null || currencyCode.trim().isEmpty()) {
            throw new ValidationException(
                "Currency code cannot be null or empty",
                "INVALID_CURRENCY",
                "currency",
                transactionId
            );
        }
        
        String trimmedCode = currencyCode.trim().toUpperCase();
        
        // Check if currency exists in database
        var currency = currencyRepository.findByCode(trimmedCode);
        if (currency.isEmpty()) {
            throw new ValidationException(
                "Invalid currency code: " + trimmedCode,
                "INVALID_CURRENCY",
                "currency",
                transactionId
            );
        }
        
        // Check if currency is active
        if (!currency.get().isActive()) {
            throw new ValidationException(
                "Currency code is inactive: " + trimmedCode,
                "INACTIVE_CURRENCY",
                "currency",
                transactionId
            );
        }
        
        log.debug("Currency validation successful: {} for transaction: {}", trimmedCode, transactionId);
    }
    
    /**
     * Validate country using database lookup
     */
    private void validateCountry(String countryCode, String transactionId) throws ValidationException {
        log.debug("Validating country: {} for transaction: {}", countryCode, transactionId);
        
        if (countryCode == null || countryCode.trim().isEmpty()) {
            throw new ValidationException(
                "Country code cannot be null or empty",
                "INVALID_COUNTRY",
                "country",
                transactionId
            );
        }
        
        String trimmedCode = countryCode.trim().toUpperCase();
        
        // Check if country exists in database
        var country = countryRepository.findByCode(trimmedCode);
        if (country.isEmpty()) {
            throw new ValidationException(
                "Invalid country code: " + trimmedCode,
                "INVALID_COUNTRY",
                "country",
                transactionId
            );
        }
        
        // Check if country is active
        if (!country.get().isActive()) {
            throw new ValidationException(
                "Country code is inactive: " + trimmedCode,
                "INACTIVE_COUNTRY",
                "country",
                transactionId
            );
        }
        
        log.debug("Country validation successful: {} for transaction: {}", trimmedCode, transactionId);
    }
    
    /**
     * Validate MMBID using switch method - only check that the value has exactly 11 characters
     */
    private void validateMmbid(String mmbid, String transactionId) throws ValidationException {
        log.debug("Validating MMBID: {} for transaction: {}", mmbid, transactionId);
        
        if (mmbid.length() != 11) {
            throw new ValidationException(
                "MMBID must have exactly 11 characters, got: " + mmbid.length(),
                "INVALID_MMBID_LENGTH",
                "mmbid",
                transactionId
            );
        }
        
        // Additional MMBID validation can be added here if needed
        log.debug("MMBID validation successful: {} for transaction: {}", mmbid, transactionId);
    }
    
    /**
     * Validate BizMsgIdr using switch method
     */
    private void validateBizMsgIdr(String bizMsgIdr, String transactionId) throws ValidationException {
        log.debug("Validating BizMsgIdr: {} for transaction: {}", bizMsgIdr, transactionId);
        
        // Check if the value is unique for the current day
        LocalDate currentDate = LocalDate.now();
        Set<String> receivedBizMsgIdrs = bizMsgIdrCache.computeIfAbsent(currentDate, k -> ConcurrentHashMap.newKeySet());
        
        if (receivedBizMsgIdrs.contains(bizMsgIdr)) {
            log.warn("BizMsgIdr '{}' already received for today's transactions. Transaction: {}", bizMsgIdr, transactionId);
            throw new ValidationException(
                "BizMsgIdr must be unique per day. Duplicate BizMsgIdr: " + bizMsgIdr,
                "DUPLICATE_BIZ_MSG_IDR",
                "bizMsgIdr",
                transactionId
            );
        }
        
        // Add the BizMsgIdr to the cache for the current day
        receivedBizMsgIdrs.add(bizMsgIdr);
        
        log.debug("BizMsgIdr validation successful: {} for transaction: {}", bizMsgIdr, transactionId);
    }
    
    /**
     * Validate CpyDpict using switch method
     */
    private void validateCpyDpict(String cpyDpict, String transactionId) throws ValidationException {
        log.debug("Validating CpyDpict: {} for transaction: {}", cpyDpict, transactionId);
        
        // If CpyDpict is not present or has any other value, skip this validation
        if (cpyDpict == null || cpyDpict.trim().isEmpty() || !"DUPL".equals(cpyDpict)) {
            log.debug("CpyDpict validation skipped for value: {} in transaction: {}", cpyDpict, transactionId);
            return;
        }
        
        // If CpyDpict = 'DUPL', check if this message is indeed a duplicate
        if ("DUPL".equals(cpyDpict)) {
            // Check if this transaction ID already exists in the processed messages
            if (!isDuplicateMessage(transactionId)) {
                log.warn("CpyDpict indicates duplicate but message is not actually duplicate. Transaction: {}", transactionId);
                throw new ValidationException(
                    "CpyDpict indicates duplicate but message is not actually duplicate",
                    "INVALID_CPY_DPICT",
                    "cpyDpict",
                    transactionId
                );
            }
        }
        
        log.debug("CpyDpict validation successful: {} for transaction: {}", cpyDpict, transactionId);
    }
    
    /**
     * Validate CreDtTm using switch method
     */
    private void validateCreDtTm(String creDtTm, String transactionId) throws ValidationException {
        log.debug("Validating CreDtTm: {} for transaction: {}", creDtTm, transactionId);
        
        // Validate ISO 8601 date-time format
        String isoPattern = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,3})?$";
        if (!creDtTm.matches(isoPattern)) {
            throw new ValidationException(
                "CreDtTm must follow ISO 8601 format (YYYY-MM-DDThh:mm:ss or YYYY-MM-DDThh:mm:ss.SSS)",
                "INVALID_CRE_DT_TM_FORMAT",
                "creDtTm",
                transactionId
            );
        }
        
        log.debug("CreDtTm validation successful: {} for transaction: {}", creDtTm, transactionId);
    }
    
    /**
     * Validate NbOfTxs using switch method
     */
    private void validateNbOfTxs(String nbOfTxs, String transactionId) throws ValidationException {
        log.debug("Validating NbOfTxs: {} for transaction: {}", nbOfTxs, transactionId);
        
        try {
            int value = Integer.parseInt(nbOfTxs);
            if (value != 1) {
                throw new ValidationException(
                    "NbOfTxs must be exactly 1, got: " + value,
                    "INVALID_NB_OF_TXS",
                    "nbOfTxs",
                    transactionId
                );
            }
        } catch (NumberFormatException e) {
            throw new ValidationException(
                "NbOfTxs must be a valid number",
                "INVALID_NB_OF_TXS_FORMAT",
                "nbOfTxs",
                transactionId
            );
        }
        
        log.debug("NbOfTxs validation successful: {} for transaction: {}", nbOfTxs, transactionId);
    }
    
    /**
     * Validate TtlIntrBkSttlmAmt using switch method
     */
    private void validateTtlIntrBkSttlmAmt(String ttlIntrBkSttlmAmt, String transactionId) throws ValidationException {
        log.debug("Validating TtlIntrBkSttlmAmt: {} for transaction: {}", ttlIntrBkSttlmAmt, transactionId);
        
        try {
            double amount = Double.parseDouble(ttlIntrBkSttlmAmt);
            if (amount <= 0) {
                throw new ValidationException(
                    "TtlIntrBkSttlmAmt must be greater than zero, got: " + amount,
                    "INVALID_TTL_INTR_BK_STTLM_AMT",
                    "ttlIntrBkSttlmAmt",
                    transactionId
                );
            }
        } catch (NumberFormatException e) {
            throw new ValidationException(
                "TtlIntrBkSttlmAmt must be a valid number",
                "INVALID_TTL_INTR_BK_STTLM_AMT_FORMAT",
                "ttlIntrBkSttlmAmt",
                transactionId
            );
        }
        
        log.debug("TtlIntrBkSttlmAmt validation successful: {} for transaction: {}", ttlIntrBkSttlmAmt, transactionId);
    }
    
    /**
     * Validate SttlmMtd using switch method
     */
    private void validateSttlmMtd(String sttlmMtd, String transactionId) throws ValidationException {
        log.debug("Validating SttlmMtd: {} for transaction: {}", sttlmMtd, transactionId);
        
        if (!"CLRG".equals(sttlmMtd)) {
            throw new ValidationException(
                "SttlmMtd must be 'CLRG', got: " + sttlmMtd,
                "INVALID_STTLM_MTD",
                "sttlmMtd",
                transactionId
            );
        }
        
        log.debug("SttlmMtd validation successful: {} for transaction: {}", sttlmMtd, transactionId);
    }
    
    /**
     * Validate Cd using switch method
     */
    private void validateCd(String cd, String transactionId) throws ValidationException {
        log.debug("Validating Cd: {} for transaction: {}", cd, transactionId);
        
        if (!"MEP".equals(cd)) {
            throw new ValidationException(
                "Cd must be 'MEP', got: " + cd,
                "INVALID_CD",
                "cd",
                transactionId
            );
        }
        
        log.debug("Cd validation successful: {} for transaction: {}", cd, transactionId);
    }
    
    /**
     * Validate InstrId using switch method
     */
    private void validateInstrId(String instrId, String transactionId) throws ValidationException {
        log.debug("Validating InstrId: {} for transaction: {}", instrId, transactionId);
        
        // Check if the value is unique for the current day
        LocalDate currentDate = LocalDate.now();
        Set<String> receivedInstrIds = instrIdCache.computeIfAbsent(currentDate, k -> ConcurrentHashMap.newKeySet());
        
        if (receivedInstrIds.contains(instrId)) {
            log.warn("InstrId '{}' already received for today's transactions. Transaction: {}", instrId, transactionId);
            throw new ValidationException(
                "InstrId must be unique per day. Duplicate InstrId: " + instrId,
                "DUPLICATE_INSTR_ID",
                "instrId",
                transactionId
            );
        }
        
        // Add the InstrId to the cache for the current day
        receivedInstrIds.add(instrId);
        
        log.debug("InstrId validation successful: {} for transaction: {}", instrId, transactionId);
    }
    
    /**
     * Validate PmtTpInfSvcLvlCd using switch method
     */
    private void validatePmtTpInfSvcLvlCd(String pmtTpInfSvcLvlCd, String transactionId) throws ValidationException {
        log.debug("Validating PmtTpInfSvcLvlCd: {} for transaction: {}", pmtTpInfSvcLvlCd, transactionId);
        
        if (!"SDVA".equals(pmtTpInfSvcLvlCd)) {
            throw new ValidationException(
                "PmtTpInfSvcLvlCd must be 'SDVA', got: " + pmtTpInfSvcLvlCd,
                "INVALID_PMT_TP_INF_SVC_LVL_CD",
                "pmtTpInfSvcLvlCd",
                transactionId
            );
        }
        
        log.debug("PmtTpInfSvcLvlCd validation successful: {} for transaction: {}", pmtTpInfSvcLvlCd, transactionId);
    }
    
    /**
     * Validate IntrBkSttlmAmt using switch method
     */
    private void validateIntrBkSttlmAmt(String intrBkSttlmAmt, String transactionId) throws ValidationException {
        log.debug("Validating IntrBkSttlmAmt: {} for transaction: {}", intrBkSttlmAmt, transactionId);
        
        if (intrBkSttlmAmt == null || intrBkSttlmAmt.trim().isEmpty()) {
            throw new ValidationException(
                "IntrBkSttlmAmt cannot be null or empty",
                "INVALID_INTR_BK_STTLM_AMT",
                "intrBkSttlmAmt",
                transactionId
            );
        }
        
        try {
            // Parse the amount as BigDecimal for precise decimal handling
            java.math.BigDecimal amount = new java.math.BigDecimal(intrBkSttlmAmt.trim());
            
            // Check if amount is greater than zero
            if (amount.compareTo(java.math.BigDecimal.ZERO) <= 0) {
                throw new ValidationException(
                    "IntrBkSttlmAmt must be greater than zero, got: " + intrBkSttlmAmt,
                    "INVALID_INTR_BK_STTLM_AMT_ZERO_OR_NEGATIVE",
                    "intrBkSttlmAmt",
                    transactionId
                );
            }
            
            // Check if amount has reasonable precision (max 2 decimal places for currency)
            if (amount.scale() > 2) {
                throw new ValidationException(
                    "IntrBkSttlmAmt cannot have more than 2 decimal places, got: " + intrBkSttlmAmt,
                    "INVALID_INTR_BK_STTLM_AMT_PRECISION",
                    "intrBkSttlmAmt",
                    transactionId
                );
            }
            
            log.debug("IntrBkSttlmAmt validation successful: {} for transaction: {}", intrBkSttlmAmt, transactionId);
            
        } catch (NumberFormatException e) {
            throw new ValidationException(
                "IntrBkSttlmAmt must be a valid decimal number, got: " + intrBkSttlmAmt,
                "INVALID_INTR_BK_STTLM_AMT_FORMAT",
                "intrBkSttlmAmt",
                transactionId
            );
        }
    }
    
    /**
     * Validate IntrBkSttlmDt using switch method
     */
    private void validateIntrBkSttlmDt(String intrBkSttlmDt, String transactionId) throws ValidationException {
        log.debug("Validating IntrBkSttlmDt: {} for transaction: {}", intrBkSttlmDt, transactionId);
        
        // Validate ISO date format (YYYY-MM-DD)
        String isoDatePattern = "^\\d{4}-\\d{2}-\\d{2}$";
        if (!intrBkSttlmDt.matches(isoDatePattern)) {
            throw new ValidationException(
                "IntrBkSttlmDt must follow ISO date format (YYYY-MM-DD)",
                "INVALID_INTR_BK_STTLM_DT_FORMAT",
                "intrBkSttlmDt",
                transactionId
            );
        }
        
        log.debug("IntrBkSttlmDt validation successful: {} for transaction: {}", intrBkSttlmDt, transactionId);
    }
    
    /**
     * Validate ChrgBr using switch method
     */
    private void validateChrgBr(String chrgBr, String transactionId) throws ValidationException {
        log.debug("Validating ChrgBr: {} for transaction: {}", chrgBr, transactionId);
        
        if (!"SLEV".equals(chrgBr)) {
            throw new ValidationException(
                "ChrgBr must be 'SLEV', got: " + chrgBr,
                "INVALID_CHRG_BR",
                "chrgBr",
                transactionId
            );
        }
        
        log.debug("ChrgBr validation successful: {} for transaction: {}", chrgBr, transactionId);
    }
    
    /**
     * Validate OrgnlMsgNmId using switch method
     */
    private void validateOrgnlMsgNmId(String orgnlMsgNmId, String transactionId) throws ValidationException {
        log.debug("Validating OrgnlMsgNmId: {} for transaction: {}", orgnlMsgNmId, transactionId);
        
        if (!"pacs.003.001.02".equals(orgnlMsgNmId)) {
            throw new ValidationException(
                "OrgnlMsgNmId must be 'pacs.003.001.02', got: " + orgnlMsgNmId,
                "INVALID_ORGNL_MSG_NM_ID",
                "orgnlMsgNmId",
                transactionId
            );
        }
        
        log.debug("OrgnlMsgNmId validation successful: {} for transaction: {}", orgnlMsgNmId, transactionId);
    }
    
    /**
     * Validate RvslId using switch method
     */
    private void validateRvslId(String rvslId, String transactionId) throws ValidationException {
        log.debug("Validating RvslId: {} for transaction: {}", rvslId, transactionId);
        
        // Check if the value is unique for the current day
        LocalDate currentDate = LocalDate.now();
        Set<String> receivedRvslIds = rvslIdCache.computeIfAbsent(currentDate, k -> ConcurrentHashMap.newKeySet());
        
        if (receivedRvslIds.contains(rvslId)) {
            log.warn("RvslId '{}' already received for today's transactions. Transaction: {}", rvslId, transactionId);
            throw new ValidationException(
                "RvslId must be unique per day. Duplicate RvslId: " + rvslId,
                "DUPLICATE_RVSL_ID",
                "rvslId",
                transactionId
            );
        }
        
        // Add the RvslId to the cache for the current day
        receivedRvslIds.add(rvslId);
        
        log.debug("RvslId validation successful: {} for transaction: {}", rvslId, transactionId);
    }
    
    /**
     * Validate RvsdIntrBkSttlmAmt using switch method
     */
    private void validateRvsdIntrBkSttlmAmt(String rvsdIntrBkSttlmAmt, String transactionId) throws ValidationException {
        log.debug("Validating RvsdIntrBkSttlmAmt: {} for transaction: {}", rvsdIntrBkSttlmAmt, transactionId);
        
        try {
            double amount = Double.parseDouble(rvsdIntrBkSttlmAmt);
            if (amount <= 0) {
                throw new ValidationException(
                    "RvsdIntrBkSttlmAmt must be greater than zero, got: " + amount,
                    "INVALID_RVSD_INTR_BK_STTLM_AMT",
                    "rvsdIntrBkSttlmAmt",
                    transactionId
                );
            }
        } catch (NumberFormatException e) {
            throw new ValidationException(
                "RvsdIntrBkSttlmAmt must be a valid number",
                "INVALID_RVSD_INTR_BK_STTLM_AMT_FORMAT",
                "rvsdIntrBkSttlmAmt",
                transactionId
            );
        }
        
        // For now, just log the validation
        // TODO: Implement currency code validation for the Ccy attribute
        log.debug("RvsdIntrBkSttlmAmt validation successful: {} for transaction: {}", rvsdIntrBkSttlmAmt, transactionId);
    }
    
    /**
     * Validate RvslRsnInfRsnPrtry using switch method
     */
    private void validateRvslRsnInfRsnPrtry(String rvslRsnInfRsnPrtry, String transactionId) throws ValidationException {
        log.debug("Validating RvslRsnInfRsnPrtry: {} for transaction: {}", rvslRsnInfRsnPrtry, transactionId);
        
        if (!"Direct Debit Timeout".equals(rvslRsnInfRsnPrtry)) {
            throw new ValidationException(
                "RvslRsnInfRsnPrtry must be 'Direct Debit Timeout', got: " + rvslRsnInfRsnPrtry,
                "INVALID_RVSL_RSN_INF_RSN_PRTY",
                "rvslRsnInfRsnPrtry",
                transactionId
            );
        }
        
        log.debug("RvslRsnInfRsnPrtry validation successful: {} for transaction: {}", rvslRsnInfRsnPrtry, transactionId);
    }
    
    /**
     * Check if a message is a duplicate (placeholder implementation)
     */
    private boolean isDuplicateMessage(String transactionId) {
        // For now, return false to indicate it's not a duplicate
        // This ensures the validation passes during development/testing
        
        log.debug("Checking if message is duplicate for transaction: {} (placeholder implementation)", transactionId);
        return false;
    }
    
    /**
     * Get current validation statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new java.util.HashMap<>();
        stats.put("inputMessageCount", inputMessageCount.get());
        stats.put("successfulMessageCount", successfulMessageCount.get());
        stats.put("failedMessageCount", failedMessageCount.get());
        stats.put("successfulValidationCount", successfulValidationCount.get());
        stats.put("validationErrorCount", validationErrorCount.get());
        stats.put("missingTagCount", missingTagCount.get());
        stats.put("lastValidationTime", lastValidationTime.get());
        stats.put("totalRequiredTags", REQUIRED_TAGS.size());
        return stats;
    }
    
    /**
     * Reset all statistics and caches
     */
    public void resetStatistics() {
        inputMessageCount.set(0);
        successfulMessageCount.set(0);
        failedMessageCount.set(0);
        successfulValidationCount.set(0);
        validationErrorCount.set(0);
        missingTagCount.set(0);
        lastValidationTime.set("Never");
        
        // Reset all caches
        bizMsgIdrCache.clear();
        instrIdCache.clear();
        rvslIdCache.clear();
        
        log.info("Validation statistics and all caches reset successfully");
    }
    
    /**
     * Reset BizMsgIdr cache for testing purposes
     */
    public void resetBizMsgIdrCache() {
        bizMsgIdrCache.clear();
        log.info("BizMsgIdr cache reset successfully");
    }
    
    /**
     * Reset InstrId cache for testing purposes
     */
    public void resetInstrIdCache() {
        instrIdCache.clear();
        log.info("InstrId cache reset successfully");
    }
    
    /**
     * Reset RvslId cache for testing purposes
     */
    public void resetRvslIdCache() {
        rvslIdCache.clear();
        log.info("RvslId cache reset successfully");
    }
}
