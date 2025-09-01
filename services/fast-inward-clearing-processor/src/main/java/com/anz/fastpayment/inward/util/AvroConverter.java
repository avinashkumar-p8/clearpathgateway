package com.anz.fastpayment.inward.util;

import com.anz.fastpayment.inward.avro.UnifiedPaymentMessage;
import com.anz.fastpayment.inward.model.ProcessedTransactionMessage;
import com.anz.fastpayment.inward.model.TransactionMessage;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;

/**
 * Utility class for converting between Avro SpecificRecord and POJO objects
 * Updated to handle complex nested payment message structure
 */
public class AvroConverter {

    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    /**
     * Convert Avro UnifiedPaymentMessage to TransactionMessage POJO
     * Updated to extract data from complex nested structure
     * 
     * @param avroRecord The Avro UnifiedPaymentMessage
     * @return TransactionMessage POJO
     */
    public static TransactionMessage convertToTransactionMessage(GenericRecord avroRecord) {
        if (avroRecord == null) {
            return null;
        }

        try {
            // Cast to the specific Avro class
            UnifiedPaymentMessage unifiedMessage = (UnifiedPaymentMessage) avroRecord;
            
            TransactionMessage transactionMessage = new TransactionMessage();
            
            // Extract transaction ID from Header.UUID or Body.PmtAddRq[0].RqUID
            String transactionId = extractTransactionId(unifiedMessage);
            transactionMessage.setTransactionId(transactionId);
            
            // Extract amount and currency from Body.PmtAddRq[0].FromAcct
            if (unifiedMessage.getBody() != null && 
                unifiedMessage.getBody().getPmtAddRq() != null && 
                unifiedMessage.getBody().getPmtAddRq().size() > 0) {
                
                var firstPaymentRequest = unifiedMessage.getBody().getPmtAddRq().get(0);
                
                // Extract amount and currency from FromAcct
                if (firstPaymentRequest.getFromAcct() != null) {
                    var fromAcct = firstPaymentRequest.getFromAcct();
                    // Amount is double, so convert to BigDecimal directly
                    transactionMessage.setAmount(new BigDecimal(fromAcct.getAmount()));
                    if (fromAcct.getCurCode() != null) {
                        transactionMessage.setCurrency(fromAcct.getCurCode().toString());
                    }
                    if (fromAcct.getAcctId() != null) {
                        transactionMessage.setSenderAccount(fromAcct.getAcctId().toString());
                    }
                }
                
                // Extract receiver account from ToAcct
                if (firstPaymentRequest.getToAcct() != null) {
                    var toAcct = firstPaymentRequest.getToAcct();
                    if (toAcct.getAcctId() != null) {
                        transactionMessage.setReceiverAccount(toAcct.getAcctId().toString());
                    }
                }
                
                // Extract transaction type from Procctxt.PmtDtls.PmtCtgry
                String transactionType = extractTransactionType(unifiedMessage);
                if (transactionType != null) {
                    transactionMessage.setTransactionType(transactionType);
                }
                
                // Extract priority (could be derived from amount or other factors)
                String priority = determinePriority(unifiedMessage);
                transactionMessage.setPriority(priority);
                
                // Extract timestamp from Header.EventInfo.EventTS or Header.RcvdTS
                LocalDateTime timestamp = extractTimestamp(unifiedMessage);
                if (timestamp != null) {
                    transactionMessage.setTimestamp(timestamp);
                }
                
                // Extract description from FromAcct.Narrative or ToAcct.Narrative
                String description = extractDescription(unifiedMessage);
                if (description != null) {
                    transactionMessage.setDescription(description);
                }
                
                // Extract reference from PayHdr.PaymentID or PayHdr.ThirdPartyPayID
                String reference = extractReference(unifiedMessage);
                if (reference != null) {
                    transactionMessage.setReference(reference);
                }
            }
            
            logger.debug("Successfully converted complex Avro UnifiedPaymentMessage to TransactionMessage: {}", 
                       transactionMessage.getTransactionId());
            
            return transactionMessage;
            
        } catch (Exception e) {
            logger.error("Error converting complex Avro record to TransactionMessage", e);
            throw new RuntimeException("Failed to convert complex Avro record to TransactionMessage", e);
        }
    }

    /**
     * Extract transaction ID from various possible locations in the message
     */
    private static String extractTransactionId(UnifiedPaymentMessage message) {
        // Try Header.UUID first
        if (message.getHeader() != null && message.getHeader().getUUID() != null) {
            return message.getHeader().getUUID().toString();
        }
        
        // Try Body.PmtAddRq[0].RqUID
        if (message.getBody() != null && 
            message.getBody().getPmtAddRq() != null && 
            message.getBody().getPmtAddRq().size() > 0) {
            
            var firstPaymentRequest = message.getBody().getPmtAddRq().get(0);
            if (firstPaymentRequest.getRqUID() != null) {
                return firstPaymentRequest.getRqUID().toString();
            }
            
            // Try PayHdr.PaymentID
            if (firstPaymentRequest.getPayHdr() != null && 
                firstPaymentRequest.getPayHdr().getPaymentID() != null) {
                return firstPaymentRequest.getPayHdr().getPaymentID().toString();
            }
        }
        
        // Fallback to generated UUID
        return java.util.UUID.randomUUID().toString();
    }

    /**
     * Extract transaction type from processing context
     */
    private static String extractTransactionType(UnifiedPaymentMessage message) {
        if (message.getProcctxt() != null && 
            message.getProcctxt().getPmtDtls() != null && 
            message.getProcctxt().getPmtDtls().getPmtCtgry() != null) {
            
            String category = message.getProcctxt().getPmtDtls().getPmtCtgry().toString();
            
            // Map payment categories to transaction types
            switch (category) {
                case "DD":
                    return "DDI"; // Direct Debit Inward
                case "CT":
                    return "CTI"; // Credit Transfer Inward
                default:
                    return category;
            }
        }
        return "UNKNOWN";
    }

    /**
     * Determine priority based on amount and other factors
     */
    private static String determinePriority(UnifiedPaymentMessage message) {
        try {
            if (message.getBody() != null && 
                message.getBody().getPmtAddRq() != null && 
                message.getBody().getPmtAddRq().size() > 0) {
                
                var firstPaymentRequest = message.getBody().getPmtAddRq().get(0);
                if (firstPaymentRequest.getFromAcct() != null) {
                    
                    double amountDouble = firstPaymentRequest.getFromAcct().getAmount();
                    BigDecimal amount = new BigDecimal(amountDouble);
                    
                    // Simple priority logic based on amount
                    if (amount.compareTo(new BigDecimal("100000")) >= 0) {
                        return "HIGH";
                    } else if (amount.compareTo(new BigDecimal("10000")) >= 0) {
                        return "NORMAL";
                    } else {
                        return "LOW";
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Could not determine priority, using default", e);
        }
        
        return "NORMAL";
    }

    /**
     * Extract timestamp from various possible locations
     */
    private static LocalDateTime extractTimestamp(UnifiedPaymentMessage message) {
        try {
            // Try Header.EventInfo.EventTS first
            if (message.getHeader() != null && 
                message.getHeader().getEventInfo() != null && 
                message.getHeader().getEventInfo().getEventTS() != null) {
                
                String eventTS = message.getHeader().getEventInfo().getEventTS().toString();
                return LocalDateTime.parse(eventTS, TIMESTAMP_FORMATTER);
            }
            
            // Try Header.RcvdTS
            if (message.getHeader() != null && 
                message.getHeader().getRcvdTS() != null) {
                
                String rcvdTS = message.getHeader().getRcvdTS().toString();
                return LocalDateTime.parse(rcvdTS, TIMESTAMP_FORMATTER);
            }
            
            // Try Body.PmtAddRq[0].MsgHdr.ClientDt
            if (message.getBody() != null && 
                message.getBody().getPmtAddRq() != null && 
                message.getBody().getPmtAddRq().size() > 0) {
                
                var firstPaymentRequest = message.getBody().getPmtAddRq().get(0);
                if (firstPaymentRequest.getMsgHdr() != null && 
                    firstPaymentRequest.getMsgHdr().getClientDt() != null) {
                    
                    String clientDt = firstPaymentRequest.getMsgHdr().getClientDt().toString();
                    return LocalDateTime.parse(clientDt, TIMESTAMP_FORMATTER);
                }
            }
            
        } catch (Exception e) {
            logger.warn("Could not parse timestamp from message, using current time", e);
        }
        
        return LocalDateTime.now();
    }

    /**
     * Extract description from narrative fields
     */
    private static String extractDescription(UnifiedPaymentMessage message) {
        try {
            if (message.getBody() != null && 
                message.getBody().getPmtAddRq() != null && 
                message.getBody().getPmtAddRq().size() > 0) {
                
                var firstPaymentRequest = message.getBody().getPmtAddRq().get(0);
                
                // Try FromAcct.Narrative first
                if (firstPaymentRequest.getFromAcct() != null && 
                    firstPaymentRequest.getFromAcct().getNarrative() != null) {
                    return firstPaymentRequest.getFromAcct().getNarrative().toString();
                }
                
                // Try ToAcct.Narrative
                if (firstPaymentRequest.getToAcct() != null && 
                    firstPaymentRequest.getToAcct().getNarrative() != null) {
                    return firstPaymentRequest.getToAcct().getNarrative().toString();
                }
            }
        } catch (Exception e) {
            logger.warn("Could not extract description", e);
        }
        
        return null;
    }

    /**
     * Extract reference from payment header
     */
    private static String extractReference(UnifiedPaymentMessage message) {
        try {
            if (message.getBody() != null && 
                message.getBody().getPmtAddRq() != null && 
                message.getBody().getPmtAddRq().size() > 0) {
                
                var firstPaymentRequest = message.getBody().getPmtAddRq().get(0);
                
                // Try PayHdr.ThirdPartyPayID first
                if (firstPaymentRequest.getPayHdr() != null && 
                    firstPaymentRequest.getPayHdr().getThirdPartyPayID() != null) {
                    return firstPaymentRequest.getPayHdr().getThirdPartyPayID().toString();
                }
                
                // Try PayHdr.PaymentID
                if (firstPaymentRequest.getPayHdr() != null && 
                    firstPaymentRequest.getPayHdr().getPaymentID() != null) {
                    return firstPaymentRequest.getPayHdr().getPaymentID().toString();
                }
            }
        } catch (Exception e) {
            logger.warn("Could not extract reference", e);
        }
        
        return null;
    }

    /**
     * Extract transaction ID from Avro message for error handling
     */
    public static String extractTransactionId(GenericRecord avroMessage) {
        try {
            if (avroMessage instanceof UnifiedPaymentMessage) {
                UnifiedPaymentMessage message = (UnifiedPaymentMessage) avroMessage;
                return extractTransactionId(message);
            }
        } catch (Exception e) {
            logger.warn("Could not extract transaction ID from Avro message", e);
        }
        return "UNKNOWN";
    }

    /**
     * Convert ProcessedTransactionMessage POJO to Avro ProcessedTransactionMessage
     * 
     * @param processedMessage The ProcessedTransactionMessage POJO
     * @return Avro ProcessedTransactionMessage
     */
    public static GenericRecord convertToAvroRecord(ProcessedTransactionMessage processedMessage) {
        if (processedMessage == null) {
            return null;
        }

        try {
            // Create Avro ProcessedTransactionMessage using the generated class
            com.anz.fastpayment.inward.avro.ProcessedTransactionMessage avroMessage = new com.anz.fastpayment.inward.avro.ProcessedTransactionMessage();
            
            // Set all fields from the POJO
            avroMessage.setTransactionId(processedMessage.getTransactionId());
            // Handle amount field - convert BigDecimal to double for resources schema compatibility
            avroMessage.setAmount(processedMessage.getAmount() != null ? processedMessage.getAmount().doubleValue() : 0.0);
            avroMessage.setCurrency(processedMessage.getCurrency());
            avroMessage.setSenderAccount(processedMessage.getSenderAccount());
            avroMessage.setReceiverAccount(processedMessage.getReceiverAccount());
            avroMessage.setTransactionType(processedMessage.getTransactionType());
            avroMessage.setPriority(processedMessage.getPriority());
            
            // Handle timestamps
            if (processedMessage.getOriginalTimestamp() != null) {
                avroMessage.setTimestamp(processedMessage.getOriginalTimestamp().format(TIMESTAMP_FORMATTER));
            }
            
            if (processedMessage.getProcessingTimestamp() != null) {
                avroMessage.setProcessingTimestamp(processedMessage.getProcessingTimestamp().format(TIMESTAMP_FORMATTER));
            }
            
            // Set additional fields
            avroMessage.setStatus(processedMessage.getStatus());
            avroMessage.setProcessingNodeId(processedMessage.getProcessingNodeId());
            
            // Note: These fields may not exist in the current Avro schema
            // Comment them out until the schema is updated
            // avroMessage.setBusinessRuleResults(processedMessage.getBusinessRuleResults());
            // avroMessage.setRiskScore(processedMessage.getRiskScore());
            // avroMessage.setErrorMessage(processedMessage.getErrorMessage());
            
            logger.debug("Successfully converted ProcessedTransactionMessage POJO to Avro: {}", 
                       processedMessage.getTransactionId());
            
            return avroMessage;
            
        } catch (Exception e) {
            logger.error("Error converting ProcessedTransactionMessage POJO to Avro", e);
            throw new RuntimeException("Failed to convert ProcessedTransactionMessage POJO to Avro", e);
        }
    }

    /**
     * Convert ProcessedTransactionMessage POJO to Avro ProcessedTransactionMessage
     * 
     * @param processedMessage The ProcessedTransactionMessage POJO
     * @return Avro ProcessedTransactionMessage
     */
    public static com.anz.fastpayment.inward.avro.ProcessedTransactionMessage convertToAvroProcessedMessage(ProcessedTransactionMessage processedMessage) {
        if (processedMessage == null) {
            return null;
        }

        try {
            // Create Avro ProcessedTransactionMessage using the generated class
            com.anz.fastpayment.inward.avro.ProcessedTransactionMessage avroMessage = new com.anz.fastpayment.inward.avro.ProcessedTransactionMessage();
            
            // Set all fields from the POJO
            avroMessage.setTransactionId(processedMessage.getTransactionId());
            // Handle amount field - convert BigDecimal to double for resources schema compatibility
            avroMessage.setAmount(processedMessage.getAmount() != null ? processedMessage.getAmount().doubleValue() : 0.0);
            avroMessage.setCurrency(processedMessage.getCurrency());
            avroMessage.setSenderAccount(processedMessage.getSenderAccount());
            avroMessage.setReceiverAccount(processedMessage.getReceiverAccount());
            avroMessage.setTransactionType(processedMessage.getTransactionType());
            avroMessage.setPriority(processedMessage.getPriority());
            
            // Handle timestamps
            if (processedMessage.getOriginalTimestamp() != null) {
                avroMessage.setTimestamp(processedMessage.getOriginalTimestamp().format(TIMESTAMP_FORMATTER));
            }
            
            if (processedMessage.getProcessingTimestamp() != null) {
                avroMessage.setProcessingTimestamp(processedMessage.getProcessingTimestamp().format(TIMESTAMP_FORMATTER));
            }
            
            // Set additional fields
            avroMessage.setStatus(processedMessage.getStatus());
            avroMessage.setProcessingNodeId(processedMessage.getProcessingNodeId());
            
            // Note: These fields may not exist in the current Avro schema
            // Comment them out until the schema is updated
            // avroMessage.setBusinessRuleResults(processedMessage.getBusinessRuleResults());
            // avroMessage.setRiskScore(processedMessage.getRiskScore());
            // avroMessage.setErrorMessage(processedMessage.getErrorMessage());
            
            logger.debug("Successfully converted ProcessedTransactionMessage POJO to Avro: {}", 
                       processedMessage.getTransactionId());
            
            return avroMessage;
            
        } catch (Exception e) {
            logger.error("Error converting ProcessedTransactionMessage POJO to Avro", e);
            throw new RuntimeException("Failed to convert ProcessedTransactionMessage POJO to Avro", e);
        }
    }
}
