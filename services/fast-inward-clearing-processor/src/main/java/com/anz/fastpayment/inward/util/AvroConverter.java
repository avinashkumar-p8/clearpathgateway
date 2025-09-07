package com.anz.fastpayment.inward.util;


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
     * Convert Avro GenericRecord to TransactionMessage POJO
     * Updated to extract data from complex nested structure using GenericRecord
     * 
     * @param avroRecord The Avro GenericRecord (InputMessage)
     * @return TransactionMessage POJO
     */
    public static TransactionMessage convertToTransactionMessage(GenericRecord avroRecord) {
        if (avroRecord == null) {
            return null;
        }

        try {
            // Work with GenericRecord directly - no casting needed
            
            TransactionMessage transactionMessage = new TransactionMessage();
            
            // Extract transaction ID from Header.UUID or Body.PmtAddRq[0].RqUID
            String transactionId = extractTransactionId(avroRecord);
            transactionMessage.setTransactionId(transactionId);
            
            // Extract amount and currency from Body.PmtAddRq[0].FromAcct
            GenericRecord body = (GenericRecord) avroRecord.get("Body");
            if (body != null) {
                @SuppressWarnings("unchecked")
                java.util.List<GenericRecord> pmtAddRq = (java.util.List<GenericRecord>) body.get("PmtAddRq");
                if (pmtAddRq != null && pmtAddRq.size() > 0) {
                    GenericRecord firstPaymentRequest = pmtAddRq.get(0);
                
                    // Extract amount and currency from FromAcct
                    GenericRecord fromAcct = (GenericRecord) firstPaymentRequest.get("FromAcct");
                    if (fromAcct != null) {
                        // Amount is double, so convert to BigDecimal directly
                        Double amount = (Double) fromAcct.get("Amount");
                        if (amount != null) {
                            transactionMessage.setAmount(new BigDecimal(amount));
                        }
                        Object curCode = fromAcct.get("CurCode");
                        if (curCode != null) {
                            transactionMessage.setCurrency(curCode.toString());
                        }
                        Object acctId = fromAcct.get("AcctId");
                        if (acctId != null) {
                            transactionMessage.setSenderAccount(acctId.toString());
                        }
                    }
                    
                    // Extract receiver account from ToAcct
                    GenericRecord toAcct = (GenericRecord) firstPaymentRequest.get("ToAcct");
                    if (toAcct != null) {
                        Object acctId = toAcct.get("AcctId");
                        if (acctId != null) {
                            transactionMessage.setReceiverAccount(acctId.toString());
                        }
                    }
                
                }
                
                // Extract transaction type from Procctxt.PmtDtls.PmtCtgry
                String transactionType = extractTransactionType(avroRecord);
                if (transactionType != null) {
                    transactionMessage.setTransactionType(transactionType);
                }
                
                // Extract priority (could be derived from amount or other factors)
                String priority = determinePriority(avroRecord);
                transactionMessage.setPriority(priority);
                
                // Extract timestamp from Header.EventInfo.EventTS or Header.RcvdTS
                LocalDateTime timestamp = extractTimestamp(avroRecord);
                if (timestamp != null) {
                    transactionMessage.setTimestamp(timestamp);
                }
                
                // Extract description from FromAcct.Narrative or ToAcct.Narrative
                String description = extractDescription(avroRecord);
                if (description != null) {
                    transactionMessage.setDescription(description);
                }
                
                // Extract reference from PayHdr.PaymentID or PayHdr.ThirdPartyPayID
                String reference = extractReference(avroRecord);
                if (reference != null) {
                    transactionMessage.setReference(reference);
                }
            }
            
            logger.debug("Successfully converted complex Avro GenericRecord to TransactionMessage: {}", 
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
    private static String extractTransactionId(GenericRecord message) {
        // Try Header.UUID first
        GenericRecord header = (GenericRecord) message.get("Header");
        if (header != null) {
            Object uuid = header.get("UUID");
            if (uuid != null) {
                return uuid.toString();
            }
        }
        
        // Try Body.PmtAddRq[0].RqUID
        GenericRecord body = (GenericRecord) message.get("Body");
        if (body != null) {
            @SuppressWarnings("unchecked")
            java.util.List<GenericRecord> pmtAddRq = (java.util.List<GenericRecord>) body.get("PmtAddRq");
            if (pmtAddRq != null && pmtAddRq.size() > 0) {
                GenericRecord firstPaymentRequest = pmtAddRq.get(0);
                Object rqUID = firstPaymentRequest.get("RqUID");
                if (rqUID != null) {
                    return rqUID.toString();
                }
                
                // Try PayHdr.PaymentID
                GenericRecord payHdr = (GenericRecord) firstPaymentRequest.get("PayHdr");
                if (payHdr != null) {
                    Object paymentID = payHdr.get("PaymentID");
                    if (paymentID != null) {
                        return paymentID.toString();
                    }
                }
            }
        }
        
        // Fallback to generated UUID
        return java.util.UUID.randomUUID().toString();
    }

    /**
     * Extract transaction type from processing context
     */
    private static String extractTransactionType(GenericRecord message) {
        GenericRecord body = (GenericRecord) message.get("Body");
        if (body == null) {
            return "UNKNOWN";
        }
        GenericRecord procctxt = (GenericRecord) body.get("Procctxt");
        if (procctxt != null) {
            GenericRecord pmtDtls = (GenericRecord) procctxt.get("PmtDtls");
            if (pmtDtls != null) {
                Object pmtCtgry = pmtDtls.get("PmtCtgry");
                if (pmtCtgry != null) {
                    String category = pmtCtgry.toString();
                    
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
            }
        }
        return "UNKNOWN";
    }

    /**
     * Determine priority based on amount and other factors
     */
    private static String determinePriority(GenericRecord message) {
        try {
            GenericRecord body = (GenericRecord) message.get("Body");
            if (body != null) {
                @SuppressWarnings("unchecked")
                java.util.List<GenericRecord> pmtAddRq = (java.util.List<GenericRecord>) body.get("PmtAddRq");
                if (pmtAddRq != null && pmtAddRq.size() > 0) {
                
                    GenericRecord firstPaymentRequest = pmtAddRq.get(0);
                    GenericRecord fromAcct = (GenericRecord) firstPaymentRequest.get("FromAcct");
                    if (fromAcct != null) {
                        Double amountDouble = (Double) fromAcct.get("Amount");
                        if (amountDouble != null) {
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
    private static LocalDateTime extractTimestamp(GenericRecord message) {
        try {
            // Try Header.EventInfo.EventTS first
            GenericRecord header = (GenericRecord) message.get("Header");
            if (header != null) {
                GenericRecord eventInfo = (GenericRecord) header.get("EventInfo");
                if (eventInfo != null) {
                    Object eventTS = eventInfo.get("EventTS");
                    if (eventTS != null) {
                        String eventTSStr = eventTS.toString();
                        return LocalDateTime.parse(eventTSStr, TIMESTAMP_FORMATTER);
                    }
                }
            }
            
            // Try Header.RcvdTS
            if (header != null) {
                Object rcvdTS = header.get("RcvdTS");
                if (rcvdTS != null) {
                    String rcvdTSStr = rcvdTS.toString();
                    return LocalDateTime.parse(rcvdTSStr, TIMESTAMP_FORMATTER);
                }
            }
            
            // Try Body.PmtAddRq[0].MsgHdr.ClientDt
            GenericRecord body = (GenericRecord) message.get("Body");
            if (body != null) {
                @SuppressWarnings("unchecked")
                java.util.List<GenericRecord> pmtAddRq = (java.util.List<GenericRecord>) body.get("PmtAddRq");
                if (pmtAddRq != null && pmtAddRq.size() > 0) {
                    GenericRecord firstPaymentRequest = pmtAddRq.get(0);
                    GenericRecord msgHdr = (GenericRecord) firstPaymentRequest.get("MsgHdr");
                    if (msgHdr != null) {
                        Object clientDt = msgHdr.get("ClientDt");
                        if (clientDt != null) {
                            String clientDtStr = clientDt.toString();
                            return LocalDateTime.parse(clientDtStr, TIMESTAMP_FORMATTER);
                        }
                    }
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
    private static String extractDescription(GenericRecord message) {
        try {
            GenericRecord body = (GenericRecord) message.get("Body");
            if (body != null) {
                @SuppressWarnings("unchecked")
                java.util.List<GenericRecord> pmtAddRq = (java.util.List<GenericRecord>) body.get("PmtAddRq");
                if (pmtAddRq != null && pmtAddRq.size() > 0) {
                    GenericRecord firstPaymentRequest = pmtAddRq.get(0);
                    
                    // Try FromAcct.Narrative first
                    GenericRecord fromAcct = (GenericRecord) firstPaymentRequest.get("FromAcct");
                    if (fromAcct != null) {
                        Object narrative = fromAcct.get("Narrative");
                        if (narrative != null) {
                            return narrative.toString();
                        }
                    }
                    
                    // Try ToAcct.Narrative
                    GenericRecord toAcct = (GenericRecord) firstPaymentRequest.get("ToAcct");
                    if (toAcct != null) {
                        Object narrative = toAcct.get("Narrative");
                        if (narrative != null) {
                            return narrative.toString();
                        }
                    }
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
    private static String extractReference(GenericRecord message) {
        try {
            GenericRecord body = (GenericRecord) message.get("Body");
            if (body != null) {
                @SuppressWarnings("unchecked")
                java.util.List<GenericRecord> pmtAddRq = (java.util.List<GenericRecord>) body.get("PmtAddRq");
                if (pmtAddRq != null && pmtAddRq.size() > 0) {
                
                    GenericRecord firstPaymentRequest = pmtAddRq.get(0);
                    
                    // Try PayHdr.ThirdPartyPayID first
                    GenericRecord payHdr = (GenericRecord) firstPaymentRequest.get("PayHdr");
                    if (payHdr != null) {
                        Object thirdPartyPayID = payHdr.get("ThirdPartyPayID");
                        if (thirdPartyPayID != null) {
                            return thirdPartyPayID.toString();
                        }
                        
                        // Try PayHdr.PaymentID
                        Object paymentID = payHdr.get("PaymentID");
                        if (paymentID != null) {
                            return paymentID.toString();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Could not extract reference", e);
        }
        
        return null;
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
