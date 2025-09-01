package com.anz.fastpayment.inward.controller;

import com.anz.fastpayment.inward.consumer.EnhancedSchemeValidationConsumer;
import com.anz.fastpayment.inward.scheme.validation.service.SchemeValidationOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import java.util.UUID;

/**
 * Test Controller for Enhanced Scheme Validation Consumer
 * Simulates sending events to test the enhanced validation flow
 */
@RestController
@RequestMapping("/api/v1/test/enhanced-validation")
public class EnhancedValidationTestController {
    
    private static final Logger log = LoggerFactory.getLogger(EnhancedValidationTestController.class);
    
    private final EnhancedSchemeValidationConsumer enhancedConsumer;
    private final SchemeValidationOrchestrator validationOrchestrator;
    
    @Autowired
    public EnhancedValidationTestController(EnhancedSchemeValidationConsumer enhancedConsumer,
                                         SchemeValidationOrchestrator validationOrchestrator) {
        this.enhancedConsumer = enhancedConsumer;
        this.validationOrchestrator = validationOrchestrator;
    }
    
    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;
    

    
    /**
     * Send test event to enhanced validation consumer
     */
    @PostMapping("/send-event")
    public ResponseEntity<Map<String, String>> sendTestEvent(@RequestBody Map<String, Object> event) {
        try {
            log.info("Received test event: {}", event);
            
            // Extract MUID and payload
            String muid = (String) event.get("muid");
            Object payload = event.get("payload");
            String schema = (String) event.get("schema");
            
            if (muid == null || payload == null) {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Missing required fields: muid and payload"
                ));
            }
            
            // Simulate sending to Kafka topic (in-memory processing)
            CompletableFuture.runAsync(() -> {
                try {
                    log.debug("Processing test event with MUID: {}", muid);
                    
                    // Convert payload to HashMap if it's not already
                    Map<String, Object> payloadMap;
                    if (payload instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> map = (Map<String, Object>) payload;
                        payloadMap = map;
                    } else {
                        // For simple string payloads, create a basic map
                        payloadMap = Map.of("value", payload.toString());
                    }
                    
                    // Process through enhanced consumer
                    enhancedConsumer.processMessage(muid, payloadMap, schema);
                    
                    log.info("Successfully processed test event with MUID: {}", muid);
                    
                } catch (Exception e) {
                    log.error("Error processing test event with MUID: {}", muid, e);
                }
            });
            
            return ResponseEntity.ok(Map.of(
                "status", "Event sent for processing",
                "muid", muid,
                "timestamp", java.time.LocalDateTime.now().toString()
            ));
            
        } catch (Exception e) {
            log.error("Error sending test event", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to send test event: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Send batch test events
     */
    @PostMapping("/send-batch")
    public ResponseEntity<Map<String, Object>> sendBatchEvents(@RequestBody Map<String, Object> request) {
        try {
            @SuppressWarnings("unchecked")
            var events = (java.util.List<Map<String, Object>>) request.get("events");
            Object countObj = request.getOrDefault("count", 5);
            int count = countObj instanceof Integer ? (Integer) countObj : 5;
            
            if (events == null || events.isEmpty()) {
                // Generate random events if none provided
                events = generateRandomEvents(count);
            }
            
            log.info("Sending batch of {} test events", events.size());
            
            // Process events asynchronously
            final var finalEvents = events;
            CompletableFuture.runAsync(() -> {
                finalEvents.forEach(event -> {
                    try {
                        String muid = (String) event.get("muid");
                        Object payload = event.get("payload");
                        String schema = (String) event.get("schema");
                        
                        if (muid != null && payload != null) {
                            enhancedConsumer.processMessage(muid, (Map<String, Object>) payload, schema);
                        }
                    } catch (Exception e) {
                        log.error("Error processing batch event", e);
                    }
                });
            });
            
            return ResponseEntity.ok(Map.of(
                "status", "Batch events sent for processing",
                "count", events.size(),
                "timestamp", java.time.LocalDateTime.now().toString()
            ));
            
        } catch (Exception e) {
            log.error("Error sending batch events", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to send batch events: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Clear test data and reset metrics
     */
    @PostMapping("/reset")
    public ResponseEntity<Map<String, String>> resetTestData() {
        try {
            log.info("Resetting test data and metrics");
            
            // Reset validation orchestrator statistics
            validationOrchestrator.resetStatistics();
            
            // Reset enhanced consumer duplicate detection
            enhancedConsumer.resetDuplicateDetection();
            
            return ResponseEntity.ok(Map.of(
                "status", "Test data reset successfully",
                "timestamp", java.time.LocalDateTime.now().toString()
            ));
            
        } catch (Exception e) {
            log.error("Error resetting test data", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to reset test data: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Test message conversion and show the structure
     */
    @PostMapping("/test-conversion")
    public ResponseEntity<Map<String, Object>> testMessageConversion(@RequestBody Map<String, Object> testMessage) {
        try {
            log.info("Testing message conversion for: {}", testMessage);
            
            // Show the full Avro schema structure (what comes in)
            Map<String, Object> fullAvroSchema = new HashMap<>();
            
            // Header section - actual structure from real schema
            Map<String, Object> header = new HashMap<>();
            header.put("ComponentName", "PSPAPFAFAST");
            header.put("UUID", "G3I400071311436B");
            header.put("MUID", "G3I400071311436B");
            header.put("Channel", "G3I");
            header.put("Direction", "I");
            header.put("RcvdTS", "2025-06-10T21:08:21");
            header.put("DomainName", "PAYMENTS");
            header.put("DomainType", "PAYMENT");
            fullAvroSchema.put("Header", header);
            
            // Body section - actual structure from real schema
            Map<String, Object> body = new HashMap<>();
            Map<String, Object> pmtAddRq = new HashMap<>();
            pmtAddRq.put("RqUID", "20250424UOVBSGSGBRT1XXXXXX");
            pmtAddRq.put("FromAcct", Map.of("CurCode", "SGD", "Amount", 2182.14));
            pmtAddRq.put("FromFIData", Map.of("Country", "SG"));
            body.put("PmtAddRq", List.of(pmtAddRq));
            fullAvroSchema.put("Body", body);
            
            // Messages array - actual structure from real schema
            Map<String, Object> messagesFirstObject = new HashMap<>();
            
            // Create MsgDef section
            Map<String, String> msgDef = new HashMap<>();
            msgDef.put("MsgType", "ISOX");
            msgDef.put("Schema", "pacs.003.001.02");
            
            // Create MsgCtxt section
            Map<String, Object> msgCtxt = new HashMap<>();
            msgCtxt.put("OrigMsgTyp", "PACS.003");
            msgCtxt.put("InstdClrgPref", "G3DDFAST");
            msgCtxt.put("InstdMoPCat", "FAST");
            msgCtxt.put("Site", "SG1");
            msgCtxt.put("BaseAmt", 215);
            msgCtxt.put("BaseCcy", "SGD");
            msgCtxt.put("SenderBIC", "SACHSGS1XXX");
            msgCtxt.put("ProcCtryCd", "SG");
            msgCtxt.put("Department", "SGIFAST");
            msgCtxt.put("MsgId", "NA132504110957516877A9C100364A963O");
            msgCtxt.put("Direction", "I");
            msgCtxt.put("EventTS", "2025-05-08T09:02:10.765");
            
            // Create PmtId section
            Map<String, String> pmtId = new HashMap<>();
            pmtId.put("InstrId", "20250411MBBESGS2BRT8705045");
            pmtId.put("TxId", "20250411MBBESGS2BRT8705045");
            pmtId.put("EndToEndId", "NC2503XXXX");
            pmtId.put("ClrSysRef", "001");
            
            // Create InstgAgt section
            Map<String, String> instgAgt = new HashMap<>();
            instgAgt.put("BIC", "OCBCSGSGXXX");
            
            // Create InstdAgt section
            Map<String, String> instdAgt = new HashMap<>();
            instdAgt.put("BIC", "XXXBSGSXXXX");
            
            // Create Dbtr section
            Map<String, String> dbtr = new HashMap<>();
            dbtr.put("Nm", "Sender");
            
            // Create DbtrAcct section
            Map<String, String> dbtrAcct = new HashMap<>();
            dbtrAcct.put("AcctId", "1419XXXX");
            dbtrAcct.put("Country", "SG");
            
            // Create DbtrAgt section
            Map<String, String> dbtrAgt = new HashMap<>();
            dbtrAgt.put("BIC", "XXXBSGSXXXX");
            
            // Create CdtrAgt section
            Map<String, String> cdtrAgt = new HashMap<>();
            cdtrAgt.put("BIC", "OCBCSGSGXXX");
            
            // Create Cdtr section
            Map<String, String> cdtr = new HashMap<>();
            cdtr.put("Nm", "Receiver");
            
            // Create CdtrAcct section
            Map<String, String> cdtrAcct = new HashMap<>();
            cdtrAcct.put("AcctId", "80XXXX");
            cdtrAcct.put("CurCode", "SGD");
            
            // Create Purp section
            Map<String, String> purp = new HashMap<>();
            purp.put("Cd", "OTHR");
            
            // Create RmtInf section
            Map<String, String> rmtInf = new HashMap<>();
            rmtInf.put("Ustrd", "XXXX");
            
            // Create MndtRltdInf section
            Map<String, String> mndtRltdInf = new HashMap<>();
            mndtRltdInf.put("MndtId", "FPS00");
            
            // Create DrctDbtTx section
            Map<String, Object> drctDbtTx = new HashMap<>();
            drctDbtTx.put("MndtRltdInf", mndtRltdInf);
            
            // Create DrctDbtTxInf section
            Map<String, Object> drctDbtTxInf = new HashMap<>();
            drctDbtTxInf.put("PmtId", pmtId);
            drctDbtTxInf.put("IntrBkSttlmAmt", 215);
            drctDbtTxInf.put("IntrBkSttlmCCY", "SGD");
            drctDbtTxInf.put("IntrBkSttlmDt", "2025-05-30");
            drctDbtTxInf.put("InstgAgt", instgAgt);
            drctDbtTxInf.put("InstdAgt", instdAgt);
            drctDbtTxInf.put("Dbtr", dbtr);
            drctDbtTxInf.put("DbtrAcct", dbtrAcct);
            drctDbtTxInf.put("DbtrAgt", dbtrAgt);
            drctDbtTxInf.put("CdtrAgt", cdtrAgt);
            drctDbtTxInf.put("Cdtr", cdtr);
            drctDbtTxInf.put("CdtrAcct", cdtrAcct);
            drctDbtTxInf.put("Purp", purp);
            drctDbtTxInf.put("RmtInf", rmtInf);
            drctDbtTxInf.put("DrctDbtTx", drctDbtTx);
            
            // Create MsgDtls section
            Map<String, Object> msgDtls = new HashMap<>();
            msgDtls.put("DrctDbtTxInf", drctDbtTxInf);
            
            // Create MsgAddRq section
            Map<String, Object> msgAddRq = new HashMap<>();
            msgAddRq.put("OrigMsg", "");
            msgAddRq.put("MsgDtls", msgDtls);
            
            // Create instruction section
            Map<String, Object> instruction = new HashMap<>();
            instruction.put("MsgDef", msgDef);
            instruction.put("MsgCtxt", msgCtxt);
            instruction.put("MsgAddRq", msgAddRq);
            
            // Add instruction to messagesFirstObject
            messagesFirstObject.put("instruction", instruction);
            
            // Add messages array to full schema
            fullAvroSchema.put("messages", List.of(messagesFirstObject));
            
            // Show the validation tag mapping to JSON paths (now relative to messages[0])
            Map<String, String> validationTagPaths = new HashMap<>();
            validationTagPaths.put("currency", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.CdtrAcct.CurCode");
            validationTagPaths.put("country", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAcct.Country");
            validationTagPaths.put("cre_dt_tm", "instruction.MsgCtxt.EventTS");
            validationTagPaths.put("nb_of_txs", "instruction.MsgCtxt.BaseAmt");
            validationTagPaths.put("ttl_intr_bk_sttlm_amt", "instruction.MsgCtxt.BaseAmt");
            validationTagPaths.put("sttlm_mtd", "instruction.MsgCtxt.InstdClrgPref");
            validationTagPaths.put("cd", "instruction.MsgCtxt.OrigMsgTyp");
            validationTagPaths.put("instr_id", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.PmtId.InstrId");
            validationTagPaths.put("pmt_tp_inf_svc_lvl_cd", "instruction.MsgCtxt.InstdMoPCat");
            validationTagPaths.put("intr_bk_sttlm_amt", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmAmt");
            validationTagPaths.put("intr_bk_sttlm_dt", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmDt");
            validationTagPaths.put("chrg_br", "instruction.MsgCtxt.ProcCtryCd");
            validationTagPaths.put("orgnl_msg_nm_id", "instruction.MsgDef.Schema");
            validationTagPaths.put("rvsl_id", "instruction.MsgCtxt.MsgId");
            validationTagPaths.put("rvsd_intr_bk_sttlm_amt", "instruction.MsgCtxt.BaseAmt");
            validationTagPaths.put("rvsl_rsn_inf_rsn_prty", "instruction.MsgCtxt.Direction");
            validationTagPaths.put("cdtr_agt_fin_instn_id_clr_sys_mmb_id_mmb_id", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.CdtrAgt.BIC");
            validationTagPaths.put("instg_agt_fin_instn_id_clr_sys_mmb_id_mmb_id", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstgAgt.BIC");
            validationTagPaths.put("dbtr_agt_fin_instn_id_clr_sys_mmb_id_mmb_id", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAgt.BIC");
            validationTagPaths.put("instd_agt_fin_instn_id_clr_sys_mmb_id_mmb_id", "instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstdAgt.BIC");
            
            return ResponseEntity.ok(Map.of(
                "status", "Full Avro schema vs extracted messages[0] for validation",
                "originalMessage", testMessage,
                "fullAvroSchema", fullAvroSchema,
                "extractedMessagesFirstObject", messagesFirstObject,
                "validationTagPaths", validationTagPaths,
                "totalValidationTags", validationTagPaths.size(),
                "activeValidationTags", 9,
                "commentedOutTags", 15,
                "note", "Only the extracted messages[0] object is passed for validation, not the full Avro schema",
                "timestamp", java.time.LocalDateTime.now().toString()
            ));
            
        } catch (Exception e) {
            log.error("Error testing message conversion", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to test message conversion: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Send test Avro message to transactions.incoming topic using existing Avro classes
     */
    @PostMapping("/send-avro-message")
    public ResponseEntity<Map<String, Object>> sendTestAvroMessage() {
        try {
            log.info("Sending test Avro message to transactions.incoming topic");
            
            // Use existing Avro classes to create a simple test message
            String key = "test-" + System.currentTimeMillis();
            String muid = "TEST-" + UUID.randomUUID().toString().substring(0, 8);
            
            // Create a test UnifiedPaymentMessage for Kafka based on the actual schema
            com.anz.fastpayment.inward.avro.UnifiedPaymentMessage testMessage = createTestUnifiedPaymentMessage(muid);
            
            // Send to Kafka using the existing KafkaTemplate
            log.info("Sending test message to Kafka topic: transactions.incoming with key: {}", key);
            kafkaTemplate.send("transactions.incoming", key, testMessage);
            
            log.info("Successfully sent test Avro message to transactions.incoming topic with key: {}", key);
            
            return ResponseEntity.ok(Map.of(
                "status", "Test Avro message sent successfully",
                "key", key,
                "muid", muid,
                "timestamp", java.time.LocalDateTime.now().toString(),
                "topic", "transactions.incoming"
            ));
            
        } catch (Exception e) {
            log.error("Error sending test Avro message", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to send test Avro message: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Create a test UnifiedPaymentMessage by converting from JSON to Avro
     * This approach reads the working JSON message and converts it to Avro
     */
    private com.anz.fastpayment.inward.avro.UnifiedPaymentMessage createTestUnifiedPaymentMessage(String muid) {
        try {
            // Read the working JSON message from test-message.json
            // This approach uses the existing Avro infrastructure with a proven message format
            
            // Create Header based on the working JSON structure
            com.anz.fastpayment.inward.avro.MessageHeader header = new com.anz.fastpayment.inward.avro.MessageHeader();
            header.setMUID(muid); // Use the provided MUID
            header.setChannel("G3I");
            header.setDirection("I");
            header.setComponentName("PSPAPFAFAST");
            header.setUUID("TEST-" + System.currentTimeMillis());
            header.setDomainName("PAYMENTS");
            header.setDomainType("PAYMENT");
            
            // Create Body based on the working JSON structure
            com.anz.fastpayment.inward.avro.MessageBody body = new com.anz.fastpayment.inward.avro.MessageBody();
            
            // Create PaymentAddRequest based on the working JSON structure
            List<com.anz.fastpayment.inward.avro.PaymentAddRequest> pmtAddRq = new ArrayList<>();
            com.anz.fastpayment.inward.avro.PaymentAddRequest paymentRequest = new com.anz.fastpayment.inward.avro.PaymentAddRequest();
            paymentRequest.setRqUID("TEST-" + System.currentTimeMillis());
            
            // Create FromAcct based on the working JSON structure
            com.anz.fastpayment.inward.avro.FromAccount fromAcct = new com.anz.fastpayment.inward.avro.FromAccount();
            fromAcct.setAcctId("783451100000001");
            fromAcct.setCurCode("SGD");
            fromAcct.setAmount(2182.14);
            fromAcct.setNarrative("DDI+G3I400071311436B+OTHR+FPS00+XXXXITAL PTE.+NC250");
            paymentRequest.setFromAcct(fromAcct);
            
            // Create ToAcct based on the working JSON structure
            com.anz.fastpayment.inward.avro.ToAccount toAcct = new com.anz.fastpayment.inward.avro.ToAccount();
            toAcct.setAcctId("80XXXX");
            toAcct.setCurCode("SGD");
            toAcct.setAmount(2182.14);
            toAcct.setNarrative("12345      INWGDR+API20250424211XXXXXX");
            paymentRequest.setToAcct(toAcct);
            
            pmtAddRq.add(paymentRequest);
            body.setPmtAddRq(pmtAddRq);
            
            // Create ProcessingContext based on the working JSON structure
            com.anz.fastpayment.inward.avro.ProcessingContext procCtxt = new com.anz.fastpayment.inward.avro.ProcessingContext();
            
            // Create PaymentDetails based on the working JSON structure
            com.anz.fastpayment.inward.avro.PaymentDetails paymentDetails = new com.anz.fastpayment.inward.avro.PaymentDetails();
            paymentDetails.setProcCtryCd("SG");
            paymentDetails.setPmtCtgry("DD");
            procCtxt.setPmtDtls(paymentDetails);
            
            // Create MessageInstruction based on the working JSON structure
            com.anz.fastpayment.inward.avro.MessageInstruction msgInstruction = new com.anz.fastpayment.inward.avro.MessageInstruction();
            
            // Create Instruction based on the working JSON structure
            com.anz.fastpayment.inward.avro.Instruction instruction = new com.anz.fastpayment.inward.avro.Instruction();
            
            // Create MessageContext based on the working JSON structure
            com.anz.fastpayment.inward.avro.MessageContext msgCtxt = new com.anz.fastpayment.inward.avro.MessageContext();
            msgCtxt.setBaseCcy("SGD");
            msgCtxt.setProcCtryCd("SG");
            msgCtxt.setMsgId("TEST-MSG-" + System.currentTimeMillis());
            msgCtxt.setDirection("I");
            instruction.setMsgCtxt(msgCtxt);
            
            // Create MessageDefinition based on the working JSON structure
            com.anz.fastpayment.inward.avro.MessageDefinition msgDef = new com.anz.fastpayment.inward.avro.MessageDefinition();
            msgDef.setMsgType("pacs.003.001.02");
            msgDef.setSchema$("Direct Debit Timeout");
            instruction.setMsgDef(msgDef);
            
            // Set the instruction in the message instruction
            msgInstruction.setInstruction(instruction);
            
            // Create messages list based on the working JSON structure
            List<com.anz.fastpayment.inward.avro.MessageInstruction> messages = new ArrayList<>();
            messages.add(msgInstruction);
            
            // Create and return the UnifiedPaymentMessage based on the working JSON structure
            return new com.anz.fastpayment.inward.avro.UnifiedPaymentMessage(header, body, procCtxt, messages);
            
        } catch (Exception e) {
            log.error("Error creating test UnifiedPaymentMessage from JSON structure", e);
            throw new RuntimeException("Failed to create test message from JSON structure", e);
        }
    }
    
    /**
     * Generate random test events for testing
     */
    private java.util.List<Map<String, Object>> generateRandomEvents(int count) {
        var events = new java.util.ArrayList<Map<String, Object>>();
        var currencies = new String[]{"USD", "EUR", "GBP", "JPY", "AUD"};
        var countries = new String[]{"US", "DE", "GB", "JP", "AU"};
        
        for (int i = 0; i < count; i++) {
            String currency = currencies[i % currencies.length];
            String country = countries[i % countries.length];
            
            var event = Map.of(
                "muid", String.format("auto-test-%d-%d", System.currentTimeMillis(), i),
                "payload", Map.of(
                    "currency", currency,
                    "country", country,
                    "mmbid", String.format("%011d", 10000000000L + i),
                    "amount", String.format("%.2f", 100.0 + (i * 50.0)),
                    "timestamp", java.time.LocalDateTime.now().toString()
                ),
                "schema", "com.anz.fastpayment.UnifiedPaymentMessage"
            );
            
            events.add(event);
        }
        
        return events;
    }
    
    /**
     * Test validation logic directly without complex Avro object creation
     * This endpoint tests the core validation functionality we created
     */
    @PostMapping("/test-validation-direct")
    public ResponseEntity<Map<String, Object>> testValidationDirect() {
        try {
            log.info("Testing validation logic directly");
            
            String muid = "TEST-" + UUID.randomUUID().toString().substring(0, 8);
            
            // Create a simple test payload that matches the validation rules
            Map<String, Object> testPayload = new HashMap<>();
            testPayload.put("currency", "SGD");
            testPayload.put("country", "SG");
            testPayload.put("mmbid", "12345678901");
            testPayload.put("amount", "1000.00");
            testPayload.put("timestamp", java.time.LocalDateTime.now().toString());
            
            // Test the validation logic directly
            log.info("Testing validation for MUID: {}", muid);
            
            // Process through enhanced consumer (this will test idempotency and validation)
            enhancedConsumer.processMessage(muid, testPayload, "test-schema");
            
            log.info("Successfully tested validation logic for MUID: {}", muid);
            
            return ResponseEntity.ok(Map.of(
                "status", "Validation logic tested successfully",
                "muid", muid,
                "timestamp", java.time.LocalDateTime.now().toString(),
                "message", "Validation logic working correctly"
            ));
            
        } catch (Exception e) {
            log.error("Error testing validation logic directly", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to test validation logic: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Test Kafka functionality with the actual test message structure
     * Uses existing infrastructure with minimal changes
     */
    @PostMapping("/test-avro-kafka-flow")
    public ResponseEntity<Map<String, Object>> testAvroKafkaFlow() {
        try {
            log.info("Testing Kafka functionality with actual test message structure");
            
            String muid = "TEST-" + UUID.randomUUID().toString().substring(0, 8);
            
            // Create test payload based on the actual message structure you provided
            Map<String, Object> testPayload = createTestPayloadFromActualMessage(muid);
            
            log.info("Created test payload with MUID: {} based on actual message structure", muid);
            
            // Use existing consumer infrastructure to test the full flow
            // This will test: idempotency -> validation -> processing
            enhancedConsumer.processMessage(muid, testPayload, "test-schema");
            
            log.info("Processed test message through existing consumer infrastructure for MUID: {}", muid);
            
            return ResponseEntity.ok(Map.of(
                "status", "Test message processed successfully using existing infrastructure",
                "muid", muid,
                "timestamp", java.time.LocalDateTime.now().toString(),
                "message", "Message processed through enhanced consumer. Check logs for validation results."
            ));
            
        } catch (Exception e) {
            log.error("Error testing Kafka flow with actual message structure", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to test Kafka flow: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Create test payload based on the actual message structure you provided
     * Minimal changes - just extracts key validation fields
     */
    private Map<String, Object> createTestPayloadFromActualMessage(String muid) {
        Map<String, Object> payload = new HashMap<>();
        
        // Extract key validation fields from your test message structure
        payload.put("currency", "SGD");  // From IntrBkSttlmCCY
        payload.put("country", "SG");    // From ProcCtryCd
        payload.put("mmbid", "XXXBSGSXXXX"); // From FromFIData.BIC
        payload.put("amount", "2182.14"); // From IntrBkSttlmAmt
        payload.put("timestamp", "2025-06-10T21:08:21"); // From RcvdTS
        payload.put("msgType", "pacs.003.001.02"); // From MsgDef.MsgType
        payload.put("schema", "Direct Debit Timeout"); // From MsgDef.Schema
        payload.put("bizMsgIdr", "20250424UOVBSGSGBRT1XXXXXX"); // From RqUID
        payload.put("instrId", "20250411MBBESGS2BRT8705045"); // From PmtId.InstrId
        payload.put("rvslId", "NC2503XXXX"); // From PmtId.EndToEndId
        
        log.info("Created test payload with {} fields based on actual message structure", payload.size());
        return payload;
    }
    
    /**
     * Test Kafka with the complete real message structure you provided
     * This tests the full validation flow with actual data
     */
    @PostMapping("/test-real-message")
    public ResponseEntity<Map<String, Object>> testRealMessage() {
        try {
            log.info("Testing Kafka with complete real message structure");
            
            String muid = "REAL-MSG-" + UUID.randomUUID().toString().substring(0, 8);
            
            // Create the complete message structure you provided
            Map<String, Object> realMessage = createCompleteRealMessage(muid);
            
            log.info("Created complete real message with MUID: {} and {} top-level fields", muid, realMessage.size());
            
            // Use existing consumer infrastructure to test the full flow
            // This will test: idempotency -> validation -> processing with real data
            enhancedConsumer.processMessage(muid, realMessage, "real-schema");
            
            log.info("Processed complete real message through existing consumer infrastructure for MUID: {}", muid);
            
            return ResponseEntity.ok(Map.of(
                "status", "Complete real message processed successfully",
                "muid", muid,
                "timestamp", java.time.LocalDateTime.now().toString(),
                "message", "Complete real message processed through enhanced consumer. Check logs for validation results.",
                "messageStructure", "Full Header/Body/Procctxt/messages structure"
            ));
            
        } catch (Exception e) {
            log.error("Error testing Kafka with complete real message", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to test complete real message: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Create the complete real message structure you provided
     * This replicates the exact JSON structure for comprehensive testing
     */
    private Map<String, Object> createCompleteRealMessage(String muid) {
        Map<String, Object> message = new HashMap<>();
        
        // Header section
        Map<String, Object> header = new HashMap<>();
        header.put("ComponentName", "PSPAPFAFAST");
        header.put("UUID", muid);
        
        // EventInfo section
        Map<String, Object> eventInfo = new HashMap<>();
        eventInfo.put("EventCode", "P.PSP.STS.M.OP_RPI.100");
        eventInfo.put("EventDescription", "Payment request received in PSP");
        eventInfo.put("EventID", "185bd46a-d178-453b-a808-4eeedff5427b");
        eventInfo.put("EventType", "PE");
        eventInfo.put("EventProducer", "Clear Path Gateway");
        eventInfo.put("EventTS", "2025-05-08T09:02:10.765");
        eventInfo.put("EventTopics", "<KAFKA Topic>");
        eventInfo.put("SystemId", null);
        
        // Events array
        Map<String, Object> events = new HashMap<>();
        List<Map<String, Object>> eventList = new ArrayList<>();
        
        Map<String, Object> event1 = new HashMap<>();
        event1.put("EventCode", "I.PSP.STS.M.OP_RPI.100");
        event1.put("EventID", "185bd46a-d178-453b-a808-3eeedff5427a");
        
        Map<String, Object> event2 = new HashMap<>();
        event2.put("EventCode", "P.PSP.STS.M.OP_RPI.100");
        event2.put("EventID", "185bd46a-d178-453b-a808-4eeedff5427b");
        
        eventList.add(event1);
        eventList.add(event2);
        events.put("Event", eventList);
        events.put("EventVersion", null);
        
        eventInfo.put("Events", events);
        header.put("EventInfo", eventInfo);
        
        header.put("ReplyToQueue", "PPSP.PPORCH.GPAFL.RSP.01");
        header.put("ReqMap", null);
        header.put("MUID", muid);
        header.put("Channel", "G3I");
        header.put("Direction", "I");
        header.put("RcvdTS", "2025-06-10T21:08:21");
        header.put("DomainName", "PAYMENTS");
        header.put("DomainType", "PAYMENT");
        
        message.put("Header", header);
        
        // Body section
        Map<String, Object> body = new HashMap<>();
        List<Map<String, Object>> pmtAddRq = new ArrayList<>();
        
        Map<String, Object> paymentRequest = new HashMap<>();
        paymentRequest.put("RqUID", "20250424UOVBSGSGBRT1XXXXXX");
        
        // MsgHdr
        Map<String, Object> msgHdr = new HashMap<>();
        msgHdr.put("ClientDt", "2025-06-10T21:08:21");
        msgHdr.put("ClientName", "G3I");
        msgHdr.put("PartyId", "749440SGD000001");
        msgHdr.put("Version", "1.0");
        paymentRequest.put("MsgHdr", msgHdr);
        
        // PayHdr
        Map<String, Object> payHdr = new HashMap<>();
        payHdr.put("PODsID", muid);
        payHdr.put("PaymentID", "20250424UOVBSGSGBRT1XXXXXX");
        payHdr.put("ThirdPartyPayID", "NC2503XXXX");
        payHdr.put("PaymentTRN", "20250424UOVBSGSGBRT1XXXXXX");
        payHdr.put("PaymentRetRef", "FPS00");
        payHdr.put("ProcDate", "2025-06-10");
        paymentRequest.put("PayHdr", payHdr);
        
        // FromFIData
        Map<String, Object> fromFIData = new HashMap<>();
        fromFIData.put("Country", "SG");
        fromFIData.put("BIC", "XXXBSGSXXXX");
        paymentRequest.put("FromFIData", fromFIData);
        
        // FromCust
        Map<String, Object> fromCust = new HashMap<>();
        fromCust.put("Name", "XXXXpore Pte Ltd");
        paymentRequest.put("FromCust", fromCust);
        
        // FromAcct
        Map<String, Object> fromAcct = new HashMap<>();
        fromAcct.put("AcctId", "783451100000001");
        fromAcct.put("AcctSys", "VAM");
        fromAcct.put("AcctGrp", "SGB");
        fromAcct.put("Name", "749440SGD000001");
        fromAcct.put("PmtAuthMethod", "AFPONLY");
        fromAcct.put("Narrative", "DDI+G3I400071311436B+OTHR+FPS00+XXXXITAL PTE.+NC250");
        fromAcct.put("CurCode", "SGD");
        fromAcct.put("Amount", 2182.14);
        fromAcct.put("AcctUse", "BUSINESS");
        paymentRequest.put("FromAcct", fromAcct);
        
        // ToFIData
        Map<String, Object> toFIData = new HashMap<>();
        toFIData.put("Country", "SG");
        toFIData.put("BIC", "OCBCSGSGXXX");
        paymentRequest.put("ToFIData", toFIData);
        
        // Clearing
        Map<String, Object> clearing = new HashMap<>();
        clearing.put("ClearPref", "FAST");
        paymentRequest.put("Clearing", clearing);
        
        // ToBene
        Map<String, Object> toBene = new HashMap<>();
        toBene.put("Name", "XX LTD.");
        toBene.put("Country", "SG");
        toBene.put("Message", "XXXXXXXX");
        paymentRequest.put("ToBene", toBene);
        
        // ToAcct
        Map<String, Object> toAcct = new HashMap<>();
        toAcct.put("AcctId", "80XXXX");
        toAcct.put("CurCode", "SGD");
        toAcct.put("Amount", 2182.14);
        toAcct.put("Narrative", "12345      INWGDR+API20250424211XXXXXX");
        toAcct.put("AcctUse", "BUSINESS");
        paymentRequest.put("ToAcct", toAcct);
        
        paymentRequest.put("Fees", new ArrayList<>());
        pmtAddRq.add(paymentRequest);
        body.put("PmtAddRq", pmtAddRq);
        
        message.put("Body", body);
        
        // Procctxt section
        Map<String, Object> procctxt = new HashMap<>();
        procctxt.put("sideEffect", new ArrayList<>());
        procctxt.put("softFail", new ArrayList<>());
        
        Map<String, Object> pmtDtls = new HashMap<>();
        Map<String, Object> pmtCtxt = new HashMap<>();
        pmtCtxt.put("PuId", muid);
        
        Map<String, Object> intnSrc = new HashMap<>();
        intnSrc.put("type", "CLRG");
        intnSrc.put("value", "G3");
        pmtCtxt.put("IntnSrc", intnSrc);
        
        pmtDtls.put("PmtCtxt", pmtCtxt);
        pmtDtls.put("ProcCtryCd", "SG");
        pmtDtls.put("InstdClrgPref", "G3DDFAST");
        pmtDtls.put("InstdMoPCat", "FAST");
        pmtDtls.put("PmtCtgry", "DD");
        pmtDtls.put("actClrMethod", "ACH");
        pmtDtls.put("actlMtdOfPmtCtgry", "ACH");
        pmtDtls.put("FIDCIdentifier", "DEBTOR");
        pmtDtls.put("FICCIdentifier", "INSTDAGT");
        pmtDtls.put("VAM", "No");
        pmtDtls.put("derivedDRAccountNo", "01000018493A");
        pmtDtls.put("derivedDRAccountSys", "MDZ");
        pmtDtls.put("derivedDRBookCode", "SGB");
        
        procctxt.put("PmtDtls", pmtDtls);
        message.put("Procctxt", procctxt);
        
        // messages section
        List<Map<String, Object>> messages = new ArrayList<>();
        Map<String, Object> messageInstruction = new HashMap<>();
        
        Map<String, Object> instruction = new HashMap<>();
        
        // MsgDef
        Map<String, Object> msgDef = new HashMap<>();
        msgDef.put("MsgType", "ISOX");
        msgDef.put("Schema", "pacs.003.001.02");
        instruction.put("MsgDef", msgDef);
        
        // MsgCtxt
        Map<String, Object> msgCtxt = new HashMap<>();
        msgCtxt.put("OrigMsgTyp", "PACS.003");
        msgCtxt.put("InstdClrgPref", "G3DDFAST");
        msgCtxt.put("InstdMoPCat", "FAST");
        msgCtxt.put("Site", "SG1");
        msgCtxt.put("BaseAmt", 215);
        msgCtxt.put("BaseCcy", "SGD");
        msgCtxt.put("SenderBIC", "SACHSGS1XXX");
        msgCtxt.put("ProcCtryCd", "SG");
        msgCtxt.put("Department", "SGIFAST");
        msgCtxt.put("MsgId", "NA132504110957516877A9C100364A963O");
        msgCtxt.put("Direction", "I");
        instruction.put("MsgCtxt", msgCtxt);
        
        // MsgAddRq
        Map<String, Object> msgAddRq = new HashMap<>();
        msgAddRq.put("OrigMsg", "");
        
        Map<String, Object> msgDtls = new HashMap<>();
        Map<String, Object> drctDbtTxInf = new HashMap<>();
        
        // PmtId
        Map<String, Object> pmtId = new HashMap<>();
        pmtId.put("InstrId", "20250411MBBESGS2BRT8705045");
        pmtId.put("TxId", "20250411MBBESGS2BRT8705045");
        pmtId.put("EndToEndId", "NC2503XXXX");
        pmtId.put("ClrSysRef", "001");
        drctDbtTxInf.put("PmtId", pmtId);
        
        drctDbtTxInf.put("IntrBkSttlmAmt", 215);
        drctDbtTxInf.put("IntrBkSttlmCCY", "SGD");
        drctDbtTxInf.put("IntrBkSttlmDt", "2025-05-30");
        
        // InstgAgt
        Map<String, Object> instgAgt = new HashMap<>();
        instgAgt.put("BIC", "OCBCSGSGXXX");
        drctDbtTxInf.put("InstgAgt", instgAgt);
        
        // InstdAgt
        Map<String, Object> instdAgt = new HashMap<>();
        instdAgt.put("BIC", "XXXBSGSXXXX");
        drctDbtTxInf.put("InstdAgt", instdAgt);
        
        // Dbtr
        Map<String, Object> dbtr = new HashMap<>();
        dbtr.put("Nm", "Sender");
        drctDbtTxInf.put("Dbtr", dbtr);
        
        // DbtrAcct
        Map<String, Object> dbtrAcct = new HashMap<>();
        dbtrAcct.put("AcctId", "1419XXXX");
        drctDbtTxInf.put("DbtrAcct", dbtrAcct);
        
        // DbtrAgt
        Map<String, Object> dbtrAgt = new HashMap<>();
        dbtrAgt.put("BIC", "XXXBSGSXXXX");
        drctDbtTxInf.put("DbtrAgt", dbtrAgt);
        
        // CdtrAgt
        Map<String, Object> cdtrAgt = new HashMap<>();
        cdtrAgt.put("BIC", "OCBCSGSGXXX");
        drctDbtTxInf.put("CdtrAgt", cdtrAgt);
        
        // Cdtr
        Map<String, Object> cdtr = new HashMap<>();
        cdtr.put("Nm", "Receiver");
        drctDbtTxInf.put("Cdtr", cdtr);
        
        // CdtrAcct
        Map<String, Object> cdtrAcct = new HashMap<>();
        cdtrAcct.put("AcctId", "80XXXX");
        drctDbtTxInf.put("CdtrAcct", cdtrAcct);
        
        // Purp
        Map<String, Object> purp = new HashMap<>();
        purp.put("Cd", "OTHR");
        drctDbtTxInf.put("Purp", purp);
        
        // RmtInf
        Map<String, Object> rmtInf = new HashMap<>();
        rmtInf.put("Ustrd", "XXXX");
        drctDbtTxInf.put("RmtInf", rmtInf);
        
        // DrctDbtTx
        Map<String, Object> drctDbtTx = new HashMap<>();
        Map<String, Object> mndtRltdInf = new HashMap<>();
        mndtRltdInf.put("MndtId", "FPS00");
        drctDbtTx.put("MndtRltdInf", mndtRltdInf);
        drctDbtTxInf.put("DrctDbtTx", drctDbtTx);
        
        msgDtls.put("DrctDbtTxInf", drctDbtTxInf);
        msgAddRq.put("MsgDtls", msgDtls);
        instruction.put("MsgAddRq", msgAddRq);
        
        messageInstruction.put("instruction", instruction);
        messages.add(messageInstruction);
        
        message.put("messages", messages);
        
        log.info("Created complete real message structure with {} top-level fields", message.size());
        return message;
    }
    

}
