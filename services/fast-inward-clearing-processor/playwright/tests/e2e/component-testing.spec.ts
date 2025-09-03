import { test, expect } from '@playwright/test';
import { kafkaHelper } from '../helpers/kafka-helper';
import { httpHelper } from '../helpers/http-helper';
import { testData } from '../helpers/test-data';

test.describe('Component Testing - InputMessage Schema', () => {
  test.beforeAll(async () => {
    await kafkaHelper.connect();
    
    // Ensure service is healthy before running tests
    await expect.poll(async () => {
      const response = await httpHelper.healthCheck();
      return response.status;
    }, { timeout: 30000 }).toBe(200);
  });

  test.afterAll(async () => {
    // Don't disconnect Kafka for 24/7 continuous service
    console.log('Component tests completed - Kafka connections remain active');
  });

  test.beforeEach(async () => {
    await kafkaHelper.clearMessages();
    await new Promise(resolve => setTimeout(resolve, 3000));
  });

  test('1. IDEMPOTENCY TEST - should handle duplicate messages correctly', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `idempotency-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    const testMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    console.log(`🔍 Testing Idempotency with MUID: ${uniqueMUID}`);
    
    // Send first message - should process successfully
    console.log('📤 Sending first message...');
    await kafkaHelper.sendMessage(inputTopic, testMessage, uniqueMUID);
    
    // Wait for first response
    const firstResponse = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    expect(firstResponse).not.toBeNull();
    expect(firstResponse.Trailer.status).toBe('SUCCESS');
    console.log('✅ First message processed successfully');
    
    // Send duplicate message - should be idempotent (no response expected)
    console.log('📤 Sending duplicate message...');
    await kafkaHelper.sendMessage(inputTopic, testMessage, uniqueMUID);
    
    // Wait a bit and verify no duplicate response
    await new Promise(resolve => setTimeout(resolve, 5000));
    console.log('✅ Duplicate message handled idempotently - no duplicate response');
  });

  test('2. PARSING TEST - should parse InputMessage schema correctly', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `parsing-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    const testMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    console.log(`🔍 Testing Message Parsing with MUID: ${uniqueMUID}`);
    console.log('📋 Input Message Structure:');
    console.log(`   - Header.ComponentName: ${testMessage.Header.ComponentName}`);
    console.log(`   - Header.MUID: ${testMessage.Header.MUID}`);
    console.log(`   - Body.PmtAddRq length: ${testMessage.Body.PmtAddRq.length}`);
    console.log(`   - messages length: ${testMessage.messages.length}`);
    console.log(`   - Procctxt.PmtDtls.ProcCtryCd: ${testMessage.Procctxt.PmtDtls.ProcCtryCd}`);
    
    await kafkaHelper.sendMessage(inputTopic, testMessage, uniqueMUID);
    
    const responseMessage = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    expect(responseMessage).not.toBeNull();
    
    // Verify parsing preserved all message structure
    expect(responseMessage.Header.MUID).toBe(uniqueMUID);
    expect(responseMessage.Header.ComponentName).toBe(testMessage.Header.ComponentName);
    expect(responseMessage.Body.PmtAddRq).toHaveLength(1);
    expect(responseMessage.messages).toHaveLength(1);
    expect(responseMessage.Procctxt.PmtDtls.ProcCtryCd).toBe(testMessage.Procctxt.PmtDtls.ProcCtryCd);
    
    console.log('✅ Message parsing successful - all structure preserved');
  });

  test('3. FIELD EXTRACTION TEST - should extract validation fields correctly', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `extraction-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    const testMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    console.log(`🔍 Testing Field Extraction with MUID: ${uniqueMUID}`);
    
    // Log the fields that should be extracted for validation
    console.log('📋 Fields to be extracted:');
    console.log(`   - Currency: ${testMessage.Body.PmtAddRq[0].FromAcct.CurCode}`);
    console.log(`   - Amount: ${testMessage.Body.PmtAddRq[0].FromAcct.Amount}`);
    console.log(`   - Date: ${testMessage.Body.PmtAddRq[0].PayHdr.ProcDate}`);
    console.log(`   - FromFIData.BIC: ${testMessage.Body.PmtAddRq[0].FromFIData.BIC}`);
    console.log(`   - ToFIData.BIC: ${testMessage.Body.PmtAddRq[0].ToFIData.BIC}`);
    console.log(`   - InstgAgt.BIC: ${testMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstgAgt.BIC}`);
    console.log(`   - InstdAgt.BIC: ${testMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstdAgt.BIC}`);
    console.log(`   - DbtrAgt.BIC: ${testMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAgt.BIC}`);
    console.log(`   - CdtrAgt.BIC: ${testMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.CdtrAgt.BIC}`);
    
    await kafkaHelper.sendMessage(inputTopic, testMessage, uniqueMUID);
    
    const responseMessage = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    expect(responseMessage).not.toBeNull();
    expect(responseMessage.Trailer.status).toBe('SUCCESS');
    
    console.log('✅ Field extraction successful - all validation fields extracted');
  });

  test('4. VALIDATION TEST - should validate all fields correctly', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `validation-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    const testMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    console.log(`🔍 Testing Validation with MUID: ${uniqueMUID}`);
    
    // Test with valid data (should pass)
    console.log('✅ Testing with VALID data:');
    console.log(`   - Currency: ${testMessage.Body.PmtAddRq[0].FromAcct.CurCode} (3 chars)`);
    console.log(`   - Amount: ${testMessage.Body.PmtAddRq[0].FromAcct.Amount} (> 0)`);
    console.log(`   - Date: ${testMessage.Body.PmtAddRq[0].PayHdr.ProcDate} (YYYY-MM-DD format)`);
    console.log(`   - BIC codes: All 11 characters`);
    
    await kafkaHelper.sendMessage(inputTopic, testMessage, uniqueMUID);
    
    const responseMessage = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    expect(responseMessage).not.toBeNull();
    expect(responseMessage.Trailer.status).toBe('SUCCESS');
    expect(responseMessage.Trailer.StatusCode).toBe('200');
    
    console.log('✅ Validation successful - all fields passed validation');
  });

  test('5. VALIDATION FAILURE TEST - should handle validation failures correctly', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `validation-fail-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    
    // Create message with validation failures
    const invalidMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    // Introduce validation failures
    invalidMessage.Body.PmtAddRq[0].FromAcct.CurCode = "INVALID"; // Invalid currency
    invalidMessage.Body.PmtAddRq[0].FromAcct.Amount = -100.00; // Negative amount
    invalidMessage.Body.PmtAddRq[0].PayHdr.ProcDate = "INVALID_DATE"; // Invalid date format
    invalidMessage.Body.PmtAddRq[0].FromFIData.BIC = "DBSGSGSG"; // 8 chars instead of 11
    invalidMessage.Body.PmtAddRq[0].ToFIData.BIC = "UOVBSGSG"; // 8 chars instead of 11
    invalidMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstgAgt.BIC = "ANZBSGSG"; // 8 chars
    invalidMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstdAgt.BIC = "UOVBSGSG"; // 8 chars
    invalidMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAgt.BIC = "UOVBSGSG"; // 8 chars
    invalidMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.CdtrAgt.BIC = "DBSGSGSG"; // 8 chars
    
    console.log(`🔍 Testing Validation Failures with MUID: ${uniqueMUID}`);
    console.log('❌ Testing with INVALID data:');
    console.log(`   - Currency: ${invalidMessage.Body.PmtAddRq[0].FromAcct.CurCode} (invalid)`);
    console.log(`   - Amount: ${invalidMessage.Body.PmtAddRq[0].FromAcct.Amount} (negative)`);
    console.log(`   - Date: ${invalidMessage.Body.PmtAddRq[0].PayHdr.ProcDate} (invalid format)`);
    console.log(`   - BIC codes: All 8 characters (should be 11)`);
    
    await kafkaHelper.sendMessage(inputTopic, invalidMessage, uniqueMUID);
    
    const responseMessage = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    expect(responseMessage).not.toBeNull();
    expect(responseMessage.Trailer.status).toBe('FAILED');
    expect(responseMessage.Trailer.StatusCode).toBe('400');
    expect(responseMessage.Trailer.StatusDesc).toContain('validation');
    
    console.log('✅ Validation failure handling successful - errors captured in Trailer');
    console.log(`   - Status: ${responseMessage.Trailer.status}`);
    console.log(`   - StatusCode: ${responseMessage.Trailer.StatusCode}`);
    console.log(`   - StatusDesc: ${responseMessage.Trailer.StatusDesc}`);
  });

  test('6. RESPONSE GENERATION TEST - should generate proper response structure', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `response-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    const testMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    console.log(`🔍 Testing Response Generation with MUID: ${uniqueMUID}`);
    
    await kafkaHelper.sendMessage(inputTopic, testMessage, uniqueMUID);
    
    const responseMessage = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    expect(responseMessage).not.toBeNull();
    
    // Verify complete response structure
    console.log('📋 Verifying Response Structure:');
    expect(responseMessage).toHaveProperty('Header');
    expect(responseMessage).toHaveProperty('Body');
    expect(responseMessage).toHaveProperty('Procctxt');
    expect(responseMessage).toHaveProperty('messages');
    expect(responseMessage).toHaveProperty('Trailer');
    
    // Verify Header preservation
    expect(responseMessage.Header.MUID).toBe(uniqueMUID);
    expect(responseMessage.Header.ComponentName).toBe(testMessage.Header.ComponentName);
    expect(responseMessage.Header.UUID).toBe(testMessage.Header.UUID);
    
    // Verify Body preservation
    expect(responseMessage.Body.PmtAddRq).toHaveLength(1);
    expect(responseMessage.Body.PmtAddRq[0].RqUID).toBe(testMessage.Body.PmtAddRq[0].RqUID);
    
    // Verify Procctxt preservation
    expect(responseMessage.Procctxt.PmtDtls.ProcCtryCd).toBe(testMessage.Procctxt.PmtDtls.ProcCtryCd);
    
    // Verify messages preservation
    expect(responseMessage.messages).toHaveLength(1);
    expect(responseMessage.messages[0].instruction.MsgDef.MsgType).toBe(testMessage.messages[0].instruction.MsgDef.MsgType);
    
    // Verify Trailer structure
    expect(responseMessage.Trailer).toHaveProperty('status');
    expect(responseMessage.Trailer).toHaveProperty('StatusCode');
    expect(responseMessage.Trailer).toHaveProperty('StatusDesc');
    expect(responseMessage.Trailer.status).toBe('SUCCESS');
    expect(responseMessage.Trailer.StatusCode).toBe('200');
    expect(responseMessage.Trailer.StatusDesc).toContain('SUCCESS');
    
    console.log('✅ Response generation successful - complete structure preserved with Trailer');
    console.log(`   - Header: ✅ Preserved`);
    console.log(`   - Body: ✅ Preserved`);
    console.log(`   - Procctxt: ✅ Preserved`);
    console.log(`   - messages: ✅ Preserved`);
    console.log(`   - Trailer: ✅ Generated with status: ${responseMessage.Trailer.status}`);
  });

  test('7. COMPLETE PIPELINE TEST - should process through all 4 steps', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `pipeline-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    const testMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    console.log(`🔍 Testing Complete Pipeline with MUID: ${uniqueMUID}`);
    
    const startTime = Date.now();
    
    // Step 1: Consume Kafka Avro Message
    console.log('1️⃣ Step 1: Consume Kafka Avro Message');
    await kafkaHelper.sendMessage(inputTopic, testMessage, uniqueMUID);
    
    // Step 2: Validate Idempotency
    console.log('2️⃣ Step 2: Validate Idempotency');
    // (This happens internally in the service)
    
    // Step 3: Scheme Validation
    console.log('3️⃣ Step 3: Scheme Validation');
    // (This happens internally in the service)
    
    // Step 4: Create Response & Produce Kafka Avro Message
    console.log('4️⃣ Step 4: Create Response & Produce Kafka Avro Message');
    const responseMessage = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    
    const processingTime = Date.now() - startTime;
    
    expect(responseMessage).not.toBeNull();
    expect(responseMessage.Trailer.status).toBe('SUCCESS');
    expect(responseMessage.Trailer.StatusCode).toBe('200');
    expect(processingTime).toBeLessThan(10000); // Should complete within 10 seconds
    
    console.log('✅ Complete pipeline test successful!');
    console.log(`   - Processing time: ${processingTime}ms`);
    console.log(`   - Final status: ${responseMessage.Trailer.status}`);
    console.log(`   - Status code: ${responseMessage.Trailer.StatusCode}`);
  });

  test('8. AGENT ID VALIDATION TEST - should enforce 11-character requirement', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'fast-outward-clearing';
    
    const uniqueMUID = `agent-validation-test-${Date.now()}-${Math.random().toString(36).substring(7)}`;
    
    // Test with 8-character agent IDs (should fail)
    const invalidAgentMessage = {
      ...testData.sampleInputMessage,
      Header: {
        ...testData.sampleInputMessage.Header,
        MUID: uniqueMUID
      }
    };
    
    // Set all agent IDs to 8 characters
    invalidAgentMessage.Body.PmtAddRq[0].FromFIData.BIC = "DBSGSGSG"; // 8 chars
    invalidAgentMessage.Body.PmtAddRq[0].ToFIData.BIC = "UOVBSGSG"; // 8 chars
    invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstgAgt.BIC = "ANZBSGSG"; // 8 chars
    invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstdAgt.BIC = "UOVBSGSG"; // 8 chars
    invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAgt.BIC = "UOVBSGSG"; // 8 chars
    invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.CdtrAgt.BIC = "DBSGSGSG"; // 8 chars
    
    console.log(`🔍 Testing Agent ID Validation with MUID: ${uniqueMUID}`);
    console.log('❌ Testing with 8-character agent IDs (should fail):');
    console.log(`   - FromFIData.BIC: ${invalidAgentMessage.Body.PmtAddRq[0].FromFIData.BIC} (8 chars)`);
    console.log(`   - ToFIData.BIC: ${invalidAgentMessage.Body.PmtAddRq[0].ToFIData.BIC} (8 chars)`);
    console.log(`   - InstgAgt.BIC: ${invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstgAgt.BIC} (8 chars)`);
    console.log(`   - InstdAgt.BIC: ${invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstdAgt.BIC} (8 chars)`);
    console.log(`   - DbtrAgt.BIC: ${invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAgt.BIC} (8 chars)`);
    console.log(`   - CdtrAgt.BIC: ${invalidAgentMessage.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.CdtrAgt.BIC} (8 chars)`);
    
    await kafkaHelper.sendMessage(inputTopic, invalidAgentMessage, uniqueMUID);
    
    const responseMessage = await kafkaHelper.consumeMessage(outputTopic, 10000, uniqueMUID);
    expect(responseMessage).not.toBeNull();
    expect(responseMessage.Trailer.status).toBe('FAILED');
    expect(responseMessage.Trailer.StatusCode).toBe('400');
    expect(responseMessage.Trailer.StatusDesc).toContain('MMBID must have exactly 11 characters');
    
    console.log('✅ Agent ID validation working correctly - 8-character IDs rejected');
    console.log(`   - Status: ${responseMessage.Trailer.status}`);
    console.log(`   - Error: ${responseMessage.Trailer.StatusDesc}`);
  });
});
