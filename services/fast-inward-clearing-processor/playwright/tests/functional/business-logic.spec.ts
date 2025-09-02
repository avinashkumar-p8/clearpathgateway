import { test, expect } from '@playwright/test';
import { KafkaTestHelper } from '../helpers/kafka-helper';
import { HttpTestHelper } from '../helpers/http-helper';
import { testData } from '../helpers/test-data';

test.describe('Business Logic Tests', () => {
  let kafkaHelper: KafkaTestHelper;
  let httpHelper: HttpTestHelper;

  test.beforeEach(async () => {
    kafkaHelper = new KafkaTestHelper();
    httpHelper = new HttpTestHelper();
  });

  test.afterEach(async () => {
    await kafkaHelper.disconnect();
    await httpHelper.disconnect();
  });

  test.describe('Payment Validation Tests', () => {
    test('should process valid payment message successfully', async () => {
      const inputMessage = testData.sampleInputMessage;
      
      // Send message to input topic
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      // Wait for processing and consume from output topic
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      expect(responseMessage.Trailer.StatusCode).toBe('200');
      expect(responseMessage.Trailer.StatusDesc).toContain('SUCCESS');
    });

    test('should reject payment with negative amount', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromAcct.Amount = -1000.00;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Invalid amount: Amount cannot be negative');
    });

    test('should reject payment with missing MUID', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Header.MUID = null;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Required field MUID is missing');
    });

    test('should reject payment with invalid currency code', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromAcct.CurCode = 'INVALID';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Invalid currency code: INVALID');
    });

    test('should reject payment with invalid BIC format', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromFIData.BIC = 'INVALID';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Invalid BIC format: INVALID');
    });

    test('should reject payment with invalid country code', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromFIData.Country = 'XX';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Invalid country code: XX');
    });
  });

  test.describe('Scheme Validation Tests', () => {
    test('should reject payment with invalid clearing preference', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].Clearing.ClearPref = 'INVALID';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Invalid clearing preference: INVALID');
    });

    test('should reject payment with invalid payment category', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Procctxt.PmtDtls.PmtCtxt.actlMtdOfPmtCtgry = 'INVALID';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Invalid payment category: INVALID');
    });
  });

  test.describe('Business Logic Tests', () => {
    test('should process high value transaction with additional validation', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromAcct.Amount = 1000000.00;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      expect(responseMessage.Trailer.StatusCode).toBe('200');
    });

    test('should reject cross-currency transaction', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromAcct.CurCode = 'USD';
      inputMessage.Body.PmtAddRq[0].ToAcct.CurCode = 'SGD';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Cross-currency transactions not supported');
    });

    test('should reject zero amount transaction', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromAcct.Amount = 0.00;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Transaction amount cannot be zero');
    });

    test('should process maximum allowed amount', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromAcct.Amount = 999999999.99;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      expect(responseMessage.Trailer.StatusCode).toBe('200');
    });
  });

  test.describe('Edge Cases and Special Scenarios', () => {
    test('should handle special characters in narrative', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].FromAcct.Narrative = 'Payment with special chars: @#$%^&*()';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      expect(responseMessage.Trailer.StatusCode).toBe('200');
    });

    test('should reject future dated transaction', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].PayHdr.ProcDate = '2025-12-31';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Future processing date not allowed');
    });

    test('should reject transaction to sanctioned country', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body.PmtAddRq[0].ToFIData.Country = 'XX';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Transactions to sanctioned countries not allowed');
    });

    test('should handle malformed message structure', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Body = null;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc).toContain('Message body is required');
    });
  });

  test.describe('Large Message Processing', () => {
    test('should process large message within SLA', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.messages = Array(1000).fill({ 
        instruction: { 
          MsgDef: { 
            MsgType: "PAYMENT" 
          } 
        } 
      });
      
      const startTime = Date.now();
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      const processingTime = Date.now() - startTime;
      
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      expect(responseMessage.Trailer.StatusCode).toBe('200');
      expect(processingTime).toBeLessThan(5000); // Should complete within 5 seconds
    });
  });

  test.describe('Message Structure Validation', () => {
    test('should validate all required header fields', async () => {
      const requiredFields = ['ComponentName', 'UUID', 'MUID', 'Channel', 'Direction'];
      
      for (const field of requiredFields) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Header[field] = null;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage.Trailer.status).toBe('FAILED');
        expect(responseMessage.Trailer.StatusCode).toBe('400');
        expect(responseMessage.Trailer.StatusDesc.some(desc => 
          desc.includes(`Required field ${field} is missing`)
        )).toBe(true);
      }
    });

    test('should validate payment amount format', async () => {
      const invalidAmounts = ['invalid', null, undefined, '0', '-0'];
      
      for (const amount of invalidAmounts) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].FromAcct.Amount = amount;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage.Trailer.status).toBe('FAILED');
        expect(responseMessage.Trailer.StatusCode).toBe('400');
      }
    });
  });
});
