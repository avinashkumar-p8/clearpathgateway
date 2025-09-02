import { test, expect } from '@playwright/test';
import { KafkaTestHelper } from '../helpers/kafka-helper';
import { HttpTestHelper } from '../helpers/http-helper';
import { testData } from '../helpers/test-data';

test.describe('Compliance and Security Tests', () => {
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

  test.describe('Regulatory Compliance Tests', () => {
    test('should enforce transaction amount limits', async () => {
      const testCases = [
        { amount: 1000000.00, expected: 'SUCCESS', description: 'High value transaction within limits' },
        { amount: 5000000.00, expected: 'FAILED', description: 'Exceeds regulatory limits' },
        { amount: 10000.00, expected: 'SUCCESS', description: 'Normal transaction amount' }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].FromAcct.Amount = testCase.amount;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        expect(responseMessage.Trailer.status).toBe(testCase.expected);
        
        if (testCase.expected === 'FAILED') {
          expect(responseMessage.Trailer.StatusCode).toBe('400');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Transaction amount exceeds regulatory limits')
          )).toBe(true);
        }
      }
    });

    test('should validate country compliance', async () => {
      const testCases = [
        { country: 'SG', expected: 'SUCCESS', description: 'Singapore - compliant' },
        { country: 'US', expected: 'SUCCESS', description: 'United States - compliant' },
        { country: 'XX', expected: 'FAILED', description: 'Sanctioned country' },
        { country: 'YY', expected: 'FAILED', description: 'Restricted country' }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].ToFIData.Country = testCase.country;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        expect(responseMessage.Trailer.status).toBe(testCase.expected);
        
        if (testCase.expected === 'FAILED') {
          expect(responseMessage.Trailer.StatusCode).toBe('400');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Country compliance check failed') ||
            desc.includes('Sanctioned country')
          )).toBe(true);
        }
      }
    });

    test('should enforce currency compliance', async () => {
      const testCases = [
        { currency: 'SGD', expected: 'SUCCESS', description: 'Singapore Dollar - compliant' },
        { currency: 'USD', expected: 'SUCCESS', description: 'US Dollar - compliant' },
        { currency: 'EUR', expected: 'SUCCESS', description: 'Euro - compliant' },
        { currency: 'INVALID', expected: 'FAILED', description: 'Invalid currency code' }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].FromAcct.CurCode = testCase.currency;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        expect(responseMessage.Trailer.status).toBe(testCase.expected);
        
        if (testCase.expected === 'FAILED') {
          expect(responseMessage.Trailer.StatusCode).toBe('400');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Invalid currency code')
          )).toBe(true);
        }
      }
    });

    test('should validate BIC compliance', async () => {
      const testCases = [
        { bic: 'DBSASG2X', expected: 'SUCCESS', description: 'Valid DBS BIC' },
        { bic: 'OCBCSGSG', expected: 'SUCCESS', description: 'Valid OCBC BIC' },
        { bic: 'INVALID', expected: 'FAILED', description: 'Invalid BIC format' },
        { bic: '12345678', expected: 'FAILED', description: 'Invalid BIC format' }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].FromFIData.BIC = testCase.bic;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        expect(responseMessage.Trailer.status).toBe(testCase.expected);
        
        if (testCase.expected === 'FAILED') {
          expect(responseMessage.Trailer.StatusCode).toBe('400');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Invalid BIC format')
          )).toBe(true);
        }
      }
    });
  });

  test.describe('Security Validation Tests', () => {
    test('should validate message integrity', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      
      // Add a digital signature or hash for integrity validation
      inputMessage.Header.MessageHash = 'valid-hash-value';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
    });

    test('should reject tampered messages', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      
      // Simulate tampered message
      inputMessage.Header.MessageHash = 'tampered-hash-value';
      inputMessage.Body.PmtAddRq[0].FromAcct.Amount = 999999.99; // Modified amount
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc.some(desc => 
        desc.includes('Message integrity check failed') ||
        desc.includes('Tampered message detected')
      )).toBe(true);
    });

    test('should validate sender authentication', async () => {
      const testCases = [
        { 
          componentName: 'FAST_SENDER', 
          expected: 'SUCCESS', 
          description: 'Authenticated sender' 
        },
        { 
          componentName: 'UNAUTHORIZED_SENDER', 
          expected: 'FAILED', 
          description: 'Unauthorized sender' 
        },
        { 
          componentName: null, 
          expected: 'FAILED', 
          description: 'Missing sender information' 
        }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Header.ComponentName = testCase.componentName;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        expect(responseMessage.Trailer.status).toBe(testCase.expected);
        
        if (testCase.expected === 'FAILED') {
          expect(responseMessage.Trailer.StatusCode).toBe('401');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Unauthorized sender') ||
            desc.includes('Authentication failed')
          )).toBe(true);
        }
      }
    });

    test('should validate message encryption', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      
      // Add encryption metadata
      inputMessage.Header.EncryptionType = 'AES-256';
      inputMessage.Header.EncryptionKey = 'encrypted-key-reference';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
    });
  });

  test.describe('Data Privacy and Protection Tests', () => {
    test('should mask sensitive data in logs', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      
      // Add sensitive data
      inputMessage.Body.PmtAddRq[0].FromAcct.AcctId = '1234567890';
      inputMessage.Body.PmtAddRq[0].FromCust.Name = 'John Doe';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      
      // Verify sensitive data is not exposed in response
      expect(responseMessage.Body.PmtAddRq[0].FromAcct.AcctId).not.toBe('1234567890');
      expect(responseMessage.Body.PmtAddRq[0].FromCust.Name).not.toBe('John Doe');
    });

    test('should handle PII data according to regulations', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      
      // Add PII data
      inputMessage.Body.PmtAddRq[0].FromCust.NationalId = 'S1234567A';
      inputMessage.Body.PmtAddRq[0].FromCust.PassportNumber = 'E12345678';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      
      // Verify PII is properly handled
      expect(responseMessage.Body.PmtAddRq[0].FromCust.NationalId).toBeUndefined();
      expect(responseMessage.Body.PmtAddRq[0].FromCust.PassportNumber).toBeUndefined();
    });
  });

  test.describe('Audit and Logging Tests', () => {
    test('should maintain audit trail for all transactions', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      const uniqueMUID = `audit-test-${Date.now()}`;
      inputMessage.Header.MUID = uniqueMUID;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      
      // Verify audit information is present
      expect(responseMessage.Header.MUID).toBe(uniqueMUID);
      expect(responseMessage.Trailer.auditId).toBeDefined();
      expect(responseMessage.Trailer.processingTimestamp).toBeDefined();
    });

    test('should log security events', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      
      // Simulate security event
      inputMessage.Header.SecurityLevel = 'HIGH';
      inputMessage.Header.RiskScore = 85;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      
      // Verify security logging
      expect(responseMessage.Trailer.securityEvents).toBeDefined();
      expect(responseMessage.Trailer.securityEvents.length).toBeGreaterThan(0);
    });
  });

  test.describe('Compliance Reporting Tests', () => {
    test('should generate compliance reports', async () => {
      const inputMessage = { ...testData.sampleInputMessage };
      
      // Add compliance metadata
      inputMessage.Header.ComplianceCategory = 'REGULATORY';
      inputMessage.Header.ReportingRequired = true;
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('SUCCESS');
      
      // Verify compliance reporting
      expect(responseMessage.Trailer.complianceReport).toBeDefined();
      expect(responseMessage.Trailer.complianceReport.reportGenerated).toBe(true);
      expect(responseMessage.Trailer.complianceReport.reportId).toBeDefined();
    });

    test('should flag suspicious transactions', async () => {
      const testCases = [
        { 
          amount: 999999.99, 
          country: 'XX', 
          expected: 'FLAGGED', 
          description: 'High amount to sanctioned country' 
        },
        { 
          amount: 1000.00, 
          country: 'SG', 
          expected: 'SUCCESS', 
          description: 'Normal transaction' 
        }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].FromAcct.Amount = testCase.amount;
        inputMessage.Body.PmtAddRq[0].ToFIData.Country = testCase.country;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        
        if (testCase.expected === 'FLAGGED') {
          expect(responseMessage.Trailer.status).toBe('FLAGGED');
          expect(responseMessage.Trailer.StatusCode).toBe('200');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Transaction flagged for review')
          )).toBe(true);
        } else {
          expect(responseMessage.Trailer.status).toBe('SUCCESS');
        }
      }
    });
  });

  test.describe('Regulatory Limit Tests', () => {
    test('should enforce daily transaction limits', async () => {
      const testCases = [
        { amount: 50000.00, expected: 'SUCCESS', description: 'Within daily limit' },
        { amount: 100000.00, expected: 'SUCCESS', description: 'At daily limit' },
        { amount: 150000.00, expected: 'FAILED', description: 'Exceeds daily limit' }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].FromAcct.Amount = testCase.amount;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        expect(responseMessage.Trailer.status).toBe(testCase.expected);
        
        if (testCase.expected === 'FAILED') {
          expect(responseMessage.Trailer.StatusCode).toBe('400');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Daily transaction limit exceeded')
          )).toBe(true);
        }
      }
    });

    test('should enforce monthly transaction limits', async () => {
      const testCases = [
        { amount: 500000.00, expected: 'SUCCESS', description: 'Within monthly limit' },
        { amount: 1000000.00, expected: 'SUCCESS', description: 'At monthly limit' },
        { amount: 1500000.00, expected: 'FAILED', description: 'Exceeds monthly limit' }
      ];

      for (const testCase of testCases) {
        const inputMessage = { ...testData.sampleInputMessage };
        inputMessage.Body.PmtAddRq[0].FromAcct.Amount = testCase.amount;
        
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        
        const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
        
        expect(responseMessage).toBeDefined();
        expect(responseMessage.Trailer.status).toBe(testCase.expected);
        
        if (testCase.expected === 'FAILED') {
          expect(responseMessage.Trailer.StatusCode).toBe('400');
          expect(responseMessage.Trailer.StatusDesc.some(desc => 
            desc.includes('Monthly transaction limit exceeded')
          )).toBe(true);
        }
      }
    });
  });
});
