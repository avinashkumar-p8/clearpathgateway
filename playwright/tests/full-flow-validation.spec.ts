import { test, expect } from '@playwright/test';

/**
 * Full Flow Validation Tests for Singapore G3 Banking Software
 * 
 * This test file validates the complete Kafka message processing flow:
 * 1. Consume Kafka message in Avro schema using existing consumer
 * 2. Check idempotency using MUID
 * 3. If duplicate → ignore the message
 * 4. If unique → proceed with scheme validation
 * 5. Run scheme validation only on the Message section
 * 6. Apply existing validations (currency, country, tag rules, ISO dates, >0 checks, fixed values, uniqueness)
 * 7. Mark message as success if it passes all validations
 * 
 * Uses the exact schema structure provided:
 * - Header with MUID, Channel, Direction, EventInfo
 * - Body with PmtAddRq containing payment details
 * - Procctxt with processing context
 * - messages array with instruction details
 */
test.describe('Full Flow Validation Tests - Singapore G3 Banking', () => {
  const baseUrl = 'http://localhost:8080';
  
  // Test data constants based on the provided schema
  const validCurrency = 'SGD';
  const validCountry = 'SG';
  const validBIC = 'OCBCSGSGXXX';
  const validDate = '2025-05-30';
  const validAmount = 215;
  const validMsgId = 'NA132504110957516877A9C100364A963O';
  const validInstrId = '20250411MBBESGS2BRT8705045';
  
  test.beforeEach(async ({ request }) => {
    // Reset test state before each test
    await request.post(`${baseUrl}/api/v1/test/enhanced-validation/reset-test-state`);
  });
  
  test.describe('Valid Unique Message - Success Case', () => {
    test('should successfully process valid unique message with all validations passing', async ({ request }) => {
      // Create a valid message using the exact schema structure provided
      const validMessage = {
        Header: {
          ComponentName: "PSPAPFAFAST",
          UUID: `G3I400071311436B-${Date.now()}`,
          EventInfo: {
            EventCode: "P.PSP.STS.M.OP_RPI.100",
            EventDescription: "Payment request received in PSP",
            EventID: `185bd46a-d178-453b-a808-${Date.now()}`,
            EventType: "PE",
            EventProducer: "Clear Path Gateway",
            EventTS: "2025-05-08T09:02:10.765",
            EventTopics: "<KAFKA Topic>",
            SystemId: null,
            Events: {
              Event: [
                {
                  EventCode: "I.PSP.STS.M.OP_RPI.100",
                  EventID: `185bd46a-d178-453b-a808-${Date.now()}`
                },
                {
                  EventCode: "P.PSP.STS.M.OP_RPI.100",
                  EventID: `185bd46a-d178-453b-a808-${Date.now()}`
                }
              ]
            },
            EventVersion: null
          },
          ReplyToQueue: "PPSP.PPORCH.GPAFL.RSP.01",
          ReqMap: null,
          MUID: `VALID-UNIQUE-${Date.now()}`,
          Channel: "G3I",
          Direction: "I",
          RcvdTS: "2025-06-10T21:08:21",
          DomainName: "PAYMENTS",
          DomainType: "PAYMENT"
        },
        Body: {
          PmtAddRq: [
            {
              RqUID: `20250424UOVBSGSGBRT1XXXXXX-${Date.now()}`,
              MsgHdr: {
                ClientDt: "2025-06-10T21:08:21",
                ClientName: "G3I",
                PartyId: "749440SGD000001",
                Version: "1.0"
              },
              PayHdr: {
                PODsID: `G3I400071311436B-${Date.now()}`,
                PaymentID: `20250424UOVBSGSGBRT1XXXXXX-${Date.now()}`,
                ThirdPartyPayID: "NC2503XXXX",
                PaymentTRN: `20250424UOVBSGSGBRT1XXXXXX-${Date.now()}`,
                PaymentRetRef: "FPS00",
                ProcDate: "2025-06-10"
              },
              FromFIData: {
                Country: validCountry,
                BIC: "XXXBSGSXXXX"
              },
              FromCust: {
                Name: "XXXXpore Pte Ltd"
              },
              FromAcct: {
                AcctId: "783451100000001",
                AcctSys: "VAM",
                AcctGrp: "SGB",
                Name: "749440SGD000001",
                PmtAuthMethod: "AFPONLY",
                Narrative: "DDI+G3I400071311436B+OTHR+FPS00+XXXXITAL PTE.+NC250",
                CurCode: validCurrency,
                Amount: 2182.14,
                AcctUse: "BUSINESS"
              },
              ToFIData: {
                Country: validCountry,
                BIC: validBIC
              },
              Clearing: {
                ClearPref: "FAST"
              },
              ToBene: {
                Name: "XX LTD.",
                Country: validCountry,
                Message: "XXXXXXXX"
              },
              ToAcct: {
                AcctId: "80XXXX",
                CurCode: validCurrency,
                Amount: 2182.14,
                Narrative: "12345      INWGDR+API20250424211XXXXXX",
                AcctUse: "BUSINESS"
              },
              Fees: []
            }
          ]
        },
        Procctxt: {
          sideEffect: [],
          softFail: [],
          PmtDtls: {
            PmtCtxt: {
              PuId: `G3I400071311436B-${Date.now()}`,
              IntnSrc: {
                type: "CLRG",
                value: "G3"
              }
            },
            ProcCtryCd: validCountry,
            InstdClrgPref: "G3DDFAST",
            InstdMoPCat: "FAST",
            PmtCtgry: "DD",
            actClrMethod: "ACH",
            actlMtdOfPmtCtgry: "ACH",
            FIDCIdentifier: "DEBTOR",
            FICCIdentifier: "INSTDAGT",
            VAM: "No",
            derivedDRAccountNo: "01000018493A",
            derivedDRAccountSys: "MDZ",
            derivedDRBookCode: "SGB"
          }
        },
        messages: [
          {
            instruction: {
              MsgDef: {
                MsgType: "ISOX",
                Schema: "pacs.003.001.02"
              },
              MsgCtxt: {
                OrigMsgTyp: "PACS.003",
                InstdClrgPref: "G3DDFAST",
                InstdMoPCat: "FAST",
                Site: "SG1",
                BaseAmt: validAmount,
                BaseCcy: validCurrency,
                SenderBIC: "SACHSGS1XXX",
                ProcCtryCd: validCountry,
                Department: "SGIFAST",
                MsgId: `MSG-${Date.now()}`,
                Direction: "I"
              },
              MsgAddRq: {
                OrigMsg: "",
                MsgDtls: {
                  DrctDbtTxInf: {
                    PmtId: {
                      InstrId: `INSTR-${Date.now()}`,
                      TxId: `TX-${Date.now()}`,
                      EndToEndId: "NC2503XXXX",
                      ClrSysRef: "001"
                    },
                    IntrBkSttlmAmt: validAmount,
                    IntrBkSttlmCCY: validCurrency,
                    IntrBkSttlmDt: validDate,
                    InstgAgt: {
                      BIC: validBIC
                    },
                    InstdAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    Dbtr: {
                      Nm: "Sender"
                    },
                    DbtrAcct: {
                      AcctId: "1419XXXX"
                    },
                    DbtrAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    CdtrAgt: {
                      BIC: validBIC
                    },
                    Cdtr: {
                      Nm: "Receiver"
                    },
                    CdtrAcct: {
                      AcctId: "80XXXX"
                    },
                    Purp: {
                      Cd: "OTHR"
                    },
                    RmtInf: {
                      Ustrd: "XXXX"
                    },
                    DrctDbtTx: {
                      MndtRltdInf: {
                        MndtId: "FPS00"
                      }
                    }
                  }
                }
              }
            }
          }
        ]
      };
      
      // Send message to Kafka topic via the test endpoint
      const response = await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-avro-message`, {
        data: validMessage
      });
      
      expect(response.status()).toBe(200);
      
      // Wait for processing (Kafka consumption, idempotency check, validation)
      await new Promise(resolve => setTimeout(resolve, 5000));
      
      // Verify message was processed successfully
      const metricsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/metrics`);
      const metrics = await metricsResponse.json();
      
      expect(metrics.messagesReceived).toBeGreaterThan(0);
      expect(metrics.messagesProcessed).toBeGreaterThan(0);
      expect(metrics.validationErrors).toBe(0);
      expect(metrics.duplicateMuidCount).toBe(0);
      
      // Verify message appears in processed topic (success case)
      const processedResponse = await request.get(`${baseUrl}/api/v1/test/enhanced-validation/check-processed-messages`);
      const processedData = await processedResponse.json();
      
      expect(processedData.processedMessages).toContainEqual(
        expect.objectContaining({
          muid: validMessage.Header.MUID,
          status: 'SUCCESS'
        })
      );
      
      // Verify validation statistics show success
      const statsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/validation-stats`);
      const stats = await statsResponse.json();
      
      expect(stats.totalValidations).toBeGreaterThan(0);
      expect(stats.successfulValidations).toBeGreaterThan(0);
      expect(stats.failedValidations).toBe(0);
      
      // Verify no messages in DLQ
      const dlqResponse = await request.get(`${baseUrl}/api/v1/test/enhanced-validation/check-dlq-messages`);
      const dlqData = await dlqResponse.json();
      
      const dlqMessagesForThisMuid = dlqData.dlqMessages.filter(
        (msg: any) => msg.muid === validMessage.Header.MUID
      );
      
      expect(dlqMessagesForThisMuid).toHaveLength(0);
    });
  });
  
  test.describe('Duplicate Message - Idempotency Rejection', () => {
    test('should reject duplicate message with same MUID and mark as duplicate', async ({ request }) => {
      const duplicateMuid = `DUPLICATE-${Date.now()}`;
      
      // Send first message with the MUID
      const firstMessage = {
        Header: {
          ComponentName: "PSPAPFAFAST",
          UUID: `G3I400071311436B-FIRST-${Date.now()}`,
          EventInfo: {
            EventCode: "P.PSP.STS.M.OP_RPI.100",
            EventDescription: "Payment request received in PSP",
            EventID: `185bd46a-d178-453b-a808-FIRST-${Date.now()}`,
            EventType: "PE",
            EventProducer: "Clear Path Gateway",
            EventTS: "2025-05-08T09:02:10.765",
            EventTopics: "<KAFKA Topic>",
            SystemId: null,
            Events: {
              Event: [
                {
                  EventCode: "I.PSP.STS.M.OP_RPI.100",
                  EventID: `185bd46a-d178-453b-a808-FIRST-${Date.now()}`
                }
              ]
            },
            EventVersion: null
          },
          ReplyToQueue: "PPSP.PPORCH.GPAFL.RSP.01",
          ReqMap: null,
          MUID: duplicateMuid,
          Channel: "G3I",
          Direction: "I",
          RcvdTS: "2025-06-10T21:08:21",
          DomainName: "PAYMENTS",
          DomainType: "PAYMENT"
        },
        Body: {
          PmtAddRq: [
            {
              RqUID: `20250424UOVBSGSGBRT1XXXXXX-FIRST-${Date.now()}`,
              MsgHdr: {
                ClientDt: "2025-06-10T21:08:21",
                ClientName: "G3I",
                PartyId: "749440SGD000001",
                Version: "1.0"
              },
              PayHdr: {
                PODsID: `G3I400071311436B-FIRST-${Date.now()}`,
                PaymentID: `20250424UOVBSGSGBRT1XXXXXX-FIRST-${Date.now()}`,
                ThirdPartyPayID: "NC2503XXXX",
                PaymentTRN: `20250424UOVBSGSGBRT1XXXXXX-FIRST-${Date.now()}`,
                PaymentRetRef: "FPS00",
                ProcDate: "2025-06-10"
              },
              FromFIData: {
                Country: validCountry,
                BIC: "XXXBSGSXXXX"
              },
              FromCust: {
                Name: "XXXXpore Pte Ltd"
              },
              FromAcct: {
                AcctId: "783451100000001",
                AcctSys: "VAM",
                AcctGrp: "SGB",
                Name: "749440SGD000001",
                PmtAuthMethod: "AFPONLY",
                Narrative: "DDI+G3I400071311436B+OTHR+FPS00+XXXXITAL PTE.+NC250",
                CurCode: validCurrency,
                Amount: 2182.14,
                AcctUse: "BUSINESS"
              },
              ToFIData: {
                Country: validCountry,
                BIC: validBIC
              },
              Clearing: {
                ClearPref: "FAST"
              },
              ToBene: {
                Name: "XX LTD.",
                Country: validCountry,
                Message: "XXXXXXXX"
              },
              ToAcct: {
                AcctId: "80XXXX",
                CurCode: validCurrency,
                Amount: 2182.14,
                Narrative: "12345      INWGDR+API20250424211XXXXXX",
                AcctUse: "BUSINESS"
              },
              Fees: []
            }
          ]
        },
        Procctxt: {
          sideEffect: [],
          softFail: [],
          PmtDtls: {
            PmtCtxt: {
              PuId: `G3I400071311436B-FIRST-${Date.now()}`,
              IntnSrc: {
                type: "CLRG",
                value: "G3"
              }
            },
            ProcCtryCd: validCountry,
            InstdClrgPref: "G3DDFAST",
            InstdMoPCat: "FAST",
            PmtCtgry: "DD",
            actClrMethod: "ACH",
            actlMtdOfPmtCtgry: "ACH",
            FIDCIdentifier: "DEBTOR",
            FICCIdentifier: "INSTDAGT",
            VAM: "No",
            derivedDRAccountNo: "01000018493A",
            derivedDRAccountSys: "MDZ",
            derivedDRBookCode: "SGB"
          }
        },
        messages: [
          {
            instruction: {
              MsgDef: {
                MsgType: "ISOX",
                Schema: "pacs.003.001.02"
              },
              MsgCtxt: {
                OrigMsgTyp: "PACS.003",
                InstdClrgPref: "G3DDFAST",
                InstdMoPCat: "FAST",
                Site: "SG1",
                BaseAmt: validAmount,
                BaseCcy: validCurrency,
                SenderBIC: "SACHSGS1XXX",
                ProcCtryCd: validCountry,
                Department: "SGIFAST",
                MsgId: `MSG-FIRST-${Date.now()}`,
                Direction: "I"
              },
              MsgAddRq: {
                OrigMsg: "",
                MsgDtls: {
                  DrctDbtTxInf: {
                    PmtId: {
                      InstrId: `INSTR-FIRST-${Date.now()}`,
                      TxId: `TX-FIRST-${Date.now()}`,
                      EndToEndId: "NC2503XXXX",
                      ClrSysRef: "001"
                    },
                    IntrBkSttlmAmt: validAmount,
                    IntrBkSttlmCCY: validCurrency,
                    IntrBkSttlmDt: validDate,
                    InstgAgt: {
                      BIC: validBIC
                    },
                    InstdAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    Dbtr: {
                      Nm: "Sender"
                    },
                    DbtrAcct: {
                      AcctId: "1419XXXX"
                    },
                    DbtrAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    CdtrAgt: {
                      BIC: validBIC
                    },
                    Cdtr: {
                      Nm: "Receiver"
                    },
                    CdtrAcct: {
                      AcctId: "80XXXX"
                    },
                    Purp: {
                      Cd: "OTHR"
                    },
                    RmtInf: {
                      Ustrd: "XXXX"
                    },
                    DrctDbtTx: {
                      MndtRltdInf: {
                        MndtId: "FPS00"
                      }
                    }
                  }
                }
              }
            }
          }
        ]
      };
      
      const firstResponse = await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-avro-message`, {
        data: firstMessage
      });
      expect(firstResponse.status()).toBe(200);
      
      // Wait for first message processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      // Send duplicate message with same MUID but different content
      const duplicateMessage = {
        Header: {
          ComponentName: "PSPAPFAFAST",
          UUID: `G3I400071311436B-DUPLICATE-${Date.now()}`,
          EventInfo: {
            EventCode: "P.PSP.STS.M.OP_RPI.100",
            EventDescription: "Payment request received in PSP",
            EventID: `185bd46a-d178-453b-a808-DUPLICATE-${Date.now()}`,
            EventType: "PE",
            EventProducer: "Clear Path Gateway",
            EventTS: "2025-05-08T09:02:10.765",
            EventTopics: "<KAFKA Topic>",
            SystemId: null,
            Events: {
              Event: [
                {
                  EventCode: "I.PSP.STS.M.OP_RPI.100",
                  EventID: `185bd46a-d178-453b-a808-DUPLICATE-${Date.now()}`
                }
              ]
            },
            EventVersion: null
          },
          ReplyToQueue: "PPSP.PPORCH.GPAFL.RSP.01",
          ReqMap: null,
          MUID: duplicateMuid, // Same MUID as first message
          Channel: "G3I",
          Direction: "I",
          RcvdTS: "2025-06-10T21:08:21",
          DomainName: "PAYMENTS",
          DomainType: "PAYMENT"
        },
        Body: {
          PmtAddRq: [
            {
              RqUID: `20250424UOVBSGSGBRT1XXXXXX-DUPLICATE-${Date.now()}`,
              MsgHdr: {
                ClientDt: "2025-06-10T21:08:21",
                ClientName: "G3I",
                PartyId: "749440SGD000001",
                Version: "1.0"
              },
              PayHdr: {
                PODsID: `G3I400071311436B-DUPLICATE-${Date.now()}`,
                PaymentID: `20250424UOVBSGSGBRT1XXXXXX-DUPLICATE-${Date.now()}`,
                ThirdPartyPayID: "NC2503XXXX",
                PaymentTRN: `20250424UOVBSGSGBRT1XXXXXX-DUPLICATE-${Date.now()}`,
                PaymentRetRef: "FPS00",
                ProcDate: "2025-06-10"
              },
              FromFIData: {
                Country: "US", // Different country
                BIC: "XXXBSGSXXXX"
              },
              FromCust: {
                Name: "Different Company Name"
              },
              FromAcct: {
                AcctId: "783451100000001",
                AcctSys: "VAM",
                AcctGrp: "SGB",
                Name: "749440SGD000001",
                PmtAuthMethod: "AFPONLY",
                Narrative: "Different narrative",
                CurCode: "USD", // Different currency
                Amount: 5000.0, // Different amount
                AcctUse: "BUSINESS"
              },
              ToFIData: {
                Country: "US", // Different country
                BIC: "DIFFERENTBICXXX"
              },
              Clearing: {
                ClearPref: "FAST"
              },
              ToBene: {
                Name: "Different Receiver",
                Country: "US", // Different country
                Message: "Different message"
              },
              ToAcct: {
                AcctId: "90XXXX",
                CurCode: "USD", // Different currency
                Amount: 5000.0, // Different amount
                Narrative: "Different narrative",
                AcctUse: "BUSINESS"
              },
              Fees: []
            }
          ]
        },
        Procctxt: {
          sideEffect: [],
          softFail: [],
          PmtDtls: {
            PmtCtxt: {
              PuId: `G3I400071311436B-DUPLICATE-${Date.now()}`,
              IntnSrc: {
                type: "CLRG",
                value: "G3"
              }
            },
            ProcCtryCd: "US", // Different country
            InstdClrgPref: "G3DDFAST",
            InstdMoPCat: "FAST",
            PmtCtgry: "DD",
            actClrMethod: "ACH",
            actlMtdOfPmtCtgry: "ACH",
            FIDCIdentifier: "DEBTOR",
            FICCIdentifier: "INSTDAGT",
            VAM: "No",
            derivedDRAccountNo: "01000018493A",
            derivedDRAccountSys: "MDZ",
            derivedDRBookCode: "SGB"
          }
        },
        messages: [
          {
            instruction: {
              MsgDef: {
                MsgType: "ISOX",
                Schema: "pacs.003.001.02"
              },
              MsgCtxt: {
                OrigMsgTyp: "PACS.003",
                InstdClrgPref: "G3DDFAST",
                InstdMoPCat: "FAST",
                Site: "SG1",
                BaseAmt: 5000, // Different amount
                BaseCcy: "USD", // Different currency
                SenderBIC: "SACHSGS1XXX",
                ProcCtryCd: "US", // Different country
                Department: "SGIFAST",
                MsgId: `MSG-DUPLICATE-${Date.now()}`,
                Direction: "I"
              },
              MsgAddRq: {
                OrigMsg: "",
                MsgDtls: {
                  DrctDbtTxInf: {
                    PmtId: {
                      InstrId: `INSTR-DUPLICATE-${Date.now()}`,
                      TxId: `TX-DUPLICATE-${Date.now()}`,
                      EndToEndId: "NC2503XXXX",
                      ClrSysRef: "001"
                    },
                    IntrBkSttlmAmt: 5000, // Different amount
                    IntrBkSttlmCCY: "USD", // Different currency
                    IntrBkSttlmDt: "2025-06-01", // Different date
                    InstgAgt: {
                      BIC: "DIFFERENTBICXXX"
                    },
                    InstdAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    Dbtr: {
                      Nm: "Different Sender"
                    },
                    DbtrAcct: {
                      AcctId: "1419XXXX"
                    },
                    DbtrAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    CdtrAgt: {
                      BIC: "DIFFERENTBICXXX"
                    },
                    Cdtr: {
                      Nm: "Different Receiver"
                    },
                    CdtrAcct: {
                      AcctId: "90XXXX"
                    },
                    Purp: {
                      Cd: "OTHR"
                    },
                    RmtInf: {
                      Ustrd: "Different remittance info"
                    },
                    DrctDbtTx: {
                      MndtRltdInf: {
                        MndtId: "FPS00"
                      }
                    }
                  }
                }
              }
            }
          }
        ]
      };
      
      const duplicateResponse = await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-avro-message`, {
        data: duplicateMessage
      });
      expect(duplicateResponse.status()).toBe(200);
      
      // Wait for duplicate processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      // Verify duplicate was detected
      const metricsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/metrics`);
      const metrics = await metricsResponse.json();
      
      expect(metrics.duplicateMuidCount).toBeGreaterThan(0);
      
      // Verify only first message was processed (duplicate should be ignored)
      const processedResponse = await request.get(`${baseUrl}/api/v1/test/enhanced-validation/check-processed-messages`);
      const processedData = await processedResponse.json();
      
      const processedMessages = processedData.processedMessages.filter(
        (msg: any) => msg.muid === duplicateMuid
      );
      
      expect(processedMessages).toHaveLength(1); // Only first message
      expect(processedMessages[0].status).toBe('SUCCESS');
      
      // Verify duplicate message is not in processed messages
      const duplicateProcessed = processedData.processedMessages.filter(
        (msg: any) => msg.muid === duplicateMuid && msg.uuid === duplicateMessage.Header.UUID
      );
      
      expect(duplicateProcessed).toHaveLength(0);
    });
  });
  
  test.describe('Invalid Schema Tag - Failure at Scheme Validation', () => {
    test('should reject message with invalid currency in messages section', async ({ request }) => {
      const invalidMessage = {
        Header: {
          ComponentName: "PSPAPFAFAST",
          UUID: `G3I400071311436B-INVALID-${Date.now()}`,
          EventInfo: {
            EventCode: "P.PSP.STS.M.OP_RPI.100",
            EventDescription: "Payment request received in PSP",
            EventID: `185bd46a-d178-453b-a808-INVALID-${Date.now()}`,
            EventType: "PE",
            EventProducer: "Clear Path Gateway",
            EventTS: "2025-05-08T09:02:10.765",
            EventTopics: "<KAFKA Topic>",
            SystemId: null,
            Events: {
              Event: [
                {
                  EventCode: "I.PSP.STS.M.OP_RPI.100",
                  EventID: `185bd46a-d178-453b-a808-INVALID-${Date.now()}`
                }
              ]
            },
            EventVersion: null
          },
          ReplyToQueue: "PPSP.PPORCH.GPAFL.RSP.01",
          ReqMap: null,
          MUID: `INVALID-CURRENCY-${Date.now()}`,
          Channel: "G3I",
          Direction: "I",
          RcvdTS: "2025-06-10T21:08:21",
          DomainName: "PAYMENTS",
          DomainType: "PAYMENT"
        },
        Body: {
          PmtAddRq: [
            {
              RqUID: `20250424UOVBSGSGBRT1XXXXXX-INVALID-${Date.now()}`,
              MsgHdr: {
                ClientDt: "2025-06-10T21:08:21",
                ClientName: "G3I",
                PartyId: "749440SGD000001",
                Version: "1.0"
              },
              PayHdr: {
                PODsID: `G3I400071311436B-INVALID-${Date.now()}`,
                PaymentID: `20250424UOVBSGSGBRT1XXXXXX-INVALID-${Date.now()}`,
                ThirdPartyPayID: "NC2503XXXX",
                PaymentTRN: `20250424UOVBSGSGBRT1XXXXXX-INVALID-${Date.now()}`,
                PaymentRetRef: "FPS00",
                ProcDate: "2025-06-10"
              },
              FromFIData: {
                Country: validCountry,
                BIC: "XXXBSGSXXXX"
              },
              FromCust: {
                Name: "XXXXpore Pte Ltd"
              },
              FromAcct: {
                AcctId: "783451100000001",
                AcctSys: "VAM",
                AcctGrp: "SGB",
                Name: "749440SGD000001",
                PmtAuthMethod: "AFPONLY",
                Narrative: "DDI+G3I400071311436B+OTHR+FPS00+XXXXITAL PTE.+NC250",
                CurCode: validCurrency,
                Amount: 2182.14,
                AcctUse: "BUSINESS"
              },
              ToFIData: {
                Country: validCountry,
                BIC: validBIC
              },
              Clearing: {
                ClearPref: "FAST"
              },
              ToBene: {
                Name: "XX LTD.",
                Country: validCountry,
                Message: "XXXXXXXX"
              },
              ToAcct: {
                AcctId: "80XXXX",
                CurCode: validCurrency,
                Amount: 2182.14,
                Narrative: "12345      INWGDR+API20250424211XXXXXX",
                AcctUse: "BUSINESS"
              },
              Fees: []
            }
          ]
        },
        Procctxt: {
          sideEffect: [],
          softFail: [],
          PmtDtls: {
            PmtCtxt: {
              PuId: `G3I400071311436B-INVALID-${Date.now()}`,
              IntnSrc: {
                type: "CLRG",
                value: "G3"
              }
            },
            ProcCtryCd: validCountry,
            InstdClrgPref: "G3DDFAST",
            InstdMoPCat: "FAST",
            PmtCtgry: "DD",
            actClrMethod: "ACH",
            actlMtdOfPmtCtgry: "ACH",
            FIDCIdentifier: "DEBTOR",
            FICCIdentifier: "INSTDAGT",
            VAM: "No",
            derivedDRAccountNo: "01000018493A",
            derivedDRAccountSys: "MDZ",
            derivedDRBookCode: "SGB"
          }
        },
        messages: [
          {
            instruction: {
              MsgDef: {
                MsgType: "ISOX",
                Schema: "pacs.003.001.02"
              },
              MsgCtxt: {
                OrigMsgTyp: "PACS.003",
                InstdClrgPref: "G3DDFAST",
                InstdMoPCat: "FAST",
                Site: "SG1",
                BaseAmt: validAmount,
                BaseCcy: validCurrency,
                SenderBIC: "SACHSGS1XXX",
                ProcCtryCd: validCountry,
                Department: "SGIFAST",
                MsgId: `MSG-INVALID-${Date.now()}`,
                Direction: "I"
              },
              MsgAddRq: {
                OrigMsg: "",
                MsgDtls: {
                  DrctDbtTxInf: {
                    PmtId: {
                      InstrId: `INSTR-INVALID-${Date.now()}`,
                      TxId: `TX-INVALID-${Date.now()}`,
                      EndToEndId: "NC2503XXXX",
                      ClrSysRef: "001"
                    },
                    IntrBkSttlmAmt: validAmount,
                    IntrBkSttlmCCY: "INVALID", // Invalid currency - this should cause validation failure
                    IntrBkSttlmDt: validDate,
                    InstgAgt: {
                      BIC: validBIC
                    },
                    InstdAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    Dbtr: {
                      Nm: "Sender"
                    },
                    DbtrAcct: {
                      AcctId: "1419XXXX"
                    },
                    DbtrAgt: {
                      BIC: "XXXBSGSXXXX"
                    },
                    CdtrAgt: {
                      BIC: validBIC
                    },
                    Cdtr: {
                      Nm: "Receiver"
                    },
                    CdtrAcct: {
                      AcctId: "80XXXX"
                    },
                    Purp: {
                      Cd: "OTHR"
                    },
                    RmtInf: {
                      Ustrd: "XXXX"
                    },
                    DrctDbtTx: {
                      MndtRltdInf: {
                        MndtId: "FPS00"
                      }
                    }
                  }
                }
              }
            }
          }
        ]
      };
      
      const response = await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-avro-message`, {
        data: invalidMessage
      });
      expect(response.status()).toBe(200);
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 5000));
      
      // Verify validation failed
      const metricsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/metrics`);
      const metrics = await metricsResponse.json();
      
      expect(metrics.validationErrors).toBeGreaterThan(0);
      
      // Verify message went to DLQ (validation failure)
      const dlqResponse = await request.get(`${baseUrl}/api/v1/test/enhanced-validation/check-dlq-messages`);
      const dlqData = await dlqResponse.json();
      
      expect(dlqData.dlqMessages).toContainEqual(
        expect.objectContaining({
          muid: invalidMessage.Header.MUID,
          reason: expect.stringContaining('currency')
        })
      );
      
      // Verify message is NOT in processed messages
      const processedResponse = await request.get(`${baseUrl}/api/v1/test/enhanced-validation/check-processed-messages`);
      const processedData = await processedResponse.json();
      
      const processedMessages = processedData.processedMessages.filter(
        (msg: any) => msg.muid === invalidMessage.Header.MUID
      );
      
      expect(processedMessages).toHaveLength(0);
    });
  });
});
