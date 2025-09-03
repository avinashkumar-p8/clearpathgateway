export const testData = {
  // Sample InputMessage structure matching the new Avro schema
  sampleInputMessage: {
    Header: {
      ComponentName: "FAST_SENDER",
      UUID: "test-uuid-12345",
      EventInfo: {
        EventCode: "PAYMENT_INITIATED",
        EventDescription: "Payment message initiated",
        EventID: "evt-001",
        EventType: "PAYMENT",
        EventProducer: "FAST_SENDER",
        EventTS: "2024-01-15T10:30:00Z",
        EventTopics: "payment,clearing",
        SystemId: null,
        Events: {
          Event: [
            {
              EventCode: "PAYMENT_CREATED",
              EventID: "evt-001-001"
            }
          ]
        },
        EventVersion: null
      },
      ReplyToQueue: "fast.response.queue",
      ReqMap: null,
      MUID: "msg-unique-id-12345",
      Channel: "FAST_CHANNEL",
      Direction: "INWARD",
      RcvdTS: "2024-01-15T10:30:00Z",
      DomainName: "PAYMENT_DOMAIN",
      DomainType: "CLEARING"
    },
    Body: {
      PmtAddRq: [
        {
          RqUID: "req-uid-12345",
          MsgHdr: {
            ClientDt: "2024-01-15",
            ClientName: "TEST_CLIENT",
            PartyId: "PARTY_001",
            Version: "1.0"
          },
          PayHdr: {
            PODsID: "pods-123",
            PaymentID: "pay-12345",
            ThirdPartyPayID: "tp-123",
            PaymentTRN: "trn-12345",
            PaymentRetRef: "ret-ref-123",
            ProcDate: "2024-01-15"
          },
          FromFIData: {
            Country: "SG",
            BIC: "DBSGSGSGXXX"
          },
          FromCust: {
            Name: "Test Sender"
          },
          FromAcct: {
            AcctId: "ACC001",
            AcctSys: "DBS",
            AcctGrp: "CURRENT",
            Name: "Sender Account",
            PmtAuthMethod: "SIGNATURE",
            Narrative: "Test payment",
            CurCode: "SGD",
            Amount: 215.00,
            AcctUse: "DEBIT"
          },
          ToFIData: {
            Country: "SG",
            BIC: "UOVBSGSGXXX"
          },
          Clearing: {
            ClearPref: "FAST"
          },
          ToBene: {
            Name: "Test Receiver",
            Country: "SG",
            Message: "Test payment message"
          },
          ToAcct: {
            AcctId: "ACC002",
            CurCode: "SGD",
            Amount: 215.00,
            Narrative: "Test payment received",
            AcctUse: "CREDIT"
          },
          Fees: ["0.50"]
        }
      ]
    },
    Procctxt: {
      sideEffect: ["none"],
      softFail: [],
      PmtDtls: {
        PmtCtxt: {
          PuId: "pu-001",
          IntnSrc: {
            type: "MANUAL",
            value: "USER_INITIATED"
          }
        },
        ProcCtryCd: "SG",
        InstdClrgPref: "FAST",
        InstdMoPCat: "CLEARING",
        PmtCtgry: "PAYMENT",
        actClrMethod: "FAST",
        actlMtdOfPmtCtgry: "CLEARING",
        FIDCIdentifier: "fidc-001",
        FICCIdentifier: "ficc-001",
        VAM: "vam-001",
        derivedDRAccountNo: "DR001",
        derivedDRAccountSys: "DBS",
        derivedDRBookCode: "CURRENT"
      }
    },
    messages: [
      {
        instruction: {
          MsgDef: {
            MsgType: "PAYMENT",
            Schema: "FAST_PAYMENT_SCHEMA"
          },
          MsgCtxt: {
            OrigMsgTyp: "PAYMENT_INITIATION",
            InstdClrgPref: "FAST",
            InstdMoPCat: "CLEARING",
            Site: "SINGAPORE",
            BaseAmt: 215.00,
            BaseCcy: "SGD",
            SenderBIC: "DBSGSGSGXXX",
            ProcCtryCd: "SG",
            Department: "PAYMENTS",
            MsgId: "msg-001",
            Direction: "INWARD"
          },
          MsgAddRq: {
            OrigMsg: "Original payment message",
            MsgDtls: {
              DrctDbtTxInf: {
                PmtId: {
                  InstrId: "instr-001",
                  TxId: "tx-001",
                  EndToEndId: "end-to-end-001",
                  ClrSysRef: "clr-001"
                },
                IntrBkSttlmAmt: 215.00,
                IntrBkSttlmCCY: "SGD",
                IntrBkSttlmDt: "2024-01-15",
                InstgAgt: {
                  BIC: "ANZBSGSGXXX"
                },
                InstdAgt: {
                  BIC: "UOVBSGSGXXX"
                },
                Dbtr: {
                  Nm: "Test Sender"
                },
                DbtrAcct: {
                  AcctId: "ACC001"
                },
                DbtrAgt: {
                  BIC: "UOVBSGSGXXX"
                },
                CdtrAgt: {
                  BIC: "DBSGSGSGXXX"
                },
                Cdtr: {
                  Nm: "Test Receiver"
                },
                CdtrAcct: {
                  AcctId: "ACC002"
                },
                Purp: {
                  Cd: "CASH"
                },
                RmtInf: {
                  Ustrd: "Test payment"
                },
                DrctDbtTx: {
                  MndtRltdInf: {
                    MndtId: "mandate-001"
                  }
                }
              }
            }
          }
        }
      }
    ]
  },

  // Expected ResponseMessage structure
  expectedResponseMessage: {
    Header: {
      ComponentName: "FAST_SENDER",
      UUID: "test-uuid-12345",
      MUID: "msg-unique-id-12345",
      // ... other header fields
    },
    Body: {
      // ... same body structure
    },
    Procctxt: {
      // ... same processing context
    },
    messages: [
      // ... same messages structure
    ],
    Trailer: {
      status: "SUCCESS",
      StatusCode: "200",
      StatusDesc: ["SUCCESS"]
    }
  },

  // Enhanced test scenarios covering all functionality
  testScenarios: {
    // Basic Functionality Tests
    validPayment: {
      name: "Valid Payment Message",
      input: "sampleInputMessage",
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Standard valid payment message processing"
    },
    
    // Validation Tests
    invalidAmount: {
      name: "Invalid Amount",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromAcct.Amount": -1000.00 },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid amount: Amount cannot be negative"]
    },
    
    missingRequiredField: {
      name: "Missing Required Field",
      input: "sampleInputMessage",
      modification: { "Header.MUID": null },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Required field MUID is missing"]
    },
    
    invalidCurrency: {
      name: "Invalid Currency Code",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromAcct.CurCode": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid currency code: INVALID"]
    },
    
    invalidBIC: {
      name: "Invalid BIC Format",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromFIData.BIC": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid BIC format: INVALID"]
    },
    
    invalidCountry: {
      name: "Invalid Country Code",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromFIData.Country": "XX" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid country code: XX"]
    },
    
    // Idempotency Tests
    duplicateMUID: {
      name: "Duplicate MUID Processing",
      input: "sampleInputMessage",
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Second message with same MUID should be idempotent"
    },
    
    // Scheme Validation Tests
    invalidClearingPreference: {
      name: "Invalid Clearing Preference",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].Clearing.ClearPref": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid clearing preference: INVALID"]
    },
    
    invalidPaymentCategory: {
      name: "Invalid Payment Category",
      input: "sampleInputMessage",
      modification: { "Procctxt.PmtDtls.PmtCtxt.actlMtdOfPmtCtgry": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid payment category: INVALID"]
    },
    
    // Business Logic Tests
    highValueTransaction: {
      name: "High Value Transaction",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromAcct.Amount": 1000000.00 },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "High value transaction should pass additional validation"
    },
    
    crossCurrencyTransaction: {
      name: "Cross Currency Transaction",
      input: "sampleInputMessage",
      modification: { 
        "Body.PmtAddRq[0].FromAcct.CurCode": "USD",
        "Body.PmtAddRq[0].ToAcct.CurCode": "SGD"
      },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Cross-currency transactions not supported"]
    },
    
    // Error Handling Tests
    malformedMessage: {
      name: "Malformed Message Structure",
      input: "sampleInputMessage",
      modification: { "Body": null },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Message body is required"]
    },
    
    // Performance Tests
    largeMessage: {
      name: "Large Message Processing",
      input: "sampleInputMessage",
      modification: { "messages": Array(1000).fill({ instruction: { MsgDef: { MsgType: "PAYMENT" } } }) },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Large message should be processed within SLA"
    },
    
    // Edge Cases
    zeroAmount: {
      name: "Zero Amount Transaction",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromAcct.Amount": 0.00 },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Transaction amount cannot be zero"]
    },
    
    maximumAmount: {
      name: "Maximum Amount Transaction",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromAcct.Amount": 999999999.99 },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Maximum allowed amount should be processed"
    },
    
    // Special Characters
    specialCharactersInNarrative: {
      name: "Special Characters in Narrative",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromAcct.Narrative": "Payment with special chars: @#$%^&*()" },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Special characters should be handled properly"
    },
    
    // Time-based Tests
    futureDatedTransaction: {
      name: "Future Dated Transaction",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].PayHdr.ProcDate": "2025-12-31" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Future processing date not allowed"]
    },
    
    // Compliance Tests
    sanctionedCountry: {
      name: "Sanctioned Country Transaction",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].ToFIData.Country": "XX" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Transactions to sanctioned countries not allowed"]
    }
  },

  // Performance test data
  performanceTests: {
    messageThroughput: {
      name: "Message Throughput Test",
      messageCount: 1000,
      expectedThroughput: 100, // messages per second
      maxLatency: 5000 // 5 seconds
    },
    
    concurrentProcessing: {
      name: "Concurrent Processing Test",
      concurrentUsers: 50,
      messagesPerUser: 20,
      expectedSuccessRate: 95 // 95% success rate
    },
    
    memoryUsage: {
      name: "Memory Usage Test",
      maxMemoryUsage: 512, // MB
      expectedStability: true
    }
  },

  // Load test scenarios
  loadTests: {
    normalLoad: {
      name: "Normal Load",
      messagesPerSecond: 10,
      duration: 300, // 5 minutes
      expectedResponseTime: 2000 // 2 seconds
    },
    
    peakLoad: {
      name: "Peak Load",
      messagesPerSecond: 100,
      duration: 60, // 1 minute
      expectedResponseTime: 5000 // 5 seconds
    },
    
    sustainedLoad: {
      name: "Sustained Load",
      messagesPerSecond: 50,
      duration: 1800, // 30 minutes
      expectedResponseTime: 3000 // 3 seconds
    }
  },

  // Error simulation scenarios
  errorScenarios: {
    kafkaConnectionLoss: {
      name: "Kafka Connection Loss",
      description: "Simulate Kafka connection failure",
      expectedBehavior: "Graceful degradation and retry"
    },
    
    schemaRegistryFailure: {
      name: "Schema Registry Failure",
      description: "Simulate schema registry unavailability",
      expectedBehavior: "Fallback to cached schemas"
    },
    
    databaseConnectionLoss: {
      name: "Database Connection Loss",
      description: "Simulate database connection failure",
      expectedBehavior: "Circuit breaker activation"
    },
    
    memoryExhaustion: {
      name: "Memory Exhaustion",
      description: "Simulate low memory conditions",
      expectedBehavior: "Graceful shutdown and restart"
    }
  }
};
