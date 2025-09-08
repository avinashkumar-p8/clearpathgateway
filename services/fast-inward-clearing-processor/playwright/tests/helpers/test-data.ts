export const testData = {
  // Sample InputMessage structure matching the actual message structure from test-message.json
  sampleInputMessage: {
    Header: {
      ComponentName: "PSPAPFAFAST",
      UUID: "G3I400071311436B",
      EventInfo: {
        EventCode: "P.PSP.STS.M.OP_RPI.100",
        EventDescription: "Payment request received in PSP",
        EventID: "185bd46a-d178-453b-a808-4eeedff5427b",
        EventType: "PE",
        EventProducer: "Clear Path Gateway",
        EventTS: "2025-05-08T09:02:10.765",
        EventTopics: "<KAFKA Topic>",
        SystemId: null,
        Events: {
          Event: [
            {
              EventCode: "I.PSP.STS.M.OP_RPI.100",
              EventID: "185bd46a-d178-453b-a808-3eeedff5427a"
            },
            {
              EventCode: "P.PSP.STS.M.OP_RPI.100",
              EventID: "185bd46a-d178-453b-a808-4eeedff5427b"
            }
          ]
        },
        EventVersion: null
      },
      ReplyToQueue: "PPSP.PPORCH.GPAFL.RSP.01",
      ReqMap: null,
      MUID: "msg-unique-id-12345",
      Channel: "G3I",
      Direction: "I",
      RcvdTS: "2025-06-10T21:08:21",
      DomainName: "PAYMENTS",
      DomainType: "PAYMENT"
    },
    Body: {
      PmtAddRq: [
        {
          RqUID: "20250424UOVBSGSGBRT1XXXXXX",
          MsgHdr: {
            ClientDt: "2025-06-10T21:08:21",
            ClientName: "G3I",
            PartyId: "749440SGD000001",
            Version: "1.0"
          },
          PayHdr: {
            PODsID: "G3I400071311436B",
            PaymentID: "20250424UOVBSGSGBRT1XXXXXX",
            ThirdPartyPayID: "NC2503XXXX",
            PaymentTRN: "20250424UOVBSGSGBRT1XXXXXX",
            PaymentRetRef: "FPS00",
            ProcDate: "2025-06-10"
          },
          FromFIData: {
            Country: "SG",
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
            CurCode: "SGD",
            Amount: 2182.14,
            AcctUse: "BUSINESS"
          },
          ToFIData: {
            Country: "SG",
            BIC: "OCBCSGSGXXX"
          },
          Clearing: {
            ClearPref: "FAST"
          },
          ToBene: {
            Name: "XX LTD.",
            Country: "SG",
            Message: "XXXXXXXX"
          },
          ToAcct: {
            AcctId: "80XXXX",
            CurCode: "SGD",
            Amount: 2182.14,
            Narrative: "12345      INWGDR+API20250424211XXXXXX",
            AcctUse: "BUSINESS"
          },
          Fees: []
        }
      ],
      Procctxt: {
        sideEffect: [],
        softFail: [],
        PmtDtls: {
          PmtCtxt: {
            PuId: "G3I400071311436B",
            IntnSrc: {
              type: "CLRG",
              value: "G3"
            }
          },
          ProcCtryCd: "SG",
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
              BaseAmt: 215,
              BaseCcy: "SGD",
              SenderBIC: "SACHSGS1XXX",
              ProcCtryCd: "SG",
              Department: "SGIFAST",
              MsgId: "NA132504110957516877A9C100364A963O",
              Direction: "I"
            },
            MsgAddRq: {
              OrigMsg: "",
              MsgDtls: {
                DrctDbtTxInf: {
                  PmtId: {
                    InstrId: "20250411MBBESGS2BRT8705045",
                    TxId: "20250411MBBESGS2BRT8705045",
                    EndToEndId: "NC2503XXXX",
                    ClrSysRef: "001"
                  },
                  IntrBkSttlmAmt: 215,
                  IntrBkSttlmCCY: "SGD",
                  IntrBkSttlmDt: "2025-05-30",
                  InstgAgt: {
                    BIC: "OCBCSGSGXXX"
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
                    BIC: "OCBCSGSGXXX"
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
    }
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
      Procctxt: {
        // ... same processing context
      },
      messages: [
        // ... same messages structure
      ]
    },
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
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmAmt": -1000.00 },
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
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmCCY": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid currency code: INVALID"]
    },
    
    invalidBIC: {
      name: "Invalid BIC Format",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.InstgAgt.BIC": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid BIC format: INVALID"]
    },
    
    invalidCountry: {
      name: "Invalid Country Code",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgCtxt.ProcCtryCd": "XX" },
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
      modification: { "Body.messages[0].instruction.MsgCtxt.InstdClrgPref": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid clearing preference: INVALID"]
    },
    
    invalidPaymentCategory: {
      name: "Invalid Payment Category",
      input: "sampleInputMessage",
      modification: { "Procctxt.PmtDtls.actlMtdOfPmtCtgry": "INVALID" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Invalid payment category: INVALID"]
    },
    
    // Business Logic Tests
    highValueTransaction: {
      name: "High Value Transaction",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmAmt": 1000000.00 },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "High value transaction should pass additional validation"
    },
    
    crossCurrencyTransaction: {
      name: "Cross Currency Transaction",
      input: "sampleInputMessage",
      modification: { 
        "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmCCY": "USD"
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
      modification: { "Body.messages": Array(1000).fill({ instruction: { MsgDef: { MsgType: "PAYMENT" } } }) },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Large message should be processed within SLA"
    },
    
    // Edge Cases
    zeroAmount: {
      name: "Zero Amount Transaction",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmAmt": 0.00 },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Transaction amount cannot be zero"]
    },
    
    maximumAmount: {
      name: "Maximum Amount Transaction",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmAmt": 999999999.99 },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Maximum allowed amount should be processed"
    },
    
    // Special Characters
    specialCharactersInNarrative: {
      name: "Special Characters in Narrative",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.RmtInf.Ustrd": "Payment with special chars: @#$%^&*()" },
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200",
      description: "Special characters should be handled properly"
    },
    
    // Time-based Tests
    futureDatedTransaction: {
      name: "Future Dated Transaction",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.IntrBkSttlmDt": "2025-12-31" },
      expectedStatus: "FAILED",
      expectedStatusCode: "400",
      expectedErrors: ["Future processing date not allowed"]
    },
    
    // Compliance Tests
    sanctionedCountry: {
      name: "Sanctioned Country Transaction",
      input: "sampleInputMessage",
      modification: { "Body.messages[0].instruction.MsgAddRq.MsgDtls.DrctDbtTxInf.DbtrAcct.Country": "XX" },
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
