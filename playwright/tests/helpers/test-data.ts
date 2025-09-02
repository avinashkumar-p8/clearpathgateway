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
        SystemId: "FAST_SYSTEM",
        Events: {
          Event: [
            {
              EventCode: "PAYMENT_CREATED",
              EventID: "evt-001-001"
            }
          ]
        },
        EventVersion: "1.0"
      },
      ReplyToQueue: "fast.response.queue",
      ReqMap: "request-mapping-123",
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
            BIC: "DBSASG2X"
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
            Amount: 1000.00,
            AcctUse: "DEBIT"
          },
          ToFIData: {
            Country: "SG",
            BIC: "OCBCSGSG"
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
            Amount: 1000.00,
            Narrative: "Test payment received",
            AcctUse: "CREDIT"
          },
          Fees: ["0.50"]
        }
      ]
    },
    Procctxt: {
      sideEffect: ["none"],
      softFail: ["none"],
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
            BaseAmt: 1000.00,
            BaseCcy: "SGD",
            SenderBIC: "DBSASG2X",
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
                IntrBkSttlmAmt: 1000.00,
                IntrBkSttlmCCY: "SGD",
                IntrBkSttlmDt: "2024-01-15",
                InstgAgt: {
                  BIC: "DBSASG2X"
                },
                InstdAgt: {
                  BIC: "OCBCSGSG"
                },
                Dbtr: {
                  Nm: "Test Sender"
                },
                DbtrAcct: {
                  AcctId: "ACC001"
                },
                DbtrAgt: {
                  BIC: "DBSASG2X"
                },
                CdtrAgt: {
                  BIC: "OCBCSGSG"
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

  // Test scenarios
  testScenarios: {
    validPayment: {
      name: "Valid Payment Message",
      input: "sampleInputMessage",
      expectedStatus: "SUCCESS",
      expectedStatusCode: "200"
    },
    invalidAmount: {
      name: "Invalid Amount",
      input: "sampleInputMessage",
      modification: { "Body.PmtAddRq[0].FromAcct.Amount": -1000.00 },
      expectedStatus: "FAILED",
      expectedStatusCode: "400"
    },
    missingRequiredField: {
      name: "Missing Required Field",
      input: "sampleInputMessage",
      modification: { "Header.MUID": null },
      expectedStatus: "FAILED",
      expectedStatusCode: "400"
    }
  }
};
