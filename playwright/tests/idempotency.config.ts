// Loosely coupled configuration - easy to change topics in the future
export const testConfig = {
  // Kafka Configuration
  kafkaBrokers: ['localhost:9092'],
  schemaRegistryUrl: 'http://localhost:8081',
  
  // Topic Configuration - Using same topic for now to simplify testing
  topics: {
    input: 'transactions.incoming',      // Where events are produced
    output: 'transactions.incoming',     // TEMPORARY: Same as input for testing
    dlq: 'transactions.dlq'             // Dead letter queue
  },
  
  // Service Configuration
  serviceUrl: 'http://localhost:8080',
  
  // Test Configuration
  testTimeout: 30000,
  messageWaitTime: 5000,
  
  // Avro Schema Configuration - Real Banking Schema
  avroSchemas: {
    unifiedPaymentMessage: {
      type: 'record' as const,
      name: 'UnifiedPaymentMessage',
      namespace: 'com.anz.fastpayment.inward.avro',
      fields: [
        {
          name: 'Header',
          type: {
            type: 'record',
            name: 'UnifiedMessageHeader',
            fields: [
              { name: 'ComponentName', type: 'string' },
              { name: 'UUID', type: 'string' },
              {
                name: 'EventInfo',
                type: {
                  type: 'record',
                  name: 'EventInformation',
                  fields: [
                    { name: 'EventCode', type: 'string' },
                    { name: 'EventDescription', type: 'string' },
                    { name: 'EventID', type: 'string' },
                    { name: 'EventType', type: 'string' },
                    { name: 'EventProducer', type: 'string' },
                    { name: 'EventTS', type: 'string' },
                    { name: 'EventTopics', type: 'string' },
                    { name: 'SystemId', type: ['null', 'string'] },
                    {
                      name: 'Events',
                      type: {
                        type: 'record',
                        name: 'EventCollection',
                        fields: [
                          {
                            name: 'Event',
                            type: {
                              type: 'array',
                              items: {
                                type: 'record',
                                name: 'EventDetail',
                                fields: [
                                  { name: 'EventCode', type: 'string' },
                                  { name: 'EventID', type: 'string' }
                                ]
                              }
                            }
                          }
                        ]
                      }
                    },
                    { name: 'EventVersion', type: ['null', 'string'] }
                  ]
                }
              },
              { name: 'ReplyToQueue', type: 'string' },
              { name: 'ReqMap', type: ['null', 'string'] },
              { name: 'MUID', type: 'string' },
              { name: 'Channel', type: 'string' },
              { name: 'Direction', type: 'string' },
              { name: 'RcvdTS', type: 'string' },
              { name: 'DomainName', type: 'string' },
              { name: 'DomainType', type: 'string' }
            ]
          }
        },
        {
          name: 'Body',
          type: {
            type: 'record',
            name: 'MessageBody',
            fields: [
              {
                name: 'PmtAddRq',
                type: {
                  type: 'array',
                  items: {
                    type: 'record',
                    name: 'PaymentAddRequest',
                    fields: [
                      { name: 'RqUID', type: 'string' },
                      {
                        name: 'MsgHdr',
                        type: {
                          type: 'record',
                          name: 'PaymentMessageHeader',
                          fields: [
                            { name: 'ClientDt', type: 'string' },
                            { name: 'ClientName', type: 'string' },
                            { name: 'PartyId', type: 'string' },
                            { name: 'Version', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'PayHdr',
                        type: {
                          type: 'record',
                          name: 'PaymentHeader',
                          fields: [
                            { name: 'PODsID', type: 'string' },
                            { name: 'PaymentID', type: 'string' },
                            { name: 'ThirdPartyPayID', type: 'string' },
                            { name: 'PaymentTRN', type: 'string' },
                            { name: 'PaymentRetRef', type: 'string' },
                            { name: 'ProcDate', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'FromFIData',
                        type: {
                          type: 'record',
                          name: 'FromFinancialInstitution',
                          fields: [
                            { name: 'Country', type: 'string' },
                            { name: 'BIC', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'FromCust',
                        type: {
                          type: 'record',
                          name: 'FromCustomer',
                          fields: [
                            { name: 'Name', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'FromAcct',
                        type: {
                          type: 'record',
                          name: 'FromAccount',
                          fields: [
                            { name: 'AcctId', type: 'string' },
                            { name: 'AcctSys', type: 'string' },
                            { name: 'AcctGrp', type: 'string' },
                            { name: 'Name', type: 'string' },
                            { name: 'PmtAuthMethod', type: 'string' },
                            { name: 'Narrative', type: 'string' },
                            { name: 'CurCode', type: 'string' },
                            { name: 'Amount', type: 'double' },
                            { name: 'AcctUse', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'ToFIData',
                        type: {
                          type: 'record',
                          name: 'ToFinancialInstitution',
                          fields: [
                            { name: 'Country', type: 'string' },
                            { name: 'BIC', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'Clearing',
                        type: {
                          type: 'record',
                          name: 'ClearingInfo',
                          fields: [
                            { name: 'ClearPref', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'ToBene',
                        type: {
                          type: 'record',
                          name: 'ToBeneficiary',
                          fields: [
                            { name: 'Name', type: 'string' },
                            { name: 'Country', type: 'string' },
                            { name: 'Message', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'ToAcct',
                        type: {
                          type: 'record',
                          name: 'ToAccount',
                          fields: [
                            { name: 'AcctId', type: 'string' },
                            { name: 'CurCode', type: 'string' },
                            { name: 'Amount', type: 'double' },
                            { name: 'Narrative', type: 'string' },
                            { name: 'AcctUse', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'Fees',
                        type: {
                          type: 'array',
                          items: 'string'
                        }
                      }
                    ]
                  }
                }
              }
            ]
          }
        },
        {
          name: 'Procctxt',
          type: {
            type: 'record',
            name: 'ProcessingContext',
            fields: [
              {
                name: 'sideEffect',
                type: {
                  type: 'array',
                  items: 'string'
                }
              },
              {
                name: 'softFail',
                type: {
                  type: 'array',
                  items: 'string'
                }
              },
              {
                name: 'PmtDtls',
                type: {
                  type: 'record',
                  name: 'PaymentDetails',
                  fields: [
                    {
                      name: 'PmtCtxt',
                      type: {
                        type: 'record',
                        name: 'PaymentContext',
                        fields: [
                          { name: 'PuId', type: 'string' },
                          {
                            name: 'IntnSrc',
                            type: {
                              type: 'record',
                              name: 'IntentionSource',
                              fields: [
                                { name: 'type', type: 'string' },
                                { name: 'value', type: 'string' }
                              ]
                            }
                          }
                        ]
                      }
                    },
                    { name: 'ProcCtryCd', type: 'string' },
                    { name: 'InstdClrgPref', type: 'string' },
                    { name: 'InstdMoPCat', type: 'string' },
                    { name: 'PmtCtgry', type: 'string' },
                    { name: 'actClrMethod', type: 'string' },
                    { name: 'actlMtdOfPmtCtgry', type: 'string' },
                    { name: 'FIDCIdentifier', type: 'string' },
                    { name: 'FICCIdentifier', type: 'string' },
                    { name: 'VAM', type: 'string' },
                    { name: 'derivedDRAccountNo', type: 'string' },
                    { name: 'derivedDRAccountSys', type: 'string' },
                    { name: 'derivedDRBookCode', type: 'string' }
                  ]
                }
              }
            ]
          }
        },
        {
          name: 'messages',
          type: {
            type: 'array',
            items: {
              type: 'record',
              name: 'MessageInstruction',
              fields: [
                {
                  name: 'instruction',
                  type: {
                    type: 'record',
                    name: 'Instruction',
                    fields: [
                      {
                        name: 'MsgDef',
                        type: {
                          type: 'record',
                          name: 'MessageDefinition',
                          fields: [
                            { name: 'MsgType', type: 'string' },
                            { name: 'Schema', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'MsgCtxt',
                        type: {
                          type: 'record',
                          name: 'MessageContext',
                          fields: [
                            { name: 'OrigMsgTyp', type: 'string' },
                            { name: 'InstdClrgPref', type: 'string' },
                            { name: 'InstdMoPCat', type: 'string' },
                            { name: 'Site', type: 'string' },
                            { name: 'BaseAmt', type: 'double' },
                            { name: 'BaseCcy', type: 'string' },
                            { name: 'SenderBIC', type: 'string' },
                            { name: 'ProcCtryCd', type: 'string' },
                            { name: 'Department', type: 'string' },
                            { name: 'MsgId', type: 'string' },
                            { name: 'Direction', type: 'string' }
                          ]
                        }
                      },
                      {
                        name: 'MsgAddRq',
                        type: {
                          type: 'record',
                          name: 'MessageAddRequest',
                          fields: [
                            { name: 'OrigMsg', type: 'string' },
                            {
                              name: 'MsgDtls',
                              type: {
                                type: 'record',
                                name: 'MessageDetails',
                                fields: [
                                  {
                                    name: 'DrctDbtTxInf',
                                    type: {
                                      type: 'record',
                                      name: 'DirectDebitTransactionInfo',
                                      fields: [
                                        {
                                          name: 'PmtId',
                                          type: {
                                            type: 'record',
                                            name: 'PaymentId',
                                            fields: [
                                              { name: 'InstrId', type: 'string' },
                                              { name: 'TxId', type: 'string' },
                                              { name: 'EndToEndId', type: 'string' },
                                              { name: 'ClrSysRef', type: 'string' }
                                            ]
                                          }
                                        },
                                        { name: 'IntrBkSttlmAmt', type: 'double' },
                                        { name: 'IntrBkSttlmCCY', type: 'string' },
                                        { name: 'IntrBkSttlmDt', type: 'string' },
                                        {
                                          name: 'InstgAgt',
                                          type: {
                                            type: 'record',
                                            name: 'InstructingAgent',
                                            fields: [
                                              { name: 'BIC', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'InstdAgt',
                                          type: {
                                            type: 'record',
                                            name: 'InstructedAgent',
                                            fields: [
                                              { name: 'BIC', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'Dbtr',
                                          type: {
                                            type: 'record',
                                            name: 'Debtor',
                                            fields: [
                                              { name: 'Nm', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'DbtrAcct',
                                          type: {
                                            type: 'record',
                                            name: 'DebtorAccount',
                                            fields: [
                                              { name: 'AcctId', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'DbtrAgt',
                                          type: {
                                            type: 'record',
                                            name: 'DebtorAgent',
                                            fields: [
                                              { name: 'BIC', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'CdtrAgt',
                                          type: {
                                            type: 'record',
                                            name: 'CreditorAgent',
                                            fields: [
                                              { name: 'BIC', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'Cdtr',
                                          type: {
                                            type: 'record',
                                            name: 'Creditor',
                                            fields: [
                                              { name: 'Nm', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'CdtrAcct',
                                          type: {
                                            type: 'record',
                                            name: 'CreditorAccount',
                                            fields: [
                                              { name: 'AcctId', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'Purp',
                                          type: {
                                            type: 'record',
                                            name: 'Purpose',
                                            fields: [
                                              { name: 'Cd', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'RmtInf',
                                          type: {
                                            type: 'record',
                                            name: 'RemittanceInformation',
                                            fields: [
                                              { name: 'Ustrd', type: 'string' }
                                            ]
                                          }
                                        },
                                        {
                                          name: 'DrctDbtTx',
                                          type: {
                                            type: 'record',
                                            name: 'DirectDebitTransaction',
                                            fields: [
                                              {
                                                name: 'MndtRltdInf',
                                                type: {
                                                  type: 'record',
                                                  name: 'MandateRelatedInformation',
                                                  fields: [
                                                    { name: 'MndtId', type: 'string' }
                                                  ]
                                                }
                                              }
                                            ]
                                          }
                                        }
                                      ]
                                    }
                                  }
                                ]
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }
          }
        }
      ]
    }
  }
};

// Helper function to get topic names - makes it easy to change in the future
export const getTopic = (topicType: 'input' | 'output' | 'dlq') => testConfig.topics[topicType];

// Helper function to create test messages with real banking schema
export const createTestMessage = (transactionId: string, amount: number, currency: string, country: string, muid?: string) => {
  const messageMuid = muid || `MUID-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  const currentDate = new Date().toISOString().split('T')[0];
  const currentDateTime = new Date().toISOString();
  
  return {
    Header: {
      ComponentName: "PSPAPFAFAST",
      UUID: messageMuid,
      EventInfo: {
        EventCode: "P.PSP.STS.M.OP_RPI.100",
        EventDescription: "Payment request received in PSP",
        EventID: `185bd46a-d178-453b-a808-${Math.random().toString(36).substr(2, 9)}`,
        EventType: "PE",
        EventProducer: "Clear Path Gateway",
        EventTS: currentDateTime,
        EventTopics: "<KAFKA Topic>",
        SystemId: null,
        Events: {
          Event: [
            {
              EventCode: "I.PSP.STS.M.OP_RPI.100",
              EventID: `185bd46a-d178-453b-a808-${Math.random().toString(36).substr(2, 9)}`
            },
            {
              EventCode: "P.PSP.STS.M.OP_RPI.100",
              EventID: `185bd46a-d178-453b-a808-${Math.random().toString(36).substr(2, 9)}`
            }
          ]
        },
        EventVersion: null
      },
      ReplyToQueue: "PPSP.PPORCH.GPAFL.RSP.01",
      ReqMap: null,
      MUID: messageMuid,
      Channel: "G3I",
      Direction: "I",
      RcvdTS: currentDateTime,
      DomainName: "PAYMENTS",
      DomainType: "PAYMENT"
    },
    Body: {
      PmtAddRq: [
        {
          RqUID: transactionId,
          MsgHdr: {
            ClientDt: currentDateTime,
            ClientName: "G3I",
            PartyId: "749440SGD000001",
            Version: "1.0"
          },
          PayHdr: {
            PODsID: messageMuid,
            PaymentID: transactionId,
            ThirdPartyPayID: "NC2503XXXX",
            PaymentTRN: transactionId,
            PaymentRetRef: "FPS00",
            ProcDate: currentDate
          },
          FromFIData: {
            Country: country,
            BIC: "XXXBSGSXXXX"
          },
          FromCust: {
            Name: "Test Sender Pte Ltd"
          },
          FromAcct: {
            AcctId: "783451100000001",
            AcctSys: "VAM",
            AcctGrp: "SGB",
            Name: "749440SGD000001",
            PmtAuthMethod: "AFPONLY",
            Narrative: `DDI+${messageMuid}+OTHR+FPS00+Test Sender Pte Ltd+NC250`,
            CurCode: currency,
            Amount: amount,
            AcctUse: "BUSINESS"
          },
          ToFIData: {
            Country: country,
            BIC: "OCBCSGSGXXX"
          },
          Clearing: {
            ClearPref: "FAST"
          },
          ToBene: {
            Name: "Test Receiver LTD.",
            Country: country,
            Message: "Test payment"
          },
          ToAcct: {
            AcctId: "80XXXX",
            CurCode: currency,
            Amount: amount,
            Narrative: `12345 INWGDR+API${transactionId}`,
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
          PuId: messageMuid,
          IntnSrc: {
            type: "CLRG",
            value: "G3"
          }
        },
        ProcCtryCd: country,
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
            BaseAmt: amount,
            BaseCcy: currency,
            SenderBIC: "SACHSGS1XXX",
            ProcCtryCd: country,
            Department: "SGIFAST",
            MsgId: `NA${Date.now()}`,
            Direction: "I"
          },
          MsgAddRq: {
            OrigMsg: "",
            MsgDtls: {
              DrctDbtTxInf: {
                PmtId: {
                  InstrId: `INSTR-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
                  TxId: transactionId,
                  EndToEndId: "NC2503XXXX",
                  ClrSysRef: "001"
                },
                IntrBkSttlmAmt: amount,
                IntrBkSttlmCCY: currency,
                IntrBkSttlmDt: currentDate,
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
                  Ustrd: "Test payment"
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
};
