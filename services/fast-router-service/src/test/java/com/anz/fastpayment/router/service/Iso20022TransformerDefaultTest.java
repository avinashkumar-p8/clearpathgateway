package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class Iso20022TransformerDefaultTest {

    @Test
    void unsupportedTypePassesThroughRaw() throws Exception {
        TransformationConfigLoader loader = mock(TransformationConfigLoader.class);
        Iso20022Transformer t = new Iso20022Transformer(loader);
        String json = t.toUnifiedJson("<x>y</x>", "unknown.type", "PZ");
        assertTrue(json.contains("\"puid\":\"PZ\""));
        assertTrue(json.contains("raw"));
    }

    @Test
    void missingFieldsProduceValidJson() throws Exception {
        TransformationConfigLoader loader = mock(TransformationConfigLoader.class);
        Iso20022Transformer t = new Iso20022Transformer(loader);
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\"><FIToFICstmrDrctDbt><GrpHdr></GrpHdr><CdtTrfTxInf><PmtId></PmtId></CdtTrfTxInf></FIToFICstmrDrctDbt></Document>";
        String json = t.toUnifiedJson(xml, "pacs.003.001.11", "P1");
        assertTrue(json.contains("\"messageType\":\"PACS_003\""));
        assertTrue(json.contains("\"messageVersion\":\"11\""));
        assertTrue(json.trim().endsWith("}"));
    }
}
