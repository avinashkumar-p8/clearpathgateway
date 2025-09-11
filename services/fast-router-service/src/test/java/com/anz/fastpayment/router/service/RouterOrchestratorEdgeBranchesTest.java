package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorEdgeBranchesTest {

	@Test
	void detectTypeNull_abortEarly_noPublish() {
		PuidGenerator puidGen = mock(PuidGenerator.class);
		when(puidGen.nextPuid()).thenReturn("PX");

		@SuppressWarnings("unchecked") ObjectProvider<InboundMessageRepository> inProv = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
		when(inProv.getIfAvailable()).thenReturn(null);

		Iso20022MessageTypeDetector det = mock(Iso20022MessageTypeDetector.class);
		when(det.detectType(anyString())).thenReturn(null);

		XmlSchemaValidator xsd = mock(XmlSchemaValidator.class);
		Iso20022Transformer tx = mock(Iso20022Transformer.class);
		KafkaPublisher pub = mock(KafkaPublisher.class);
		EventPublisher evt = mock(EventPublisher.class);
		UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
		@SuppressWarnings("unchecked") ObjectProvider<UnifiedMessageRepository> uniProv = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);
		when(uniProv.getIfAvailable()).thenReturn(null);
		DuplicateChecker dup = mock(DuplicateChecker.class);

		RouterOrchestrator orch = new RouterOrchestrator(puidGen, inProv, det, xsd, tx, pub, evt, uid, uniProv, dup, new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry());

		assertDoesNotThrow(() -> orch.processInboundXml("<x/>"));
		verifyNoInteractions(pub);
	}

	@Test
	void dedupExtractorThrows_isIgnored_andProcessingContinues() throws Exception {
		PuidGenerator puidGen = mock(PuidGenerator.class);
		when(puidGen.nextPuid()).thenReturn("PY");
		@SuppressWarnings("unchecked") ObjectProvider<InboundMessageRepository> inProv = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
		when(inProv.getIfAvailable()).thenReturn(null);
		Iso20022MessageTypeDetector det = mock(Iso20022MessageTypeDetector.class);
		when(det.detectType(anyString())).thenReturn("pacs.008.001.13");
		XmlSchemaValidator xsd = mock(XmlSchemaValidator.class); // no throw
		Iso20022Transformer tx = mock(Iso20022Transformer.class);
		when(tx.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{\"ok\":true}");
		KafkaPublisher pub = mock(KafkaPublisher.class);
		org.apache.avro.Schema testSchema = new org.apache.avro.Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": \"string\" },\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
		when(pub.getUnifiedSchema()).thenReturn(testSchema);
		EventPublisher evt = mock(EventPublisher.class);
		UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
		when(uid.extractUniqueId(anyString(), anyString())).thenThrow(new RuntimeException("boom"));
		@SuppressWarnings("unchecked") ObjectProvider<UnifiedMessageRepository> uniProv = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);
		when(uniProv.getIfAvailable()).thenReturn(null);
		DuplicateChecker dup = mock(DuplicateChecker.class);
		when(dup.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

		RouterOrchestrator orch = new RouterOrchestrator(puidGen, inProv, det, xsd, tx, pub, evt, uid, uniProv, dup, new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry());
		orch.processInboundXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\"><FIToFICstmrCdtTrf><GrpHdr><MsgId>M</MsgId></GrpHdr><CdtTrfTxInf/></FIToFICstmrCdtTrf></Document>");
		verify(pub, times(1)).publishValidUnified(any(), eq("PY"));
	}
}
