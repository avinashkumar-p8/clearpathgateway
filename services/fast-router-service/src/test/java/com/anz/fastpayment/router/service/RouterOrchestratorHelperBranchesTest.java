package com.anz.fastpayment.router.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorHelperBranchesTest {

	private RouterOrchestrator newOrch() {
		PuidGenerator puid = mock(PuidGenerator.class);
		when(puid.nextPuid()).thenReturn("P");
		@SuppressWarnings("unchecked") ObjectProvider<com.anz.fastpayment.router.repository.InboundMessageRepository> inProv = (ObjectProvider<com.anz.fastpayment.router.repository.InboundMessageRepository>) mock(ObjectProvider.class);
		@SuppressWarnings("unchecked") ObjectProvider<com.anz.fastpayment.router.repository.UnifiedMessageRepository> uniProv = (ObjectProvider<com.anz.fastpayment.router.repository.UnifiedMessageRepository>) mock(ObjectProvider.class);
		return new RouterOrchestrator(puid, inProv, mock(Iso20022MessageTypeDetector.class), mock(XmlSchemaValidator.class), mock(Iso20022Transformer.class), mock(KafkaPublisher.class), mock(EventPublisher.class), mock(UniqueIdExtractor.class), uniProv, mock(DuplicateChecker.class), new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry());
	}

	@Test
	void mapToAvroEnum_default_unknown_and_extractVersion_null() throws Exception {
		RouterOrchestrator orch = newOrch();
		java.lang.reflect.Method m1 = RouterOrchestrator.class.getDeclaredMethod("mapToAvroEnum", String.class);
		m1.setAccessible(true);
		assertEquals("UNKNOWN", m1.invoke(orch, (Object) null));
		assertEquals("UNKNOWN", m1.invoke(orch, "foo.bar"));

		java.lang.reflect.Method m2 = RouterOrchestrator.class.getDeclaredMethod("extractVersion", String.class);
		m2.setAccessible(true);
		assertNull(m2.invoke(orch, (Object) null));
		assertNull(m2.invoke(orch, "abc"));
	}
}
