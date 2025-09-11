package com.anz.fastpayment.sender.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ModelsTest {
	@Test
	void pacs002Response_accessors() {
		Pacs002Response r = new Pacs002Response();
		r.setPuid("P"); r.setStatus("ACCEPTED");
		assertEquals("P", r.getPuid());
		assertEquals("ACCEPTED", r.getStatus());
		Pacs002Response r2 = new Pacs002Response("P2", "REJECTED");
		assertEquals("P2", r2.getPuid());
		assertEquals("REJECTED", r2.getStatus());
	}

	@Test
	void pacs002Entity_accessors() {
		Pacs002Entity e = new Pacs002Entity();
		Instant now = Instant.now();
		e.setPuid("P1"); e.setUniqueId("U"); e.setCreatedAt(now); e.setXml("<x/>"); e.setEventJson("{}");
		assertEquals("P1", e.getPuid());
		assertEquals("U", e.getUniqueId());
		assertEquals(now, e.getCreatedAt());
		assertEquals("<x/>", e.getXml());
		assertEquals("{}", e.getEventJson());
	}

	@Test
	void wrapperRequest_nested_accessors() {
		WrapperRequest.ServiceStatus ss = new WrapperRequest.ServiceStatus();
		ss.setStatusCode("0000"); ss.setStatusDesc("OK");
		WrapperRequest.ResHdr rh = new WrapperRequest.ResHdr();
		WrapperRequest.MapEntry me = new WrapperRequest.MapEntry();
		me.setMapKey("k"); me.setMapValue("v");
		rh.setMap(List.of(me));
		WrapperRequest.Trailer tr = new WrapperRequest.Trailer();
		tr.setServiceStatus(ss); tr.setResHdr(rh);
		WrapperRequest.Body body = new WrapperRequest.Body();
		WrapperRequest w = new WrapperRequest();
		w.setTrailer(tr); w.setBody(body);
		assertEquals("0000", w.getTrailer().getServiceStatus().getStatusCode());
		assertEquals("v", w.getTrailer().getResHdr().getMap().get(0).getMapValue());
		assertNotNull(w.getBody());
	}
}
