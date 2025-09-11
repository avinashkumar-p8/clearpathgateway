package com.anz.fastpayment.sender.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GenericAvroSerializerTest {
	@Test
	void serialize_success() {
		Schema s = new Schema.Parser().parse("{\n \"type\":\"record\",\n \"name\":\"E\",\n \"fields\":[{\"name\":\"a\",\"type\":\"string\"}]\n}");
		GenericRecord rec = new GenericData.Record(s);
		rec.put("a", "b");
		GenericAvroSerializer ser = new GenericAvroSerializer();
		byte[] out = ser.serialize("t", rec);
		assertNotNull(out);
		assertTrue(out.length > 0);
	}

	@Test
	void serialize_null_returnsNull() {
		GenericAvroSerializer ser = new GenericAvroSerializer();
		assertNull(ser.serialize("t", (GenericRecord) null));
	}
}
