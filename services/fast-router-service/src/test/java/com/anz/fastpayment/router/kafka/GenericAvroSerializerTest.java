package com.anz.fastpayment.router.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GenericAvroSerializerTest {

    @Test
    void returnsNullForNullRecord() {
        GenericAvroSerializer s = new GenericAvroSerializer();
        assertNull(s.serialize("t", (GenericRecord) null));
    }

    @Test
    void serializesRecordToBytes() {
        String schemaStr = "{\n  \"type\": \"record\",\n  \"name\": \"T\",\n  \"fields\": [{\"name\":\"a\",\"type\":\"string\"}]\n}";
        Schema sc = new Schema.Parser().parse(schemaStr);
        GenericRecord rec = new GenericData.Record(sc);
        rec.put("a", "v");
        GenericAvroSerializer s = new GenericAvroSerializer();
        byte[] bytes = s.serialize("t", rec);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
        byte[] bytes2 = s.serialize("t", new RecordHeaders(), rec);
        assertArrayEquals(bytes, bytes2);
    }
}
