package com.anz.fastpayment.router.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GenericAvroSerializerLifecycleTest {

    @Test
    void configureAndCloseAreNoOps() {
        GenericAvroSerializer s = new GenericAvroSerializer();
        s.configure(Map.of("k","v"), false);
        s.close();
        // also sanity serialize path
        Schema schema = new Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"T\",\n  \"fields\": [{\"name\":\"f\",\"type\":\"string\"}]\n}");
        GenericRecord r = new GenericData.Record(schema);
        r.put("f", "x");
        byte[] b = s.serialize("t", r);
        assertNotNull(b);
    }
}
