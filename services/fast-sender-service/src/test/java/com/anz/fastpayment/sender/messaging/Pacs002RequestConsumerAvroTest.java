package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.model.Pacs002Response;
import com.anz.fastpayment.sender.service.Pacs002Service;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class Pacs002RequestConsumerAvroTest {

	@Test
	void avroFramed_decodes_and_processes() throws Exception {
		Pacs002Service svc = mock(Pacs002Service.class);
		when(svc.handlePacs002Request(any(Pacs002Request.class))).thenReturn(new Pacs002Response("PA","ACCEPTED"));
		Pacs002RequestConsumer c = new Pacs002RequestConsumer(svc);

		String schemaJson = new String(java.util.Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("avro/pacs002-request.avsc")).readAllBytes(), StandardCharsets.UTF_8);
		Schema schema = new Schema.Parser().parse(schemaJson);
		GenericRecord rec = new GenericData.Record(schema);
		rec.put("puid", "PA");
		rec.put("payload", "<x/>");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
		new GenericDatumWriter<GenericRecord>(schema).write(rec, enc);
		enc.flush();
		byte[] body = out.toByteArray();
		byte[] framed = new byte[body.length + 5];
		framed[0] = 0; // magic
		framed[1]=0; framed[2]=0; framed[3]=0; framed[4]=1; // schema id
		System.arraycopy(body, 0, framed, 5, body.length);

		ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("pacs002-requests", 0, 10L, "K", framed);
		c.onMessage(record);
		verify(svc, times(1)).handlePacs002Request(any(Pacs002Request.class));
	}
}
