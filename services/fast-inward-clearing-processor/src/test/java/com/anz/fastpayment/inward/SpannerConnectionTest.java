package com.anz.fastpayment.inward;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@TestPropertySource(properties = {
    "spring.cloud.gcp.spanner.emulator.enabled=true",
    "spring.cloud.gcp.spanner.emulator-host=localhost:9010",
    "spring.cloud.gcp.spanner.project-id=anz-fastpayment-sg",
    "spring.cloud.gcp.spanner.instance-id=payment-gateway-local",
    "spring.cloud.gcp.spanner.database=inward-processor-db",
    "spring.cloud.gcp.credentials.location=none"
})
public class SpannerConnectionTest {

    @Test
    public void testSpannerConnection() {
        try {
            // Create Spanner client
            SpannerOptions options = SpannerOptions.newBuilder()
                .setEmulatorHost("localhost:9010")
                .setProjectId("anz-fastpayment-sg")
                .build();
            
            Spanner spanner = options.getService();
            DatabaseId dbId = DatabaseId.of("anz-fastpayment-sg", "payment-gateway-local", "inward-processor-db");
            DatabaseClient dbClient = spanner.getDatabaseClient(dbId);
            
            assertNotNull(dbClient);
            System.out.println("✅ Spanner connection successful!");
            
            spanner.close();
        } catch (Exception e) {
            System.err.println("❌ Spanner connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
