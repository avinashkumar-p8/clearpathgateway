package com.anz.fastpayment.inward.config;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spring.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import jakarta.annotation.PostConstruct;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Configuration
@Profile("local")
public class SpannerLocalSchema {

    private static final Logger log = LoggerFactory.getLogger(SpannerLocalSchema.class);

    private final SpannerDatabaseAdminTemplate adminTemplate;

    @Value("${spring.cloud.gcp.spanner.instance-id}")
    private String instanceId;

    @Value("${spring.cloud.gcp.spanner.database}")
    private String databaseId;

    @Value("${spring.cloud.gcp.project-id:${SPRING_CLOUD_GCP_PROJECT_ID:local-project}}")
    private String projectId;

    public SpannerLocalSchema(SpannerDatabaseAdminTemplate adminTemplate) {
        this.adminTemplate = adminTemplate;
    }

    @PostConstruct
    public void ensureTables() {
        log.info("🚀 SpannerLocalSchema @PostConstruct method called - ensuring instance and database exist");
        try {
            // Retry instance/db ensure to tolerate emulator cold start
            int attempts = 0;
            while (true) {
                try {
                    ensureInstanceAndDatabase();
                    break;
                } catch (Exception initEx) {
                    attempts++;
                    if (attempts >= 3) throw initEx;
                    log.info("Spanner emulator not ready yet, retrying instance/db ensure (attempt {} of 3)", attempts + 1);
                    try { Thread.sleep(500L * attempts); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }
            
            // Drop and recreate message_unique_ids table (required for idempotency)
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList("DROP TABLE message_unique_ids"), true);
                log.info("Dropped existing message_unique_ids table");
            } catch (Exception ce) {
                log.info("No existing message_unique_ids table to drop: {}", ce.getMessage());
            }
            
            String ddl1 = "CREATE TABLE message_unique_ids (" +
                    " muid STRING(255) NOT NULL,\n" +
                    " message_topic STRING(100) NOT NULL,\n" +
                    " message_partition INT64 NOT NULL,\n" +
                    " message_offset INT64 NOT NULL,\n" +
                    " event_payload STRING(MAX),\n" +
                    " processing_status STRING(50) NOT NULL,\n" +
                    " created_at TIMESTAMP NOT NULL,\n" +
                    " processed_at TIMESTAMP,\n" +
                    " is_active BOOL NOT NULL\n" +
                    ") PRIMARY KEY (muid)";
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(ddl1), true);
                log.info("Created table message_unique_ids in Spanner emulator");
            } catch (Exception ce) {
                log.error("Failed to create message_unique_ids table: {}", ce.getMessage());
                throw ce;
            }

            // Drop and recreate countries table
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList("DROP TABLE countries"), true);
                log.info("Dropped existing countries table");
            } catch (Exception ce) {
                log.info("No existing countries table to drop: {}", ce.getMessage());
            }
            
            String ddl2 = "CREATE TABLE countries (" +
                    " id INT64 NOT NULL,\n" +
                    " code STRING(10) NOT NULL,\n" +
                    " name STRING(100) NOT NULL,\n" +
                    " iso_code STRING(3) NOT NULL,\n" +
                    " currency_code STRING(3) NOT NULL,\n" +
                    " is_active BOOL NOT NULL,\n" +
                    " created_at TIMESTAMP NOT NULL,\n" +
                    " updated_at TIMESTAMP,\n" +
                    " created_by STRING(100),\n" +
                    " updated_by STRING(100),\n" +
                    " validation_rules ARRAY<STRING(MAX)>\n" +
                    ") PRIMARY KEY (id)";
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(ddl2), true);
                log.info("Created table countries in Spanner emulator");
            } catch (Exception ce) {
                log.error("Failed to create countries table: {}", ce.getMessage());
                throw ce;
            }

            // Drop and recreate currencies table
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList("DROP TABLE currencies"), true);
                log.info("Dropped existing currencies table");
            } catch (Exception ce) {
                log.info("No existing currencies table to drop: {}", ce.getMessage());
            }
            
            String ddl3 = "CREATE TABLE currencies (" +
                    " id INT64 NOT NULL,\n" +
                    " code STRING(3) NOT NULL,\n" +
                    " name STRING(100) NOT NULL,\n" +
                    " symbol STRING(10),\n" +
                    " is_active BOOL NOT NULL,\n" +
                    " decimal_places INT64 NOT NULL,\n" +
                    " valid_countries ARRAY<STRING(10)>,\n" +
                    " created_at TIMESTAMP NOT NULL,\n" +
                    " updated_at TIMESTAMP,\n" +
                    " created_by STRING(100),\n" +
                    " updated_by STRING(100)\n" +
                    ") PRIMARY KEY (id)";
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(ddl3), true);
                log.info("Created table currencies in Spanner emulator");
            } catch (Exception ce) {
                log.error("Failed to create currencies table: {}", ce.getMessage());
                throw ce;
            }

            // Drop and recreate TransactionMessages table
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList("DROP TABLE TransactionMessages"), true);
                log.info("Dropped existing TransactionMessages table");
            } catch (Exception ce) {
                log.info("No existing TransactionMessages table to drop: {}", ce.getMessage());
            }
            
            String ddl4 = "CREATE TABLE TransactionMessages (" +
                    " muid STRING(64) NOT NULL,\n" +
                    " transaction_id STRING(64),\n" +
                    " message_type STRING(64),\n" +
                    " received_at TIMESTAMP,\n" +
                    " raw_message STRING(MAX),\n" +
                    " status STRING(32),\n" +
                    " error STRING(MAX)\n" +
                    ") PRIMARY KEY (muid)";
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(ddl4), true);
                log.info("Created table TransactionMessages in Spanner emulator");
            } catch (Exception ce) {
                log.error("Failed to create TransactionMessages table: {}", ce.getMessage());
                throw ce;
            }

            // Drop and recreate IdempotencyCache table
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList("DROP TABLE IdempotencyCache"), true);
                log.info("Dropped existing IdempotencyCache table");
            } catch (Exception ce) {
                log.info("No existing IdempotencyCache table to drop: {}", ce.getMessage());
            }
            
            String ddl5 = "CREATE TABLE IdempotencyCache (" +
                    " muid STRING(64) NOT NULL,\n" +
                    " topic STRING(128),\n" +
                    " partition_id INT64,\n" +
                    " offset INT64,\n" +
                    " created_at TIMESTAMP,\n" +
                    " expires_at TIMESTAMP\n" +
                    ") PRIMARY KEY (muid)";
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(ddl5), true);
                log.info("Created table IdempotencyCache in Spanner emulator");
            } catch (Exception ce) {
                log.error("Failed to create IdempotencyCache table: {}", ce.getMessage());
                throw ce;
            }

            // Drop and recreate ProcessingEvents table
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList("DROP TABLE ProcessingEvents"), true);
                log.info("Dropped existing ProcessingEvents table");
            } catch (Exception ce) {
                log.info("No existing ProcessingEvents table to drop: {}", ce.getMessage());
            }
            
            String ddl6 = "CREATE TABLE ProcessingEvents (" +
                    " event_id STRING(64) NOT NULL,\n" +
                    " muid STRING(64),\n" +
                    " event_type STRING(64),\n" +
                    " created_at TIMESTAMP,\n" +
                    " event_data STRING(MAX)\n" +
                    ") PRIMARY KEY (event_id)";
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(ddl6), true);
                log.info("Created table ProcessingEvents in Spanner emulator");
            } catch (Exception ce) {
                log.error("Failed to create ProcessingEvents table: {}", ce.getMessage());
                throw ce;
            }

            // Insert dummy data for countries
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(
                    "INSERT INTO countries (id, code, name, iso_code, currency_code, is_active, created_at, created_by) VALUES " +
                    "(1, 'SG', 'Singapore', 'SGP', 'SGD', true, CURRENT_TIMESTAMP(), 'system'), " +
                    "(2, 'AU', 'Australia', 'AUS', 'AUD', true, CURRENT_TIMESTAMP(), 'system'), " +
                    "(3, 'US', 'United States', 'USA', 'USD', true, CURRENT_TIMESTAMP(), 'system'), " +
                    "(4, 'GB', 'United Kingdom', 'GBR', 'GBP', true, CURRENT_TIMESTAMP(), 'system'), " +
                    "(5, 'JP', 'Japan', 'JPN', 'JPY', true, CURRENT_TIMESTAMP(), 'system')"
                ), true);
                log.info("Inserted dummy data into countries table");
            } catch (Exception ce) {
                String msg = ce.getMessage() == null ? "" : ce.getMessage();
                if (msg.contains("ALREADY_EXISTS") || msg.contains("duplicate") || msg.contains("already exists")) {
                    log.info("Countries data already exists; skipping insert");
                } else {
                    log.warn("Failed to insert countries data: {}", ce.getMessage());
                }
            }

            // Insert dummy data for currencies
            try {
                adminTemplate.executeDdlStrings(Collections.singletonList(
                    "INSERT INTO currencies (id, code, name, symbol, is_active, decimal_places, created_at, created_by) VALUES " +
                    "(1, 'SGD', 'Singapore Dollar', 'S$', true, 2, CURRENT_TIMESTAMP(), 'system'), " +
                    "(2, 'AUD', 'Australian Dollar', 'A$', true, 2, CURRENT_TIMESTAMP(), 'system'), " +
                    "(3, 'USD', 'US Dollar', '$', true, 2, CURRENT_TIMESTAMP(), 'system'), " +
                    "(4, 'GBP', 'British Pound', '£', true, 2, CURRENT_TIMESTAMP(), 'system'), " +
                    "(5, 'JPY', 'Japanese Yen', '¥', true, 0, CURRENT_TIMESTAMP(), 'system')"
                ), true);
                log.info("Inserted dummy data into currencies table");
            } catch (Exception ce) {
                String msg = ce.getMessage() == null ? "" : ce.getMessage();
                if (msg.contains("ALREADY_EXISTS") || msg.contains("duplicate") || msg.contains("already exists")) {
                    log.info("Currencies data already exists; skipping insert");
                } else {
                    log.warn("Failed to insert currencies data: {}", ce.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("❌ Spanner emulator init failed: {}", e.getMessage(), e);
        }
    }

    private void ensureInstanceAndDatabase() throws Exception {
        log.info("🔧 ensureInstanceAndDatabase() called - creating instance and database if needed");
        SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
        try (Spanner spanner = options.getService()) {
            InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
            DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

            InstanceId iid = InstanceId.of(projectId, instanceId);
            boolean instanceExists;
            try {
                Instance i = instanceAdminClient.getInstance(iid.getInstance());
                instanceExists = i != null;
            } catch (Exception e) {
                instanceExists = false;
            }
            if (!instanceExists) {
                log.info("Creating Spanner emulator instance {} in project {}", instanceId, projectId);
                InstanceInfo info = InstanceInfo.newBuilder(iid)
                        .setDisplayName("Local Instance")
                        .setInstanceConfigId(InstanceConfigId.of(projectId, "emulator-config"))
                        .setNodeCount(1)
                        .build();
                try {
                    instanceAdminClient.createInstance(info).get(30, TimeUnit.SECONDS);
                    log.info("✅ Instance created successfully");
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("ALREADY_EXISTS")) {
                        log.info("✅ Instance already exists, continuing...");
                    } else {
                        throw e;
                    }
                }
            } else {
                log.info("✅ Instance already exists, continuing...");
            }

            DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
            boolean dbExists;
            try {
                databaseAdminClient.getDatabase(instanceId, databaseId);
                dbExists = true;
            } catch (Exception e) {
                dbExists = false;
            }
            if (!dbExists) {
                log.info("Creating Spanner emulator database {} on instance {}", databaseId, instanceId);
                try {
                    databaseAdminClient.createDatabase(instanceId, databaseId, Collections.emptyList()).get(30, TimeUnit.SECONDS);
                    log.info("✅ Database created successfully");
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("ALREADY_EXISTS")) {
                        log.info("✅ Database already exists, continuing...");
                    } else {
                        throw e;
                    }
                }
            } else {
                log.info("✅ Database already exists, continuing...");
            }
        }
    }
}
