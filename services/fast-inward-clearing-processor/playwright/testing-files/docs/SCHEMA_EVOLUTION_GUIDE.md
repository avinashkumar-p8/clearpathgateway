# Schema Evolution Guide

## Overview
This guide explains how to handle schema changes in a production environment without manual intervention.

## Schema Evolution Strategies

### 1. Backward Compatibility
- ✅ **Add new optional fields** (with default values)
- ✅ **Add new union types** (e.g., `["null", "string"]`)
- ❌ **Remove fields** (breaks backward compatibility)
- ❌ **Change field types** (breaks backward compatibility)

### 2. Forward Compatibility
- ✅ **Add new required fields** (consumers can ignore)
- ✅ **Remove optional fields**
- ❌ **Change existing field types**

### 3. Schema Versioning
```json
{
  "name": "InputMessage",
  "namespace": "com.anz.fastpayment.inward.avro",
  "version": "1.0.0",
  "fields": [
    // ... existing fields
    {
      "name": "newField",
      "type": ["null", "string"],
      "default": null,
      "doc": "New field added in v1.1.0"
    }
  ]
}
```

## Production Deployment Process

### Step 1: Schema Development
1. Create new schema version in `src/main/resources/avro/`
2. Ensure backward compatibility
3. Test locally with existing data

### Step 2: CI/CD Pipeline
```yaml
# .github/workflows/deploy.yml
- name: Deploy Application
  run: |
    # Application automatically registers new schemas
    kubectl apply -f k8s/
    # No manual schema registration needed
```

### Step 3: Rolling Deployment
1. **Deploy new application version**
2. **Application auto-registers new schemas**
3. **Gradual consumer updates**
4. **Old consumers continue working**

## Schema Registry Configuration

### Production Settings
```yaml
spring:
  kafka:
    producer:
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL}
        auto.register.schemas: true
        use.latest.version: true
    consumer:
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL}
        auto.register.schemas: true
        use.latest.version: true
```

### Schema Registry Policies
- **Compatibility**: BACKWARD (default)
- **Auto-registration**: ENABLED
- **Schema validation**: ENABLED

## Monitoring and Alerts

### Schema Registry Metrics
- Schema registration success/failure rates
- Schema evolution events
- Compatibility check results

### Application Metrics
- Message serialization/deserialization errors
- Schema version mismatches
- Consumer lag due to schema issues

## Best Practices

### 1. Schema Design
- Always add optional fields with defaults
- Use union types for nullable fields
- Document all schema changes
- Maintain backward compatibility

### 2. Deployment Strategy
- Deploy producers before consumers
- Use feature flags for gradual rollouts
- Monitor schema compatibility
- Have rollback plans ready

### 3. Testing Strategy
- Test schema evolution locally
- Validate with production data samples
- Use schema registry compatibility checks
- Test both old and new consumers

## Emergency Procedures

### Schema Registration Failure
1. Check Schema Registry connectivity
2. Verify schema compatibility
3. Review application logs
4. Rollback if necessary

### Breaking Schema Changes
1. Stop all producers
2. Update all consumers
3. Deploy new schema
4. Restart producers

## Tools and Commands

### Schema Validation
```bash
# Validate schema compatibility
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}' \
  http://schema-registry:8081/compatibility/subjects/InputMessage/versions/latest
```

### Schema Management
```bash
# List all schemas
curl http://schema-registry:8081/subjects

# Get schema details
curl http://schema-registry:8081/subjects/InputMessage/versions/latest
```

## Conclusion
- **Never manually register schemas in production**
- **Use automated schema registration**
- **Maintain backward compatibility**
- **Monitor schema evolution**
- **Have rollback procedures ready**
