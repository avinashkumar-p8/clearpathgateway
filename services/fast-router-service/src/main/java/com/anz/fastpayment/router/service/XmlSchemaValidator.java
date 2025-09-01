package com.anz.fastpayment.router.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

@Component
public class XmlSchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(XmlSchemaValidator.class);

    @Value("${app.validation.xsd.enabled:true}")
    private boolean xsdValidationEnabled;

    @Value("${app.validation.max-bytes:1048576}")
    private int maxXmlBytes;

    public void validate(String xmlContent, String messageType) {
        if (!xsdValidationEnabled) {
            log.info("[XSD] Validation disabled by configuration; skipping XSD check for type={}", messageType);
            return;
        }
        if (messageType == null || messageType.trim().isEmpty()) {
            throw new IllegalArgumentException("Unsupported or empty message type for XSD validation: " + messageType);
        }
        if (xmlContent == null || xmlContent.trim().isEmpty()) {
            throw new IllegalArgumentException("xmlContent must not be null or blank for XSD validation");
        }
        if (xmlContent.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > maxXmlBytes) {
            throw new IllegalArgumentException("XML too large for validation");
        }
        String schemaPath = resolveSchemaPath(messageType);
        if (schemaPath == null) {
            throw new IllegalArgumentException("Unsupported message type for XSD validation: " + messageType);
        }
        try {
            log.debug("[XSD] Loading schema from classpath:{}", schemaPath);
            ClassPathResource schemaResource = new ClassPathResource(schemaPath);
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            try {
                factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
                factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            } catch (Exception secureEx) {
                log.debug("[XSD] Could not set secure properties on SchemaFactory: {}", secureEx.getMessage());
            }
            // Use URL-based source so XSD includes/imports resolve correctly
            Schema schema = factory.newSchema(schemaResource.getURL());
            Validator validator = schema.newValidator();
            log.debug("[XSD] Running validator for messageType={}", messageType);
            validator.validate(new StreamSource(new ByteArrayInputStream(
                    xmlContent.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        } catch (Exception ex) {
            log.warn("XSD validation failed for type {}: {}", messageType, ex.getMessage());
            throw new IllegalArgumentException("XML does not conform to XSD for type " + messageType, ex);
        }
    }

    private String resolveSchemaPath(String messageType) {
        return switch (messageType) {
            case "pacs.008.001.13" -> "schema/pacs.008.001.13.xsd";
            case "pacs.003.001.11" -> "schema/pacs.003.001.11.xsd";
            case "pacs.007.001.13" -> "schema/pacs.007.001.13.xsd";
            case "camt.056.001.11" -> "schema/camt.056.001.11.xsd";
            default -> null;
        };
    }
}


