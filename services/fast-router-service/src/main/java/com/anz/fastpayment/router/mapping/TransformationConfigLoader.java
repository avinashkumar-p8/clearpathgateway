package com.anz.fastpayment.router.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

@Component
public class TransformationConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(TransformationConfigLoader.class);

    private final ObjectMapper objectMapper;
    private JsonNode root;

    public TransformationConfigLoader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        load();
    }

    public JsonNode getRoot() {
        return root;
    }

    private void load() {
        try {
            ClassPathResource resource = new ClassPathResource("mappings/transformation-config.json");
            try (InputStream is = resource.getInputStream()) {
                this.root = objectMapper.readTree(is);
                log.info("Loaded transformation config with version {}",
                        root.path("version").asText("unknown"));
            }
        } catch (IOException e) {
            log.error("Failed to load transformation config", e);
            this.root = objectMapper.createObjectNode();
        }
    }
}


