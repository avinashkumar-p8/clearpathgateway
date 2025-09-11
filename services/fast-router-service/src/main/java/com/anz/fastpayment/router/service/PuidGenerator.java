package com.anz.fastpayment.router.service;

import org.springframework.stereotype.Component;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@Component
public class PuidGenerator {

    private final RestTemplate restTemplate;
    private final String baseUrl;
    private final String channel;

    public PuidGenerator() {
        this.restTemplate = new RestTemplate();
        this.baseUrl = System.getenv().getOrDefault("ID_BASE_URL", "http://localhost:8091");
        this.channel = System.getenv().getOrDefault("ID_CHANNEL", "G3I");
    }

    // Test-friendly constructor
    public PuidGenerator(RestTemplate restTemplate, String baseUrl, String channel) {
        this.restTemplate = restTemplate == null ? new RestTemplate() : restTemplate;
        this.baseUrl = baseUrl == null || baseUrl.isBlank() ? "http://localhost:8091" : baseUrl;
        this.channel = channel == null || channel.isBlank() ? "G3I" : channel;
    }

    public String nextPuid() {
        try {
            String url = baseUrl + "/api/ids/next-puid?channel=" + channel;
            ResponseEntity<String> resp = restTemplate.getForEntity(url, String.class);
            if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null && !resp.getBody().isBlank()) {
                return resp.getBody().trim();
            }
        } catch (Exception ignore) { }
        throw new IllegalStateException("Unable to retrieve PUID from id-generator-service");
    }
}


