package com.anz.fastpayment.router.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

@Component
public class PuidGenerator {

    @Value("${app.id.remote.enabled:false}")
    private boolean remoteEnabled;

    @Value("${app.id.remote.base-url:http://localhost:8091}")
    private String idServiceBaseUrl;

    @Value("${app.id.remote.channel:G3I}")
    private String channel;

    private final RestTemplate restTemplate = new RestTemplate();
    private final Deque<String> cache = new ArrayDeque<>();

    public String nextPuid() {
        if (!remoteEnabled) {
            // fallback to local shape: 3 + 13 digits
            long now = System.currentTimeMillis() / 1000L;
            String epoch = String.format("%013d", now % 1_000_000_000_000L);
            return channel.substring(0, Math.min(3, channel.length())) + epoch.substring(0, 13);
        }
        if (cache.isEmpty()) refill();
        String puid = cache.pollFirst();
        if (puid == null) {
            refill();
            puid = cache.pollFirst();
        }
        return puid;
    }

    private void refill() {
        try {
            String url = idServiceBaseUrl + "/api/ids/puid-block?channel=" + channel + "&size=100";
            ResponseEntity<Map> resp = restTemplate.getForEntity(url, Map.class);
            if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                Object list = resp.getBody().get("puids");
                if (list instanceof java.util.List<?> l) {
                    for (Object o : l) {
                        if (o != null) cache.addLast(String.valueOf(o));
                    }
                }
            }
        } catch (Exception ignore) { }
    }
}


