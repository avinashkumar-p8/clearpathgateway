package com.anz.fastpayment.id.controller;

import com.anz.fastpayment.id.service.IdGeneratorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

@RestController
@RequestMapping("/api/ids")
public class IdController {

    private final IdGeneratorService service;

    public IdController(IdGeneratorService service) {
        this.service = service;
    }

    @GetMapping("/puid")
    public ResponseEntity<Map<String, String>> puid(@RequestParam(name = "channel", defaultValue = "G31") String channel) {
        String puid = service.nextPuid(channel);
        Map<String, String> body = new HashMap<>();
        body.put("puid", puid);
        return ResponseEntity.ok(body);
    }

    @GetMapping("/muid")
    public ResponseEntity<Map<String, String>> muid() {
        String muid = service.nextMuid();
        Map<String, String> body = new HashMap<>();
        body.put("muid", muid);
        return ResponseEntity.ok(body);
    }

    @GetMapping("/puid-block")
    public ResponseEntity<Map<String, Object>> puidBlock(@RequestParam(name = "channel", defaultValue = "G31") String channel,
                                                         @RequestParam(name = "size", defaultValue = "50") int size) {
        if (size < 1) size = 1;
        if (size > 1000) size = 1000;
        List<String> puids = service.nextPuidBlock(channel, size);
        Map<String, Object> body = new HashMap<>();
        body.put("channel", channel);
        body.put("count", puids.size());
        body.put("puids", puids);
        return ResponseEntity.ok(body);
    }
}


