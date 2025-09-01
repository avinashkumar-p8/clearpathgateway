package com.anz.fastpayment.router.service;

import org.springframework.stereotype.Component;

@Component
public class Iso20022MessageTypeDetector {

    public String detectType(String xml) {
        if (xml == null) {
            return "unknown";
        }
        String lower = xml.toLowerCase();
        if (lower.contains("pacs.008.001.13")) return "pacs.008.001.13";
        if (lower.contains("pacs.003.001.11")) return "pacs.003.001.11";
        if (lower.contains("pacs.007.001.13")) return "pacs.007.001.13";
        if (lower.contains("camt.056.001.11")) return "camt.056.001.11";
        org.slf4j.LoggerFactory.getLogger(Iso20022MessageTypeDetector.class)
                .warn("[DETECT] Unknown message type; defaulting to 'unknown'");
        return "unknown";
    }
}


