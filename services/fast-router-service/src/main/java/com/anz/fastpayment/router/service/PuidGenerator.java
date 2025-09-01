package com.anz.fastpayment.router.service;

import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
public class PuidGenerator {

    private static final String CHANNEL_PREFIX = "G3I"; // first 3 characters
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyMMdd");
    private final SecureRandom random = new SecureRandom();

    public PuidGenerator() {}

    public String nextPuid() {
        String date = LocalDate.now().format(DATE_FMT); // yymmdd -> 6 digits
        int sevenDigits = random.nextInt(10_000_000); // 0..9_999_999
        String randDigits = String.format("%07d", sevenDigits);
        String thirteenDigits = date + randDigits; // 6 + 7 = 13
        return CHANNEL_PREFIX + thirteenDigits;
    }
}


