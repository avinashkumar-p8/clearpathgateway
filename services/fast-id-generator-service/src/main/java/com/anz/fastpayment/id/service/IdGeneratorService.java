package com.anz.fastpayment.id.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class IdGeneratorService {

	@Value("${id.shard:0}")
	private String shardConfig; // last digit used

	private volatile long lastSecond = -1L;
	private final AtomicInteger sequence = new AtomicInteger(0);
	private final java.util.Random random = new java.util.Random();

	public synchronized String nextPuid(String channel) {
		if (channel == null || channel.isBlank()) channel = "G3I";
		channel = channel.length() >= 3 ? channel.substring(0, 3) : String.format("%-3s", channel).replace(' ', 'X');
		long nowSec = Instant.now().getEpochSecond();
		if (nowSec != lastSecond) {
			lastSecond = nowSec;
			sequence.set(0);
		}
		int seq = sequence.getAndIncrement();
		if (seq >= 1000) {
			// wait for next second to keep uniqueness
			do { nowSec = Instant.now().getEpochSecond(); } while (nowSec == lastSecond);
			lastSecond = nowSec;
			sequence.set(1);
			seq = 0;
		}
		String epochSec = String.format("%09d", (int)(nowSec % 1_000_000_000L));
		char shard = (shardConfig == null || shardConfig.isBlank()) ? '0' : shardConfig.charAt(shardConfig.length() - 1);
		String seq3 = String.format("%03d", seq);
		return channel + epochSec + shard + seq3; // 3 + 9 + 1 + 3 = 16
	}

	@Cacheable(cacheNames = "muidByPuid", key = "#root.args[0]")
	public String nextMuid(String puid) {
		long nowPart = System.nanoTime() & 0xFFFFFL; // lower 20 bits
		int rndPart = random.nextInt(1 << 12); // 12 bits
		int mix = (int)((nowPart << 12) | rndPart) & 0xFFFFFF; // 24 bits total
		String suffix = Integer.toString(mix, 36);
		while (suffix.length() < 5) suffix = "0" + suffix; // pad to at least 5
		return "%s-%s".formatted(puid, suffix);
	}

    public synchronized List<String> nextPuidBlock(String channel, int size) {
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(nextPuid(channel));
        }
        return list;
    }
}


