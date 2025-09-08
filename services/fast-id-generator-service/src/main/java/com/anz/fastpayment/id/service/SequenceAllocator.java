package com.anz.fastpayment.id.service;

public interface SequenceAllocator {
    long allocateBlock(int blockSize);
}


