package org.apache.nifi.controller.repository;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestRocksDBMetronome {

    private Random random = new Random();

    @Test
    public void testReadWriteLong() throws IOException {
        byte[] key = new byte[8];
        for (long i = 0; i < 10; i++) {
            {
                RocksDBMetronome.writeLong(i, key);
                assertEquals(i, RocksDBMetronome.readLong(key));
            }
            {
                long testValue = Long.MIN_VALUE + i;
                RocksDBMetronome.writeLong(testValue, key);
                assertEquals(testValue, RocksDBMetronome.readLong(key));
            }
            {
                long testValue = Long.MAX_VALUE - i;
                RocksDBMetronome.writeLong(testValue, key);
                assertEquals(testValue, RocksDBMetronome.readLong(key));
            }
            {
                long testValue = random.nextLong();
                RocksDBMetronome.writeLong(testValue, key);
                assertEquals(testValue, RocksDBMetronome.readLong(key));
            }
        }
    }
}