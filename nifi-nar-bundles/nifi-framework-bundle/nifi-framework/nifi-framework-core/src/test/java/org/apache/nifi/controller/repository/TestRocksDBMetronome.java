package org.apache.nifi.controller.repository;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRocksDBMetronome {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Random random = new Random();

    private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_2 = "key 2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_2 = "value 2".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testReadWriteLong() throws Exception {
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

    @Test
    public void testPutGet() throws Exception {

        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .build()) {
            db.initialize();

            assertNull(db.get(KEY));
            db.put(KEY, VALUE);
            assertArrayEquals(VALUE, db.get(KEY));
        }
    }

    @Test
    public void testAddColumnFamily() throws Exception {

        String secondFamilyName = "second family";
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .addColumnFamily(secondFamilyName)
                .build()) {
            db.initialize();
            ColumnFamilyHandle secondFamily = db.getColumnFamilyHandle(secondFamilyName);

            assertNull(db.get(KEY));
            assertNull(db.get(KEY_2));

            assertNull(db.get(secondFamily, KEY));
            assertNull(db.get(secondFamily, KEY_2));

            db.put(KEY, VALUE);
            db.put(secondFamily, KEY_2, VALUE_2);

            assertArrayEquals(VALUE, db.get(KEY));
            assertNull(db.get(KEY_2));

            assertArrayEquals(VALUE_2, db.get(secondFamily, KEY_2));
            assertNull(db.get(secondFamily, KEY));
        }
    }

    @Test
    public void testIterator() throws Exception {

        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .build()) {
            db.initialize();

            db.put(KEY, VALUE);
            db.put(KEY_2, VALUE_2);

            RocksIterator iterator = db.recordIterator();
            iterator.seekToFirst();

            Map<String,byte[]> recovered = new HashMap<>();

            while(iterator.isValid()){
                recovered.put(new String(iterator.key(), StandardCharsets.UTF_8),iterator.value());
                iterator.next();
            }

            assertEquals(2,recovered.size());
            assertArrayEquals(VALUE,recovered.get(new String(KEY, StandardCharsets.UTF_8)));
            assertArrayEquals(VALUE_2,recovered.get(new String(KEY_2, StandardCharsets.UTF_8)));
        }
    }


}