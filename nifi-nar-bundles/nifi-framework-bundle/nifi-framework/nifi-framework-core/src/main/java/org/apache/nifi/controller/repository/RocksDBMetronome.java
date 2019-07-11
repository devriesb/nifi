/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.repository;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.StringUtils;
import org.rocksdb.AccessHint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This RocksDB helper class, instead of forcing to disk every time it's given a record,
 * writes all waiting records on a regular interval (using a ScheduledExecutorService).
 * Like when a metronome ticks.
 */

public class RocksDBMetronome implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBMetronome.class);

    static final String CONFIGURATION_FAMILY = "configuration.column.family";
    static final String RECORDS_FAMILY = "records.column.family";

    private final AtomicLong lastSyncWarningNanos = new AtomicLong(0L);
    private final int parallelThreads;
    private final int maxWriteBufferNumber;
    private final int minWriteBufferNumberToMerge;
    private final long writeBufferSize;
    private final long maxTotalWalSize;
    private final long delayedWriteRate;
    private final int level0SlowdownWritesTrigger;
    private final int level0StopWritesTrigger;
    private final int maxBackgroundFlushes;
    private final int maxBackgroundCompactions;
    private final int statDumpSeconds;
    private final long syncMillis;
    private final long syncWarningNanos;
    private final Path storagePath;
    private final boolean adviseRandomOnOpen;
    private final boolean createIfMissing;
    private final boolean createMissingColumnFamilies;
    private final boolean useFsync;

    private final Set<byte[]> columnFamilyNames;
    private final Map<String, ColumnFamilyHandle> columnFamilyHandles;

    private final boolean automaticSyncEnabled;
    private final ScheduledExecutorService syncExecutor;
    private final ReentrantLock syncLock = new ReentrantLock();
    private final Condition syncCondition = syncLock.newCondition();

    private final AtomicInteger syncCounter = new AtomicInteger(0);
    private RocksDB db;
    private ColumnFamilyHandle configurationColumnFamilyHandle;
    private ColumnFamilyHandle recordsColumnFamilyHandle;
    private WriteOptions forceSyncWriteOptions;
    private WriteOptions noSyncWriteOptions;

    private RocksDBMetronome(Builder builder) {
        statDumpSeconds = builder.statDumpSeconds;
        parallelThreads = builder.parallelThreads;
        maxWriteBufferNumber = builder.maxWriteBufferNumber;
        minWriteBufferNumberToMerge = builder.minWriteBufferNumberToMerge;
        writeBufferSize = builder.writeBufferSize;
        maxTotalWalSize = builder.getMaxTotalWalSize();
        delayedWriteRate = builder.delayedWriteRate;
        level0SlowdownWritesTrigger = builder.level0SlowdownWritesTrigger;
        level0StopWritesTrigger = builder.level0StopWritesTrigger;
        maxBackgroundFlushes = builder.maxBackgroundFlushes;
        maxBackgroundCompactions = builder.maxBackgroundCompactions;
        syncMillis = builder.syncMillis;
        syncWarningNanos = builder.syncWarningNanos;
        storagePath = builder.storagePath;
        adviseRandomOnOpen = builder.adviseRandomOnOpen;
        createIfMissing = builder.createIfMissing;
        createMissingColumnFamilies = builder.createMissingColumnFamilies;
        useFsync = builder.useFsync;
        columnFamilyNames = builder.columnFamilyNames;
        columnFamilyHandles = new HashMap<>(columnFamilyNames.size());

        automaticSyncEnabled = builder.automaticSyncEnabled;
        syncExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Initialize the metronome
     *
     * @throws java.io.IOException if there is an issue with the underlying database
     */
    public void initialize() throws IOException {

        final String rocksSharedLibDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
        final String javaTmpDir = System.getProperty("java.io.tmpdir");
        final String libDir = !StringUtils.isBlank(rocksSharedLibDir) ? rocksSharedLibDir : javaTmpDir;
        try {
            Files.createDirectories(Paths.get(libDir));
        } catch (IOException e) {
            throw new IOException("Unable to load the RocksDB shared library into directory: " + libDir, e);
        }

        // delete any previous librocksdbjni.so files
        final File[] rocksSos = Paths.get(libDir).toFile().listFiles((dir, name) -> name.startsWith("librocksdbjni") && name.endsWith(".so"));
        if (rocksSos != null) {
            for (File rocksSo : rocksSos) {
                if (!rocksSo.delete()) {
                    logger.warn("Could not delete existing librocksdbjni*.so file {}", rocksSo);
                }
            }
        }

        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            if (System.getProperty("os.name").startsWith("Windows")) {
                logger.error("The RocksDBMetronome will only work on Windows if you have Visual C++ runtime libraries for Visual Studio 2015 installed.  " +
                        "If the DLLs required to support RocksDB cannot be found, then NiFi will not start!");
            }
            throw t;
        }

        Files.createDirectories(storagePath);

        forceSyncWriteOptions = new WriteOptions()
                .setDisableWAL(false)
                .setSync(true);

        noSyncWriteOptions = new WriteOptions()
                .setDisableWAL(false)
                .setSync(false);

        try (final DBOptions dbOptions = new DBOptions()
                .setAccessHintOnCompactionStart(AccessHint.SEQUENTIAL)
                .setAdviseRandomOnOpen(adviseRandomOnOpen)
                .setAllowMmapWrites(false) // required to be false for RocksDB.syncWal() to work
                .setCreateIfMissing(createIfMissing)
                .setCreateMissingColumnFamilies(createMissingColumnFamilies)
                .setDelayedWriteRate(delayedWriteRate)
                .setIncreaseParallelism(parallelThreads)
                .setLogger(getRocksLogger())
                .setMaxBackgroundCompactions(maxBackgroundCompactions)
                .setMaxBackgroundFlushes(maxBackgroundFlushes)
                .setMaxTotalWalSize(maxTotalWalSize)
                .setUseFsync(useFsync)
                .setStatsDumpPeriodSec(statDumpSeconds)
             ;

             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                     .setCompressionType(CompressionType.LZ4_COMPRESSION)
                     .setMaxWriteBufferNumber(maxWriteBufferNumber)
                     .setMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge)
                     .setLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger)
                     .setLevel0StopWritesTrigger(level0StopWritesTrigger)
                     .setWriteBufferSize(writeBufferSize)
        ) {

            // create family descriptors
            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>(columnFamilyNames.size());
            for (byte[] name : columnFamilyNames) {
                familyDescriptors.add(new ColumnFamilyDescriptor(name, cfOptions));
            }

            List<ColumnFamilyHandle> columnFamilyList = new ArrayList<>(columnFamilyNames.size());

            db = RocksDB.open(dbOptions, storagePath.toString(), familyDescriptors, columnFamilyList);

            // create the map of names to handles
            for (ColumnFamilyHandle cf : columnFamilyList) {
                columnFamilyHandles.put(new String(cf.getName(), StandardCharsets.UTF_8), cf);
            }

            // set specific special handles
            configurationColumnFamilyHandle = columnFamilyHandles.get(CONFIGURATION_FAMILY);
            recordsColumnFamilyHandle = columnFamilyHandles.get(RECORDS_FAMILY);

        } catch (RocksDBException e) {
            throw new IOException(e);
        }

        if (automaticSyncEnabled) {
            syncExecutor.scheduleWithFixedDelay(this::doSync, syncMillis, syncMillis, TimeUnit.MILLISECONDS);
        }

        logger.info("Initialized RocksDB Repository at {}", storagePath);
    }

    public ColumnFamilyHandle getColumnFamilyHandle(String familyName) {
        return columnFamilyHandles.get(familyName);
    }

    public byte[] getConfiguration(final byte[] key) throws RocksDBException {
        return db.get(configurationColumnFamilyHandle, key);
    }

    public void putConfiguration(final byte[] key, final byte[] value) throws RocksDBException {
        db.put(configurationColumnFamilyHandle, forceSyncWriteOptions, key, value);
    }

    public void put(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value) throws RocksDBException {
        put(columnFamilyHandle, key, value, false);
    }

    public void put(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final byte[] value, final boolean forceSync) throws RocksDBException {
        db.put(columnFamilyHandle, getWriteOptions(forceSync), key, value);
    }

    public void put(final byte[] key, final byte[] value, final boolean forceSync) throws RocksDBException {
        db.put(recordsColumnFamilyHandle, getWriteOptions(forceSync), key, value);
    }

    public byte[] get(final byte[] key) throws RocksDBException {
        return db.get(recordsColumnFamilyHandle, key);
    }

    public byte[] get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key) throws RocksDBException {
        return db.get(columnFamilyHandle, key);
    }

    public void put(final byte[] key, final byte[] value) throws RocksDBException {
        db.put(recordsColumnFamilyHandle, noSyncWriteOptions, key, value);
    }

    public void delete(byte[] key) throws RocksDBException {
        db.delete(recordsColumnFamilyHandle, key);
    }

    public void delete(final ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        db.delete(columnFamilyHandle, key);
    }

    public void delete(final ColumnFamilyHandle columnFamilyHandle, final byte[] key, final boolean forceSync) throws RocksDBException {
        db.delete(columnFamilyHandle, getWriteOptions(forceSync), key);
    }

    private WriteOptions getWriteOptions(boolean forceSync) {
        return forceSync ? forceSyncWriteOptions : noSyncWriteOptions;
    }

    public RocksIterator recordIterator() {
        return getIterator(recordsColumnFamilyHandle);
    }

    public RocksIterator getIterator(final ColumnFamilyHandle columnFamilyHandle) {
        return db.newIterator(columnFamilyHandle);
    }

    public void forceSync() throws RocksDBException {
        db.syncWal();
    }

    public int getSyncCounterValue() {
        return syncCounter.get();
    }

    /**
     * Close the Metronome and the RocksDB Objects
     *
     * @throws IOException if there are issues in closing any of the underlying RocksDB objects
     */
    @Override
    public void close() throws IOException {

        // syncing here so we don't close the db while doSync() might be using it
        syncLock.lock();
        try {
            if (syncExecutor != null) {
                syncExecutor.shutdownNow();
            }

            final AtomicReference<Exception> exceptionReference = new AtomicReference<>();

            safeClose(forceSyncWriteOptions, exceptionReference);
            safeClose(noSyncWriteOptions, exceptionReference);

            // close the column family handles first, then the db last
            for (ColumnFamilyHandle cfh : columnFamilyHandles.values()) {
                safeClose(cfh, exceptionReference);
            }
            safeClose(db, exceptionReference);

            if (exceptionReference.get() != null) {
                throw new IOException(exceptionReference.get());
            }
        } finally {
            syncLock.unlock();
        }
    }

    /**
     * @param autoCloseable      An {@link AutoCloseable} to be closed
     * @param exceptionReference A reference to contain any encountered {@link Exception}
     */
    private void safeClose(final AutoCloseable autoCloseable, final AtomicReference<Exception> exceptionReference) {
        if (autoCloseable != null) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                exceptionReference.set(e);
            }
        }
    }

    /**
     * @return The capacity of the store
     * @throws IOException if encountered
     */
    public long getStorageCapacity() throws IOException {
        return Files.getFileStore(storagePath).getTotalSpace();
    }

    /**
     * @return The usable space of the store
     * @throws IOException if encountered
     */
    public long getUsableStorageSpace() throws IOException {
        return Files.getFileStore(storagePath).getUsableSpace();
    }

    /**
     * This method is scheduled by the syncExecutor.  It runs at the specified interval, forcing the RocksDB WAL to disk.
     * <p>
     * If the sync is successful, it notifies threads that had been waiting for their records to be persisted using
     * syncCondition and the syncCounter, which is incremented to indicate success
     */
    void doSync() {
        syncLock.lock();
        try {
            // if we're interrupted, return
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            forceSync();
            syncCounter.incrementAndGet(); // its ok if it rolls over... we're just going to check that the value changed
            syncCondition.signalAll();
        } catch (final IllegalArgumentException e) {
            logger.error("Unable to sync, likely because the repository is out of space.", e);
        } catch (final Throwable t) {
            logger.error("Unable to sync", t);
        } finally {
            syncLock.unlock();
        }
    }

    /**
     * This method block until the next time the WAL is forced to disk, ensuring that all records written before this point have been persisted.
     */
    public void waitForSync() throws InterruptedException {
        final int counterValue = syncCounter.get();
        waitForSync(counterValue);
    }

    /**
     * This method blocks until the WAL has been forced to disk, ensuring that all records written before the point specified by the counterValue have been persisted.
     *
     * @param counterValue The value of the counter at the time of a write we must persist
     * @throws InterruptedException if the thread is interrupted
     */
    public void waitForSync(final int counterValue) throws InterruptedException {
        if (counterValue != syncCounter.get()) {
            return; // if the counter has already changed, we don't need to wait (or grab the lock) because the records we're concerned with have already been persisted
        }
        long waitTimeRemaining = syncWarningNanos;
        syncLock.lock();
        try {
            while (counterValue == syncCounter.get()) {  // wait until the counter changes (indicating sync occurred)
                waitTimeRemaining = syncCondition.awaitNanos(waitTimeRemaining);
                if (waitTimeRemaining <= 0L) { // this means the wait timed out

                    // only log a warning every syncWarningNanos... don't spam the logs
                    final long now = System.nanoTime();
                    final long lastWarning = lastSyncWarningNanos.get();
                    if (now - lastWarning > syncWarningNanos
                            && lastSyncWarningNanos.compareAndSet(lastWarning, now)) {
                        logger.warn("Failed to sync within {} seconds... system configuration may need to be adjusted", TimeUnit.NANOSECONDS.toSeconds(syncWarningNanos));
                    }

                    // reset waiting time
                    waitTimeRemaining = syncWarningNanos;
                }
            }
        } finally {
            syncLock.unlock();
        }
    }

    public static byte[] getBytes(long value) {
        byte[] bytes = new byte[8];
        writeLong(value, bytes);
        return bytes;
    }

    public static void writeLong(long l, byte[] bytes) {
        bytes[0] = (byte) (l >>> 56);
        bytes[1] = (byte) (l >>> 48);
        bytes[2] = (byte) (l >>> 40);
        bytes[3] = (byte) (l >>> 32);
        bytes[4] = (byte) (l >>> 24);
        bytes[5] = (byte) (l >>> 16);
        bytes[6] = (byte) (l >>> 8);
        bytes[7] = (byte) (l);
    }

    public static long readLong(final byte[] bytes) throws IOException {
        if (bytes.length != 8) {
            throw new IOException("wrong number of bytes to convert to long (must be 8)");
        }
        return (((long) (bytes[0]) << 56) +
                ((long) (bytes[1] & 255) << 48) +
                ((long) (bytes[2] & 255) << 40) +
                ((long) (bytes[3] & 255) << 32) +
                ((long) (bytes[4] & 255) << 24) +
                ((long) (bytes[5] & 255) << 16) +
                ((long) (bytes[6] & 255) << 8) +
                ((long) (bytes[7] & 255)));
    }

    /**
     * @return the storage path of rocks db
     */
    public Path getStoragePath() {
        return storagePath;
    }

    /**
     * @return A RocksDB logger capturing all logging output from RocksDB
     */
    private org.rocksdb.Logger getRocksLogger() {
        try (Options options = new Options()
                // make RocksDB give us everything, and we'll decide what we want to log in our wrapper
                .setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
            return new LogWrapper(options);
        }
    }

    /**
     * An Extension of org.rocksdb.Logger that wraps the slf4j Logger
     */
    private class LogWrapper extends org.rocksdb.Logger {

        LogWrapper(Options options) {
            super(options);
        }

        @Override
        protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
            switch (infoLogLevel) {
                case ERROR_LEVEL:
                case FATAL_LEVEL:
                    logger.error(logMsg);
                    break;
                case WARN_LEVEL:
                    logger.warn(logMsg);
                    break;
                case DEBUG_LEVEL:
                    logger.debug(logMsg);
                    break;
                case INFO_LEVEL:
                case HEADER_LEVEL:
                default:
                    logger.info(logMsg);
                    break;
            }
        }
    }

    public static class Builder {

        int parallelThreads = 8;
        int maxWriteBufferNumber = 4;
        int minWriteBufferNumberToMerge = 1;
        long writeBufferSize = (long) DataUnit.MB.toB(256);
        long delayedWriteRate = (long) DataUnit.MB.toB(16);
        int level0SlowdownWritesTrigger = 20;
        int level0StopWritesTrigger = 40;
        int maxBackgroundFlushes = 1;
        int maxBackgroundCompactions = 1;
        int statDumpSeconds = 600;
        long syncMillis = 10;
        long syncWarningNanos = TimeUnit.SECONDS.toNanos(30);
        Path storagePath;
        boolean adviseRandomOnOpen = false;
        boolean createIfMissing = true;
        boolean createMissingColumnFamilies = true;
        boolean useFsync = true;
        boolean automaticSyncEnabled = true;
        final Set<byte[]> columnFamilyNames = new HashSet<>();

        public RocksDBMetronome build() {
            if (storagePath == null) {
                throw new IllegalStateException("Cannot create RocksDBMetronome because storagePath is not set");
            }

            // add default column families
            columnFamilyNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
            columnFamilyNames.add(CONFIGURATION_FAMILY.getBytes(StandardCharsets.UTF_8));
            columnFamilyNames.add(RECORDS_FAMILY.getBytes(StandardCharsets.UTF_8));

            return new RocksDBMetronome(this);
        }

        public Builder addColumnFamily(String name) {
            this.columnFamilyNames.add(name.getBytes(StandardCharsets.UTF_8));
            return this;
        }

        public Builder setStoragePath(Path storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public Builder setParallelThreads(int parallelThreads) {
            this.parallelThreads = parallelThreads;
            return this;
        }

        public Builder setMaxWriteBufferNumber(int maxWriteBufferNumber) {
            this.maxWriteBufferNumber = maxWriteBufferNumber;
            return this;
        }

        public Builder setMinWriteBufferNumberToMerge(int minWriteBufferNumberToMerge) {
            this.minWriteBufferNumberToMerge = minWriteBufferNumberToMerge;
            return this;
        }

        public Builder setWriteBufferSize(long writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }

        public Builder setDelayedWriteRate(long delayedWriteRate) {
            this.delayedWriteRate = delayedWriteRate;
            return this;
        }

        public Builder setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger) {
            this.level0SlowdownWritesTrigger = level0SlowdownWritesTrigger;
            return this;
        }

        public Builder setLevel0StopWritesTrigger(int level0StopWritesTrigger) {
            this.level0StopWritesTrigger = level0StopWritesTrigger;
            return this;
        }

        public Builder setMaxBackgroundFlushes(int maxBackgroundFlushes) {
            this.maxBackgroundFlushes = maxBackgroundFlushes;
            return this;
        }

        public Builder setMaxBackgroundCompactions(int maxBackgroundCompactions) {
            this.maxBackgroundCompactions = maxBackgroundCompactions;
            return this;
        }

        public Builder setStatDumpSeconds(int statDumpSeconds) {
            this.statDumpSeconds = statDumpSeconds;
            return this;
        }

        public Builder setSyncMillis(long syncMillis) {
            this.syncMillis = syncMillis;
            return this;
        }

        public Builder setSyncWarningNanos(long syncWarningNanos) {
            this.syncWarningNanos = syncWarningNanos;
            return this;
        }


        public Builder setAdviseRandomOnOpen(boolean adviseRandomOnOpen) {
            this.adviseRandomOnOpen = adviseRandomOnOpen;
            return this;
        }

        public Builder setCreateMissingColumnFamilies(boolean createMissingColumnFamilies) {
            this.createMissingColumnFamilies = createMissingColumnFamilies;
            return this;
        }

        public Builder setCreateIfMissing(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }

        public Builder setUseFsync(boolean useFsync) {
            this.useFsync = useFsync;
            return this;
        }

        public Builder setAutomaticSyncEnabled(boolean automaticSyncEnabled) {
            this.automaticSyncEnabled = automaticSyncEnabled;
            return this;
        }

        long getMaxTotalWalSize() {
            return writeBufferSize * maxWriteBufferNumber;
        }
    }
}
