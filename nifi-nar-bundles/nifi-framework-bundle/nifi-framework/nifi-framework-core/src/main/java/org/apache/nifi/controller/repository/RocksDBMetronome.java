package org.apache.nifi.controller.repository;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.nifi.processor.DataUnit;
import org.rocksdb.*;


/**
 * This RocksDB helper class, instead of forcing to disk every time it's given a record,
 * writes all waiting records on a regular interval (using a ScheduledExecutorService).
 * Like when a metronome ticks.
 */

public class RocksDBMetronome implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBMetronome.class);

    private static Charset UTF8 = Charset.forName("UTF-8");
    private static final byte[] ZERO_BYTE = new byte[]{0};
    private static final byte[] CONFIGURATION_FAMILY = "configuration.column.family".getBytes(UTF8);
    private static final byte[] RECORDS_FAMILY = "records.column.family".getBytes(UTF8);

    private final AtomicLong lastSyncWarningNanos = new AtomicLong(0L);
    private final int parallelThreads;
    private final int maxWriteBufferNumber;
    private final int minWriteBufferNumberToMerge;
    private final long writeBufferSize;
    private final long delayedWriteRate;
    private final int level0SlowdownWritesTrigger;
    private final int level0StopWritesTrigger;
    private final int maxBackgroundFlushes;
    private final int maxBackgroundCompactions;
    private final int statDumpSeconds;
    private final long syncWarningNanos;
    private final boolean adviseRandomOnOpen;
    private final boolean createIfMissing;
    private final boolean createMissingColumnFamilies;
    private final boolean automaticSyncEnabled;
    private final long minSyncDelayMillis;
    private final Path storagePath;

    private final ScheduledExecutorService syncExecutor;

    private final AtomicInteger syncCounter = new AtomicInteger(0);
    private final ReentrantLock syncLock = new ReentrantLock();
    private final Condition syncCondition = syncLock.newCondition();

    private WriteOptions forceSyncWriteOptions;
    private WriteOptions noSyncWriteOptions;
    private RocksDB db;
    private ColumnFamilyHandle defaultColumnFamilyHandle;
    private ColumnFamilyHandle configurationColumnFamilyHandle;
    private ColumnFamilyHandle recordsColumnFamilyHandle;


    public RocksDBMetronome(Builder builder) {
        minWriteBufferNumberToMerge = builder.minWriteBufferNumberToMerge;
        delayedWriteRate = builder.delayedWriteRate;
        writeBufferSize = builder.writeBufferSize;
        level0SlowdownWritesTrigger = builder.level0SlowdownWritesTrigger;
        level0StopWritesTrigger = builder.level0StopWritesTrigger;
        maxBackgroundFlushes = builder.maxBackgroundFlushes;
        maxBackgroundCompactions = builder.maxBackgroundCompactions;
        statDumpSeconds = builder.statDumpSeconds;
        storagePath = builder.storagePath;
        parallelThreads = builder.parallelThreads;
        maxWriteBufferNumber = builder.maxWriteBufferNumber;
        syncWarningNanos = builder.syncWarningNanos;
        adviseRandomOnOpen = builder.adviseRandomOnOpen;
        createIfMissing = builder.createIfMissing;
        createMissingColumnFamilies = builder.createMissingColumnFamilies;
        automaticSyncEnabled = builder.automaticSyncEnabled;
        minSyncDelayMillis = builder.minSyncDelayMillis;

        syncExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Initialize the metronome
     *
     * @throws java.io.IOException - if there is an issue with the underlying database
     */
    public void initialize() throws IOException {

        final String rocksSharedLibDir = System.getenv("ROCKSDB_SHAREDLIB_DIR").trim();
        final String javaTmpDir = System.getProperty("java.io.tmpdir");
        final String libDir = (rocksSharedLibDir.length() > 0) ? rocksSharedLibDir : javaTmpDir;
        try {
            Files.createDirectories(Paths.get(libDir));
        } catch (IOException e) {
            throw new IOException("Unable to load the RocksDB shared library into directory: " + libDir, e);
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
                .setCreateIfMissing(createIfMissing)
                .setCreateMissingColumnFamilies(createMissingColumnFamilies)
                .setLogger(getRocksLogger())
                .setIncreaseParallelism(parallelThreads)
                .setMaxBackgroundFlushes(maxBackgroundFlushes)
                .setMaxBackgroundCompactions(maxBackgroundCompactions)
                .setAdviseRandomOnOpen(adviseRandomOnOpen)
                .setAccessHintOnCompactionStart(AccessHint.SEQUENTIAL)
                .setStatsDumpPeriodSec(statDumpSeconds)
                .setDelayedWriteRate(delayedWriteRate);

             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                     .setCompressionType(CompressionType.LZ4_COMPRESSION)
                     .setWriteBufferSize(writeBufferSize)
                     .setMaxWriteBufferNumber(maxWriteBufferNumber)
                     .setMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge)
                     .setLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger)
                     .setLevel0StopWritesTrigger(level0StopWritesTrigger)
        ) {
            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>(3);
            familyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            familyDescriptors.add(new ColumnFamilyDescriptor(CONFIGURATION_FAMILY, cfOptions));
            familyDescriptors.add(new ColumnFamilyDescriptor(RECORDS_FAMILY, cfOptions));

            final List<ColumnFamilyHandle> columnFamilyList = new ArrayList<>(familyDescriptors.size());
            db = RocksDB.open(dbOptions, storagePath.toString(), familyDescriptors, columnFamilyList);

            defaultColumnFamilyHandle = columnFamilyList.get(0);
            configurationColumnFamilyHandle = columnFamilyList.get(1);
            recordsColumnFamilyHandle = columnFamilyList.get(2);

        } catch (RocksDBException e) {
            throw new IOException(e);
        }

        if (automaticSyncEnabled) {
            syncExecutor.scheduleWithFixedDelay(this::doSync, minSyncDelayMillis, minSyncDelayMillis, TimeUnit.MILLISECONDS);
        }

        logger.info("Initialized at {}", storagePath);
    }


    public byte[] getConfiguration(final byte[] key) throws RocksDBException {
        return db.get(configurationColumnFamilyHandle, key);
    }

    public void putConfiguration(final byte[] key, final byte[] value) throws RocksDBException {
        db.put(configurationColumnFamilyHandle, forceSyncWriteOptions, key, value);
    }

    public byte[] get(final byte[] key) throws RocksDBException {
        return db.get(recordsColumnFamilyHandle, key);
    }

    public void put(final byte[] key, final byte[] value) throws RocksDBException {
        db.put(recordsColumnFamilyHandle, noSyncWriteOptions, key, value);
    }

    public void put(final byte[] key, final byte[] value, final boolean forceSync) throws RocksDBException {
        if (forceSync) {
            db.put(recordsColumnFamilyHandle, forceSyncWriteOptions, key, value);
        } else {
            db.put(recordsColumnFamilyHandle, noSyncWriteOptions, key, value);
        }
    }

    public RocksIterator recordIterator() {
        return db.newIterator(recordsColumnFamilyHandle);
    }

    public void forceSync() throws RocksDBException {
        db.put(recordsColumnFamilyHandle, forceSyncWriteOptions, ZERO_BYTE, ZERO_BYTE); // this forces the WAL to disk
    }


    public int getSyncCounterValue() {
        return syncCounter.get();
    }

    /**
     * Close the Metronome and the RocksDB Objects
     *
     * @throws IOException - if there are issues in closing any of the underlyingRocksDB objects
     */
    @Override
    public void close() throws IOException {

        //syncing here so we don't close the db while doSync() might be using it
        syncLock.lock();
        try {
            if (syncExecutor != null) {
                syncExecutor.shutdownNow();
            }

            final AtomicReference<Exception> exceptionReference = new AtomicReference<>();

            safeClose(forceSyncWriteOptions, exceptionReference);
            safeClose(noSyncWriteOptions, exceptionReference);

            // close the column family handles first, then the db last
            safeClose(defaultColumnFamilyHandle, exceptionReference);
            safeClose(configurationColumnFamilyHandle, exceptionReference);
            safeClose(recordsColumnFamilyHandle, exceptionReference);
            safeClose(db, exceptionReference);

            if (exceptionReference.get() != null) {
                throw new IOException(exceptionReference.get());
            }
        } finally {
            syncLock.unlock();
        }
    }

    /**
     * @param autoCloseable      - An {@link AutoCloseable} to be closed
     * @param exceptionReference - A reference to contain any encountered {@link Exception}
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

    public long getStorageCapacity() throws IOException {
        return Files.getFileStore(storagePath).getTotalSpace();
    }

    public long getUsableStorageSpace() throws IOException {
        return Files.getFileStore(storagePath).getUsableSpace();
    }

    /**
     * This method is scheduled by the syncExecutor.  It runs at the specified interval, forcing the RocksDB WAL to disk.
     * <p>
     * If the sync is successful, it notifies threads that had been waiting for their records to be persisted using
     * syncCondition and the syncCounter, which is incremented to indicate success
     */
    private void doSync() {
        syncLock.lock();
        try {
            forceSync();
            syncCounter.incrementAndGet(); // its ok if it rolls over... we're just going to check that the value changed
            syncCondition.signalAll();
        } catch (final Throwable t) { // normally shouldn't catch Throwable, but we don't want to suppress future executions due to a potentially transient issue
            logger.error("Unable to sync due to " + t.toString(), t);
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
     * @param counterValue - The value of the counter at the time of a write we must persist
     */
    private void waitForSync(final int counterValue) throws InterruptedException {
        if (counterValue != syncCounter.get()) {
            return; // the counter has already changed, meaning the records we're concerned with have already been persisted
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

    private Path getStoragePath() {
        return storagePath;
    }

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
                case DEBUG_LEVEL:
                    logger.debug(logMsg);
                    break;
                case INFO_LEVEL:
                case HEADER_LEVEL:
                    logger.info(logMsg);
                    break;
                case WARN_LEVEL:
                    logger.warn(logMsg);
                    break;
                case ERROR_LEVEL:
                case FATAL_LEVEL:
                    logger.error(logMsg);
                    break;
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
        long minSyncDelayMillis;
        long syncWarningNanos = TimeUnit.SECONDS.toNanos(30);
        Path storagePath;
        boolean adviseRandomOnOpen = false;
        boolean createIfMissing = true;
        boolean createMissingColumnFamilies = true;
        boolean automaticSyncEnabled = true;

        public RocksDBMetronome build() {
            if (storagePath == null) {
                throw new IllegalStateException("Cannot create RocksDBMetronome because storagePath is not set");
            }
            return new RocksDBMetronome(this);
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

        public Builder setMinSyncDelayMillis(long minSyncDelayMillis) {
            this.minSyncDelayMillis = minSyncDelayMillis;
            return this;
        }

        public Builder setSyncWarningNanos(long syncWarningNanos) {
            this.syncWarningNanos = syncWarningNanos;
            return this;
        }

        public Builder setStoragePath(Path storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public Builder setAdviseRandomOnOpen(boolean adviseRandomOnOpen) {
            this.adviseRandomOnOpen = adviseRandomOnOpen;
            return this;
        }

        public Builder setCreateIfMissing(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }

        public Builder setCreateMissingColumnFamilies(boolean createMissingColumnFamilies) {
            this.createMissingColumnFamilies = createMissingColumnFamilies;
            return this;
        }

        public Builder setAutomaticSyncEnabled(boolean automaticSyncEnabled) {
            this.automaticSyncEnabled = automaticSyncEnabled;
            return this;
        }
    }
}
