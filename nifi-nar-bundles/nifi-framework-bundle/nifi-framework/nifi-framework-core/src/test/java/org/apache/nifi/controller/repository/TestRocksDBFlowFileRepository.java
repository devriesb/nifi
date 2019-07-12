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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.NopConnectionEventListener;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.StandardFlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.swap.StandardSwapContents;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.rocksdb.RocksDBMetronome;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestRocksDBFlowFileRepository {


    private final Map<String, String> additionalProperties = new HashMap<>();
    private String nifiPropertiesPath;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void createRepo() throws IOException {
        File testRepo = temporaryFolder.newFolder();
        additionalProperties.put(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, testRepo.getAbsolutePath());

        File properties = temporaryFolder.newFile();
        Files.copy(Paths.get("src/test/resources/conf/nifi.properties"), properties.toPath(), StandardCopyOption.REPLACE_EXISTING);
        nifiPropertiesPath = properties.getAbsolutePath();
    }

    @Test
    public void testNormalizeSwapLocation() {
        assertEquals("/", RocksDBFlowFileRepository.normalizeSwapLocation("/"));
        assertEquals("", RocksDBFlowFileRepository.normalizeSwapLocation(""));
        assertEquals(null, RocksDBFlowFileRepository.normalizeSwapLocation(null));
        assertEquals("test", RocksDBFlowFileRepository.normalizeSwapLocation("test.txt"));
        assertEquals("test", RocksDBFlowFileRepository.normalizeSwapLocation("/test.txt"));
        assertEquals("test", RocksDBFlowFileRepository.normalizeSwapLocation("/tmp/test.txt"));
        assertEquals("test", RocksDBFlowFileRepository.normalizeSwapLocation("//test.txt"));
        assertEquals("test", RocksDBFlowFileRepository.normalizeSwapLocation("/path/to/other/file/repository/test.txt"));
        assertEquals("test", RocksDBFlowFileRepository.normalizeSwapLocation("test.txt/"));
        assertEquals("test", RocksDBFlowFileRepository.normalizeSwapLocation("/path/to/test.txt/"));
        assertEquals("test", WriteAheadFlowFileRepository.normalizeSwapLocation(WriteAheadFlowFileRepository.normalizeSwapLocation("/path/to/test.txt/")));
    }

    @Test
    public void testSwapLocationsRestored() throws IOException {

        final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties));
        repo.initialize(new StandardResourceClaimManager());

        final TestRocksDBFlowFileRepository.TestQueueProvider queueProvider = new TestRocksDBFlowFileRepository.TestQueueProvider();
        repo.loadFlowFiles(queueProvider);

        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);

        StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        ffBuilder.id(1L);
        ffBuilder.size(0L);
        final FlowFileRecord flowFileRecord = ffBuilder.build();

        final List<RepositoryRecord> records = new ArrayList<>();
        final StandardRepositoryRecord record = new StandardRepositoryRecord(queue, flowFileRecord, "swap123");
        record.setDestination(queue);
        records.add(record);

        repo.updateRepository(records);
        repo.close();

        // restore
        final RocksDBFlowFileRepository repo2 = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties));
        repo2.initialize(new StandardResourceClaimManager());
        repo2.loadFlowFiles(queueProvider);
        assertTrue(repo2.isValidSwapLocationSuffix("swap123"));
        assertFalse(repo2.isValidSwapLocationSuffix("other"));
        repo2.close();
    }

    @Test
    public void testSwapLocationsUpdatedOnRepoUpdate() throws IOException {
        final Path path = Paths.get("target/test-swap-repo");
        if (Files.exists(path)) {
            FileUtils.deleteFile(path.toFile(), true);
        }

        final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties));
        repo.initialize(new StandardResourceClaimManager());

        final TestRocksDBFlowFileRepository.TestQueueProvider queueProvider = new TestRocksDBFlowFileRepository.TestQueueProvider();
        repo.loadFlowFiles(queueProvider);

        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);

        StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        ffBuilder.id(1L);
        ffBuilder.size(0L);
        final FlowFileRecord flowFileRecord = ffBuilder.build();

        final List<RepositoryRecord> records = new ArrayList<>();
        final StandardRepositoryRecord record = new StandardRepositoryRecord(queue, flowFileRecord, "/tmp/swap123");
        record.setDestination(queue);
        records.add(record);

        assertFalse(repo.isValidSwapLocationSuffix("swap123"));
        repo.updateRepository(records);
        assertTrue(repo.isValidSwapLocationSuffix("swap123"));
        repo.close();
    }

    @Test
    public void testResourceClaimsIncremented() throws IOException {
        final ResourceClaimManager claimManager = new StandardResourceClaimManager();

        final TestRocksDBFlowFileRepository.TestQueueProvider queueProvider = new TestRocksDBFlowFileRepository.TestQueueProvider();
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");
        when(connection.getDestination()).thenReturn(Mockito.mock(Connectable.class));

        final FlowFileSwapManager swapMgr = new TestRocksDBFlowFileRepository.MockFlowFileSwapManager();
        final FlowFileQueue queue = new StandardFlowFileQueue("1234", new NopConnectionEventListener(), null, null, claimManager, null, swapMgr, null, 10000, 0L, "0 B");

        when(connection.getFlowFileQueue()).thenReturn(queue);
        queueProvider.addConnection(connection);

        final ResourceClaim resourceClaim1 = claimManager.newResourceClaim("container", "section", "1", false, false);
        final ContentClaim claim1 = new StandardContentClaim(resourceClaim1, 0L);

        final ResourceClaim resourceClaim2 = claimManager.newResourceClaim("container", "section", "2", false, false);
        final ContentClaim claim2 = new StandardContentClaim(resourceClaim2, 0L);

        // Create a flowfile repo, update it once with a FlowFile that points to one resource claim. Then,
        // indicate that a FlowFile was swapped out. We should then be able to recover these FlowFiles and the
        // resource claims' counts should be updated for both the swapped out FlowFile and the non-swapped out FlowFile
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(claimManager);
            repo.loadFlowFiles(queueProvider);

            // Create a Repository Record that indicates that a FlowFile was created
            final FlowFileRecord flowFile1 = new StandardFlowFileRecord.Builder()
                    .id(1L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-111111111111")
                    .contentClaim(claim1)
                    .build();
            final StandardRepositoryRecord rec1 = new StandardRepositoryRecord(queue);
            rec1.setWorking(flowFile1);
            rec1.setDestination(queue);

            // Create a Record that we can swap out
            final FlowFileRecord flowFile2 = new StandardFlowFileRecord.Builder()
                    .id(2L)
                    .addAttribute("uuid", "11111111-1111-1111-1111-111111111112")
                    .contentClaim(claim2)
                    .build();

            final StandardRepositoryRecord rec2 = new StandardRepositoryRecord(queue);
            rec2.setWorking(flowFile2);
            rec2.setDestination(queue);

            final List<RepositoryRecord> records = new ArrayList<>();
            records.add(rec1);
            records.add(rec2);
            repo.updateRepository(records);

            final String swapLocation = swapMgr.swapOut(Collections.singletonList(flowFile2), queue, null);
            repo.swapFlowFilesOut(Collections.singletonList(flowFile2), queue, swapLocation);
        }

        final ResourceClaimManager recoveryClaimManager = new StandardResourceClaimManager();
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(recoveryClaimManager);
            final long largestId = repo.loadFlowFiles(queueProvider);

            // largest ID known is 1 because this doesn't take into account the FlowFiles that have been swapped out
            assertEquals(1, largestId);
        }

        // resource claim 1 will have a single claimant count while resource claim 2 will have no claimant counts
        // because resource claim 2 is referenced only by flowfiles that are swapped out.
        assertEquals(1, recoveryClaimManager.getClaimantCount(resourceClaim1));
        assertEquals(0, recoveryClaimManager.getClaimantCount(resourceClaim2));

        final SwapSummary summary = queue.recoverSwappedFlowFiles();
        assertNotNull(summary);
        assertEquals(2, summary.getMaxFlowFileId().intValue());
        assertEquals(new QueueSize(1, 0L), summary.getQueueSize());

        final List<ResourceClaim> swappedOutClaims = summary.getResourceClaims();
        assertNotNull(swappedOutClaims);
        assertEquals(1, swappedOutClaims.size());
        assertEquals(claim2.getResourceClaim(), swappedOutClaims.get(0));
    }

    @Test
    public void testRestartWithOneRecord() throws IOException {

        final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties));
        repo.initialize(new StandardResourceClaimManager());

        final TestRocksDBFlowFileRepository.TestQueueProvider queueProvider = new TestRocksDBFlowFileRepository.TestQueueProvider();
        repo.loadFlowFiles(queueProvider);

        final List<FlowFileRecord> flowFileCollection = new ArrayList<>();

        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        doAnswer((Answer<Object>) invocation -> {
            flowFileCollection.add((FlowFileRecord) invocation.getArguments()[0]);
            return null;
        }).when(queue).put(any(FlowFileRecord.class));

        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);

        StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
        ffBuilder.id(1L);
        ffBuilder.addAttribute("abc", "xyz");
        ffBuilder.size(0L);
        final FlowFileRecord flowFileRecord = ffBuilder.build();

        final List<RepositoryRecord> records = new ArrayList<>();
        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
        record.setWorking(flowFileRecord);
        record.setDestination(connection.getFlowFileQueue());
        records.add(record);

        repo.updateRepository(records);

        // update to add new attribute
        ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(flowFileRecord).addAttribute("hello", "world");
        final FlowFileRecord flowFileRecord2 = ffBuilder.build();
        record.setWorking(flowFileRecord2);
        repo.updateRepository(records);

        // update size but no attribute
        ffBuilder = new StandardFlowFileRecord.Builder().fromFlowFile(flowFileRecord2).size(40L);
        final FlowFileRecord flowFileRecord3 = ffBuilder.build();
        record.setWorking(flowFileRecord3);
        repo.updateRepository(records);

        repo.close();

        // restore
        final RocksDBFlowFileRepository repo2 = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties));
        repo2.initialize(new StandardResourceClaimManager());
        repo2.loadFlowFiles(queueProvider);

        assertEquals(1, flowFileCollection.size());
        final FlowFileRecord flowFile = flowFileCollection.get(0);
        assertEquals(1L, flowFile.getId());
        assertEquals("xyz", flowFile.getAttribute("abc"));
        assertEquals(40L, flowFile.getSize());
        assertEquals("world", flowFile.getAttribute("hello"));

        repo2.close();
    }


    @Test
    public void testDoNotRemoveOrphans() throws Exception {

        final List<FlowFileRecord> flowFileCollection = new ArrayList<>();
        final TestQueueProvider queueProvider = new TestQueueProvider();

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {

            final Connection connection = addConnectionToProvider(queueProvider, flowFileCollection);

            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);

            StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            ffBuilder.id(1L);
            ffBuilder.addAttribute("abc", "xyz");
            ffBuilder.size(0L);
            final FlowFileRecord flowFileRecord = ffBuilder.build();

            final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
            record.setWorking(flowFileRecord);
            record.setDestination(connection.getFlowFileQueue());
            repo.updateRepository(Collections.singletonList(record));
        }

        // restore (& confirm present)
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);
            assertEquals(1, repo.getInMemoryFlowFiles());
        }
        // restore with empty queue provider (should throw exception)
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(new TestQueueProvider());
            fail();
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("Found FlowFile in repository without a corresponding queue"));
        }
    }

    @Test
    public void testRemoveOrphans() throws Exception {

        final List<FlowFileRecord> flowFileCollection = new ArrayList<>();
        final TestQueueProvider queueProvider = new TestQueueProvider();

        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.REMOVE_ORPHANED_FLOWFILES.propertyName, "true");

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {

            final Connection connection = addConnectionToProvider(queueProvider, flowFileCollection);

            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);

            StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            ffBuilder.id(1L);
            ffBuilder.addAttribute("abc", "xyz");
            ffBuilder.size(0L);
            final FlowFileRecord flowFileRecord = ffBuilder.build();

            final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
            record.setWorking(flowFileRecord);
            record.setDestination(connection.getFlowFileQueue());
            repo.updateRepository(Collections.singletonList(record));
        }

        // restore (& confirm present)
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);
            assertEquals(1, repo.getInMemoryFlowFiles());
        }
        // restore with empty queue provider (should throw exception)
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(new TestQueueProvider());
            assertEquals(0, repo.getInMemoryFlowFiles());
        }
    }

    @Test
    public void testKnownVersion() throws Exception {
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties);

        // create db with known version
        try (RocksDBMetronome db = new RocksDBMetronome.Builder().setStoragePath(RocksDBFlowFileRepository.getFlowFileRepoPath(niFiProperties)).build()) {
            db.initialize();
            db.putConfiguration(RocksDBFlowFileRepository.REPOSITORY_VERSION_KEY, RocksDBFlowFileRepository.VERSION_ONE_BYTES);
        }
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUnknownVersion() throws Exception {
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties);

        // create db with known version
        try (RocksDBMetronome db = new RocksDBMetronome.Builder().setStoragePath(RocksDBFlowFileRepository.getFlowFileRepoPath(niFiProperties)).build()) {
            db.initialize();
            db.putConfiguration(RocksDBFlowFileRepository.REPOSITORY_VERSION_KEY, "UNKNOWN".getBytes(StandardCharsets.UTF_8));
        }
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
        }
    }

    private List<RepositoryRecord> getCreateRecord(FlowFileRecord flowFileRecord, Connection connection) {
        final StandardRepositoryRecord record = new StandardRepositoryRecord(null);
        record.setWorking(flowFileRecord);
        record.setDestination(connection.getFlowFileQueue());
        return Collections.singletonList(record);

    }

    @Test
    public void testRecoveryMode() throws Exception {

        int totalFlowFiles = 50;
        final TestQueueProvider queueProvider = new TestQueueProvider();

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {

            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);

            final Connection connection = addConnectionToProvider(queueProvider, null);

            StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            ffBuilder.addAttribute("abc", "xyz");
            ffBuilder.size(0L);

            // add records to the repo
            for (int i = 1; i <= totalFlowFiles; i++) {
                repo.updateRepository(getCreateRecord(ffBuilder.id(i).build(), connection));
            }
            assertEquals(totalFlowFiles, repo.getInMemoryFlowFiles());
        }

        // restore in recovery mode with varying limits
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.ENABLE_RECOVERY_MODE.propertyName, "true");
        for (int recoveryLimit = 0; recoveryLimit < totalFlowFiles; recoveryLimit+=10) {

            additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName, Integer.toString(recoveryLimit));
            try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
                repo.initialize(new StandardResourceClaimManager());
                repo.loadFlowFiles(queueProvider);
                assertEquals(recoveryLimit, repo.getInMemoryFlowFiles());
            }
        }

        // restore in recovery mode with limit equal to available files
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName, Integer.toString(totalFlowFiles));
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);
            assertEquals(totalFlowFiles, repo.getInMemoryFlowFiles());
        }

        // restore in recovery mode with limit higher than available files
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName, Integer.toString(Integer.MAX_VALUE));
        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);
            assertEquals(totalFlowFiles, repo.getInMemoryFlowFiles());
        }

        // restore in normal mode
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.ENABLE_RECOVERY_MODE.propertyName, "false");
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName, Integer.toString(0));

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);
            assertEquals(totalFlowFiles, repo.getInMemoryFlowFiles());
        }
    }

    @Test
    public void testRecoveryModeWithContinuedLoading() throws Exception {

        int totalFlowFiles = 50;
        int recoveryLimit = 10;

        final TestQueueProvider queueProvider = new TestQueueProvider();
        List<RepositoryRecord> createdRecords = new ArrayList<>();
        final List<FlowFileRecord> flowFileCollection = new ArrayList<>();

        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.CLAIM_CLEANUP_PERIOD.propertyName, "24 hours"); // "disable" the cleanup thread, let us manually force recovery

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {

            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);

            final Connection connection = addConnectionToProvider(queueProvider, flowFileCollection);

            StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            ffBuilder.addAttribute("abc", "xyz");
            ffBuilder.size(0L);

            // add records to the repo
            for (int i = 1; i <= totalFlowFiles; i++) {
                List<RepositoryRecord> createRecord = getCreateRecord(ffBuilder.id(i).build(), connection);
                repo.updateRepository(createRecord);
                createdRecords.addAll(createRecord);
            }
            assertEquals(totalFlowFiles, repo.getInMemoryFlowFiles());
        }

        // mark all our created records for delete
        for (RepositoryRecord rec : createdRecords) {
            ((StandardRepositoryRecord) rec).markForDelete();
        }

        // restore in recovery mode
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.ENABLE_RECOVERY_MODE.propertyName, "true");
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName, Integer.toString(recoveryLimit));

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);
            assertEquals(recoveryLimit, repo.getInMemoryFlowFiles());
            assertEquals(totalFlowFiles - recoveryLimit, repo.getRecordsToRestoreCount());

            for (int i = 0; i < 4; i++) {
                List<RepositoryRecord> recordsToDelete = getRecordsToDelete(createdRecords, flowFileCollection);
                repo.updateRepository(recordsToDelete);
                flowFileCollection.clear(); // our mock only adds, doesn't remove...
                repo.doRecovery();
                assertEquals(recoveryLimit, repo.getInMemoryFlowFiles());
                assertEquals(totalFlowFiles - (recoveryLimit * (i + 2)), repo.getRecordsToRestoreCount());
            }

            // should have restored all files
            assertEquals(0, repo.getRecordsToRestoreCount());
            assertEquals(recoveryLimit, repo.getInMemoryFlowFiles());

            // delete last files
            List<RepositoryRecord> recordsToDelete = getRecordsToDelete(createdRecords, flowFileCollection);
            repo.updateRepository(recordsToDelete);
            flowFileCollection.clear(); // our mock only adds, doesn't remove...
            repo.doRecovery();

            // should have nothing left
            assertEquals(0, repo.getRecordsToRestoreCount());
            assertEquals(0, repo.getInMemoryFlowFiles());

        }

        // restore in normal mode
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.ENABLE_RECOVERY_MODE.propertyName, "false");
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName, Integer.toString(1));

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {
            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);
            assertEquals(0, repo.getRecordsToRestoreCount());
            assertEquals(0, repo.getInMemoryFlowFiles());
        }
    }

    @Test
    public void testStallStop() throws IOException {
        final TestQueueProvider queueProvider = new TestQueueProvider();

        // set stall & stop properties
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.ENABLE_STALL_STOP.propertyName, "true");
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.STALL_FLOWFILE_COUNT.propertyName, "2");
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.STOP_FLOWFILE_COUNT.propertyName, "3");

        // take heap usage out of the calculation
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.STALL_HEAP_USAGE_PERCENT.propertyName, "100%");
        additionalProperties.put(RocksDBFlowFileRepository.RocksDbProperty.STOP_HEAP_USAGE_PERCENT.propertyName, "100%");

        try (final RocksDBFlowFileRepository repo = new RocksDBFlowFileRepository(NiFiProperties.createBasicNiFiProperties(nifiPropertiesPath, additionalProperties))) {

            repo.initialize(new StandardResourceClaimManager());
            repo.loadFlowFiles(queueProvider);

            final Connection connection = addConnectionToProvider(queueProvider, null);

            StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            ffBuilder.addAttribute("abc", "xyz");
            ffBuilder.size(0L);

            List<RepositoryRecord> record1 = getCreateRecord(ffBuilder.id(1).build(), connection);
            List<RepositoryRecord> record2 = getCreateRecord(ffBuilder.id(2).build(), connection);
            List<RepositoryRecord> record3 = getCreateRecord(ffBuilder.id(3).build(), connection);

            // CREATE one... should incur no penalty
            repo.updateRepository(record1);
            repo.updateStallStop();
            assertFalse(repo.stallNewFlowFiles);
            assertFalse(repo.stopNewFlowFiles);

            // CREATE another... should stall
            repo.updateRepository(record2);
            repo.updateStallStop();
            assertTrue(repo.stallNewFlowFiles);
            assertFalse(repo.stopNewFlowFiles);

            // CREATE another... should stop
            repo.updateRepository(record3);
            repo.updateStallStop();
            assertTrue(repo.stallNewFlowFiles);
            assertTrue(repo.stopNewFlowFiles);

            // mark records for delete
            ((StandardRepositoryRecord) record1.get(0)).markForDelete();
            ((StandardRepositoryRecord) record2.get(0)).markForDelete();
            ((StandardRepositoryRecord) record3.get(0)).markForDelete();

            // DELETE one... should be stalled but not stopped
            repo.updateRepository(record1);
            repo.updateStallStop();
            assertTrue(repo.stallNewFlowFiles);
            assertFalse(repo.stopNewFlowFiles);

            // DELETE another... shouldn't be stalled or stopped
            repo.updateRepository(record1);
            repo.updateStallStop();
            assertFalse(repo.stallNewFlowFiles);
            assertFalse(repo.stopNewFlowFiles);
        }
    }

    private List<RepositoryRecord> getRecordsToDelete(List<RepositoryRecord> createdRecords, List<FlowFileRecord> flowFileCollection) {
        final Collection<Long> inMemoryIds = flowFileCollection.stream().map(FlowFile::getId).collect(Collectors.toList());
        return createdRecords.stream().filter(rec -> inMemoryIds.contains(rec.getCurrent().getId())).collect(Collectors.toList());
    }

    private Connection addConnectionToProvider(TestQueueProvider
                                                       queueProvider, List<FlowFileRecord> flowFileCollection) {
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getIdentifier()).thenReturn("1234");

        final FlowFileQueue queue = Mockito.mock(FlowFileQueue.class);
        when(queue.getIdentifier()).thenReturn("1234");
        doAnswer((Answer<Object>) invocation -> {
            if (flowFileCollection != null) {
                flowFileCollection.add((FlowFileRecord) invocation.getArguments()[0]);
            }
            return null;
        }).when(queue).put(any(FlowFileRecord.class));

        when(connection.getFlowFileQueue()).thenReturn(queue);

        queueProvider.addConnection(connection);
        return connection;


    }

    private static class TestQueueProvider implements QueueProvider {

        private List<Connection> connectionList = new ArrayList<>();

        public void addConnection(final Connection connection) {
            this.connectionList.add(connection);
        }

        @Override
        public Collection<FlowFileQueue> getAllQueues() {
            final List<FlowFileQueue> queueList = new ArrayList<>();
            for (final Connection conn : connectionList) {
                queueList.add(conn.getFlowFileQueue());
            }

            return queueList;
        }
    }

    private static class MockFlowFileSwapManager implements FlowFileSwapManager {

        private final Map<FlowFileQueue, Map<String, List<FlowFileRecord>>> swappedRecords = new HashMap<>();

        @Override
        public void initialize(SwapManagerInitializationContext initializationContext) {
        }

        @Override
        public String swapOut(List<FlowFileRecord> flowFiles, FlowFileQueue flowFileQueue, final String partitionName) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.computeIfAbsent(flowFileQueue, k -> new HashMap<>());

            final String location = UUID.randomUUID().toString();
            swapMap.put(location, new ArrayList<>(flowFiles));
            return location;
        }

        @Override
        public SwapContents peek(String swapLocation, FlowFileQueue flowFileQueue) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            final List<FlowFileRecord> flowFiles = swapMap.get(swapLocation);
            final SwapSummary summary = getSwapSummary(swapLocation);
            return new StandardSwapContents(summary, flowFiles);
        }

        @Override
        public SwapContents swapIn(String swapLocation, FlowFileQueue flowFileQueue) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            final List<FlowFileRecord> flowFiles = swapMap.remove(swapLocation);
            final SwapSummary summary = getSwapSummary(swapLocation);
            return new StandardSwapContents(summary, flowFiles);
        }

        @Override
        public List<String> recoverSwapLocations(FlowFileQueue flowFileQueue, final String partitionName) throws IOException {
            Map<String, List<FlowFileRecord>> swapMap = swappedRecords.get(flowFileQueue);
            if (swapMap == null) {
                return null;
            }

            return new ArrayList<>(swapMap.keySet());
        }

        @Override
        public SwapSummary getSwapSummary(String swapLocation) throws IOException {
            List<FlowFileRecord> records = null;
            for (final Map<String, List<FlowFileRecord>> swapMap : swappedRecords.values()) {
                records = swapMap.get(swapLocation);
                if (records != null) {
                    break;
                }
            }

            if (records == null) {
                return null;
            }

            final List<ResourceClaim> resourceClaims = new ArrayList<>();
            long size = 0L;
            Long maxId = null;
            for (final FlowFileRecord flowFile : records) {
                size += flowFile.getSize();

                if (maxId == null || flowFile.getId() > maxId) {
                    maxId = flowFile.getId();
                }

                final ContentClaim contentClaim = flowFile.getContentClaim();
                if (contentClaim != null) {
                    final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
                    resourceClaims.add(resourceClaim);
                }
            }

            return new StandardSwapSummary(new QueueSize(records.size(), size), maxId, resourceClaims);
        }

        @Override
        public void purge() {
            this.swappedRecords.clear();
        }

        @Override
        public Set<String> getSwappedPartitionNames(FlowFileQueue queue) throws IOException {
            return Collections.emptySet();
        }

        @Override
        public String changePartitionName(String swapLocation, String newPartitionName) throws IOException {
            return swapLocation;
        }
    }
}
