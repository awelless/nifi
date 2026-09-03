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

package org.apache.nifi.tests.system.migration;

import org.apache.nifi.migration.StandardControllerServiceFactory;
import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that a Controller Service created by property migration survives the operations a deployed flow goes
 * through: a runtime upgrade that introduces the migration, a plain runtime restart, and version changes of the
 * enclosing versioned Process Group.
 *
 * Two independent flow lineages are pre-seeded in the registry. In the first, a later version declares the store
 * Controller Service itself, as a published flow would once the vendor adds it. In the second, no version ever
 * declares the service, so the flow relies entirely on property migration to create it, and the later version only
 * adds an unrelated processor.
 */
public class MigrationCreatedControllerServiceVersioningIT extends AbstractNarSwapMigrationIT {
    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String SERVICE_DECLARED_FLOW_ID = "11111111-2222-3333-4444-555555555555";
    private static final String SERVICE_ABSENT_FLOW_ID = "22222222-3333-4444-5555-666666666666";
    private static final String STORE_SERVICE_PROPERTY = "Store Service";
    private static final String STORE_SERVICE_TYPE = "org.apache.nifi.cs.tests.system.FileBackedStoreService";
    private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.tests.system.MigrateToControllerService";
    private static final String MIGRATING_PROCESSOR_NAME = "MigrateToControllerService";
    private static final String ADDED_PROCESSOR_NAME = "Added Processor";
    private static final String STORE_DIRECTORY_NAME = "store";
    private static final long STORE_GROWTH_TIMEOUT_MILLIS = 30_000L;

    /**
     * After the runtime is upgraded, the Controller Service that property migration creates must be present,
     * enabled and referenced, and the flow that was running before the upgrade must be running again, with no manual action.
     */
    @Test
    public void testRuntimeUpgradeCreatesEnabledServiceAndKeepsFlowRunning() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_DECLARED_FLOW_ID);
        final ControllerServiceEntity service = waitForSingleStoreService(flow.groupId());
        final String serviceId = service.getComponent().getId();

        assertEquals(StandardControllerServiceFactory.MIGRATION_CREATED_COMMENT, service.getComponent().getComments());
        waitForControllerServiceEnabled(serviceId);
        waitForProcessorRunning(flow.processorId());
        assertEquals(serviceId, getStoreServiceId(flow.processorId()));

        final Collection<String> validationErrors = getNifiClient().getProcessorClient().getProcessor(flow.processorId()).getComponent().getValidationErrors();
        final boolean processorValid = validationErrors == null || validationErrors.isEmpty();
        assertTrue(processorValid, "Processor must be valid after the runtime upgrade");

        waitFor(() -> countRows(serviceId) > 0);

        final String versionedFlowState = getClientUtil().getVersionedFlowState(flow.groupId(), "root");
        assertNotEquals("LOCALLY_MODIFIED", versionedFlowState, "The migration-created Controller Service must not make the flow dirty");
        assertNotEquals("LOCALLY_MODIFIED_AND_STALE", versionedFlowState, "The migration-created Controller Service must not make the flow dirty");

        final boolean serviceReportedAsLocalModification = getNifiClient().getProcessGroupClient().getLocalModifications(flow.groupId())
                .getComponentDifferences().stream()
                .anyMatch(diff -> serviceId.equals(diff.getComponentId()));
        assertFalse(serviceReportedAsLocalModification);
    }

    /**
     * Upgrading to a flow version that declares the store Controller Service must keep using the service that
     * property migration already created, rather than removing it and substituting the one the published version declares.
     *
     * FAILS
     * The flow upgrade replaced the migration-created Controller Service with a different one ==>
     * Expected :d4c71d8a-c6a7-3227-b436-c82f32f863bd
     * Actual   :9309b690-d4cd-3069-1250-a84fb4936295
     */
    @Test
    public void testFlowUpgradePreservesMigrationCreatedControllerService() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_DECLARED_FLOW_ID);
        final String serviceId = awaitPopulatedStoreService(flow);

        final VersionedFlowUpdateRequestEntity upgradeRequest = getClientUtil().changeFlowVersion(flow.groupId(), "2", false);

        assertStorePreserved(flow, serviceId, "flow upgrade");
        assertNull(upgradeRequest.getRequest().getFailureReason(),
                "The migration-created Controller Service must not prevent the versioned flow from upgrading");
    }

    /**
     * A flow whose definition never declares the store Controller Service relies entirely on property migration to
     * create it. Upgrading such a flow to a version that only adds an unrelated processor, leaving the migrating
     * processor untouched, must not disturb the service: the version change has nothing to say about it.
     *
     * FAILS
     * The migration-created Controller Service was removed during the flow upgrade, destroying its store ==>
     * Expected :false
     * Actual   :true
     */
    @Test
    public void testFlowUpgradeAddingUnrelatedProcessorPreservesMigrationCreatedControllerService() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_ABSENT_FLOW_ID);
        final String serviceId = awaitPopulatedStoreService(flow);

        final VersionedFlowUpdateRequestEntity upgradeRequest = getClientUtil().changeFlowVersion(flow.groupId(), "2", false);

        assertTrue(hasProcessorNamed(flow.groupId(), ADDED_PROCESSOR_NAME), "The upgrade must have added the unrelated processor");

        assertStorePreserved(flow, serviceId, "flow upgrade");
        assertNull(upgradeRequest.getRequest().getFailureReason(),
                "The migration-created Controller Service must not prevent the versioned flow from upgrading");
    }

    /**
     * Downgrading to a flow version published before the store Controller Service existed must not destroy the store.
     *
     * FAILS
     * Precondition failed: the upgrade to version 2 already removed the service, so the downgrade cannot be evaluated ==>
     * Expected :false
     * Actual   :true
     *
     * i'm not sure if we want to support it actually
     */
    @Test
    public void testFlowDowngradePreservesMigrationCreatedControllerService() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_DECLARED_FLOW_ID);
        final String serviceId = awaitPopulatedStoreService(flow);

        getClientUtil().changeFlowVersion(flow.groupId(), "2", false);
        assertFalse(removalMarkerFile(serviceId).exists(),
                "Precondition failed: the upgrade to version 2 already removed the service, so the downgrade cannot be evaluated");

        final VersionedFlowUpdateRequestEntity downgradeRequest = getClientUtil().changeFlowVersion(flow.groupId(), "1", false);

        assertStorePreserved(flow, serviceId, "flow downgrade");
        assertNull(downgradeRequest.getRequest().getFailureReason(),
                "The migration-created Controller Service must not prevent the versioned flow from being downgraded");
    }

    /**
     * Restarting the runtime with no NAR change must reuse the Controller Service that property migration
     * created previously rather than creating a second one.
     */
    @Test
    public void testRuntimeRestartDoesNotRecreateMigrationCreatedControllerService() throws NiFiClientException, IOException, InterruptedException {
        final MigratedFlow flow = importAndUpgradeRuntime(SERVICE_DECLARED_FLOW_ID);
        final String serviceId = awaitPopulatedStoreService(flow);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        assertStorePreserved(flow, serviceId, "runtime restart");
    }

    /**
     * Imports version 1 of the given pre-seeded flow, waits for its processor to be running, and then simulates a
     * runtime upgrade by stopping NiFi, swapping in the alternate-config extensions and starting NiFi again. Nothing
     * in the flow is stopped or started by hand, so the assertions that follow observe what the upgrade does on its own.
     */
    private MigratedFlow importAndUpgradeRuntime(final String flowId) throws NiFiClientException, IOException, InterruptedException {
        final File storageDirectory = copySnapshotsToTarget();
        final FlowRegistryClientEntity registryClient = registerClient(storageDirectory);
        final ProcessGroupEntity group = getClientUtil().importFlowFromRegistry("root", registryClient.getId(), TEST_FLOWS_BUCKET, flowId, "1");
        final ProcessorEntity processor = findProcessor(group.getId(), MIGRATING_PROCESSOR_NAME);
        waitForProcessorRunning(processor.getId());

        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);

        return new MigratedFlow(group.getId(), processor.getId());
    }

    /**
     * Waits until the migration-created Controller Service is enabled and its store has accumulated at least one row,
     * so that the flow is known to be working before the operation under test runs, and returns its identifier.
     */
    private String awaitPopulatedStoreService(final MigratedFlow flow) throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity service = waitForSingleStoreService(flow.groupId());
        final String serviceId = service.getComponent().getId();
        waitForControllerServiceEnabled(serviceId);
        waitFor(() -> countRows(serviceId) > 0);

        return serviceId;
    }

    /**
     * Asserts that the given operation left the migration-created Controller Service in place and the flow working.
     *
     * Two independent properties are checked. The absence of the removal marker proves the service was never torn
     * down, which is the only thing that destroys the store. A row count that keeps climbing afterwards proves the
     * flow is doing work again, which a RUNNING state alone does not show: a processor whose Controller Service
     * reference is broken still reports RUNNING while writing nothing.
     *
     * The store is read through the service the processor references now rather than the one recorded earlier, so
     * that a service substituted by the operation is detected rather than silently passing.
     */
    private void assertStorePreserved(final MigratedFlow flow, final String serviceId, final String operation)
            throws NiFiClientException, IOException, InterruptedException {

        final ControllerServiceEntity serviceAfter = waitForSingleStoreService(flow.groupId());
        assertEquals(serviceId, serviceAfter.getComponent().getId(),
                "The " + operation + " replaced the migration-created Controller Service with a different one");
        assertEquals(serviceId, getStoreServiceId(flow.processorId()));

        waitForControllerServiceEnabled(serviceId);
        waitForProcessorRunning(flow.processorId());

        assertFalse(removalMarkerFile(serviceId).exists(),
                "The migration-created Controller Service was removed during the " + operation + ", destroying its store");

        final long rowsAfterOperation = countRows(getStoreServiceId(flow.processorId()));
        final long deadline = System.currentTimeMillis() + STORE_GROWTH_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            if (countRows(getStoreServiceId(flow.processorId())) > rowsAfterOperation) {
                return;
            }

            Thread.sleep(100L);
        }

        fail("The flow stopped writing to the store after the " + operation + "; the row count stayed at " + rowsAfterOperation);
    }

    private File copySnapshotsToTarget() throws IOException {
        final String sanitisedTestName = getTestName().replaceAll("[^a-zA-Z0-9_-]", "-");
        final Path source = Path.of("src/test/resources/versioned-flows");
        final Path destination = Path.of("target/versioned-flows", sanitisedTestName);
        Files.createDirectories(destination);

        try (final Stream<Path> paths = Files.walk(source)) {
            paths.forEach(sourcePath -> {
                try {
                    final Path targetPath = destination.resolve(source.relativize(sourcePath));
                    if (Files.isDirectory(sourcePath)) {
                        Files.createDirectories(targetPath);
                    } else {
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        return destination.toFile();
    }

    private ProcessorEntity findProcessor(final String groupId, final String name) throws NiFiClientException, IOException {
        final List<ProcessorEntity> matching = getNifiClient().getFlowClient().getProcessGroup(groupId).getProcessGroupFlow().getFlow().getProcessors().stream()
                .filter(processor -> PROCESSOR_TYPE.equals(processor.getComponent().getType()))
                .filter(processor -> name.equals(processor.getComponent().getName()))
                .toList();

        if (matching.size() != 1) {
            throw new AssertionError("Expected exactly one processor named " + name + " in group " + groupId + " but found " + matching.size());
        }

        return matching.getFirst();
    }

    private boolean hasProcessorNamed(final String groupId, final String name) throws NiFiClientException, IOException {
        return getNifiClient().getFlowClient().getProcessGroup(groupId).getProcessGroupFlow().getFlow().getProcessors().stream()
                .anyMatch(processor -> name.equals(processor.getComponent().getName()));
    }

    private List<ControllerServiceEntity> findStoreServices(final String groupId) throws NiFiClientException, IOException {
        return getNifiClient().getFlowClient().getControllerServices(groupId).getControllerServices().stream()
                .filter(service -> STORE_SERVICE_TYPE.equals(service.getComponent().getType()))
                .filter(service -> groupId.equals(service.getComponent().getParentGroupId()))
                .toList();
    }

    private ControllerServiceEntity waitForSingleStoreService(final String groupId) throws NiFiClientException, IOException, InterruptedException {
        waitFor(() -> findStoreServices(groupId).size() == 1);
        return findStoreServices(groupId).getFirst();
    }

    private String getStoreServiceId(final String processorId) throws NiFiClientException, IOException {
        final Map<String, String> properties = getNifiClient().getProcessorClient().getProcessor(processorId).getComponent().getConfig().getProperties();
        return properties.get(STORE_SERVICE_PROPERTY);
    }

    private File storeDirectory() {
        return new File(getNiFiInstance().getInstanceDirectory(), STORE_DIRECTORY_NAME);
    }

    private File storeFile(final String serviceId) {
        return new File(storeDirectory(), "store-" + serviceId + ".log");
    }

    private File removalMarkerFile(final String serviceId) {
        return new File(storeDirectory(), "removed-" + serviceId + ".log");
    }

    private long countRows(final String serviceId) {
        final File file = storeFile(serviceId);
        if (!file.exists()) {
            return 0;
        }

        try {
            return Files.readAllLines(file.toPath()).size();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void waitForProcessorRunning(final String processorId) throws NiFiClientException, IOException, InterruptedException {
        waitFor(() -> "RUNNING".equals(getNifiClient().getProcessorClient().getProcessor(processorId).getComponent().getState()));
    }

    private void waitForControllerServiceEnabled(final String serviceId) throws NiFiClientException, IOException, InterruptedException {
        waitFor(() -> "ENABLED".equals(getNifiClient().getControllerServicesClient().getControllerService(serviceId).getComponent().getState()));
    }

    private record MigratedFlow(String groupId, String processorId) {
    }

}
