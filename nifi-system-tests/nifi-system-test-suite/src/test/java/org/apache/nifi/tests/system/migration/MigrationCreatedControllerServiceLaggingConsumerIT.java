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

import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A consumer that is one version behind the flow author when both upgrade their runtime.
 *
 * The author is on V2 (processors A and B) at the upgrade, so both processors migrate together. The consumer is still on V1
 * (processor A only), so only A migrates at the upgrade. The consumer then changes directly to V3, which the author saved after
 * the upgrade and which declares the migration-created services, so B arrives already migrated.
 *
 * Each test checks that the consumer's processors keep their own services, with their state and secrets, across those updates.
 */
class MigrationCreatedControllerServiceLaggingConsumerIT extends AbstractNarSwapMigrationIT {

    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String VERSION_1 = "1";
    private static final String VERSION_2 = "2";
    private static final String VERSION_3 = "3";

    private static final String MIGRATING_PROCESSOR_NAME = "MigrateToControllerService";
    private static final String STORE_SERVICE_PROPERTY = "Store Service";
    private static final String STORE_NAME_PROPERTY = "Store Name";
    private static final String STORE_PASSWORD_PROPERTY = "Store Password";
    private static final String STORE_NAME = "store";
    private static final String PROCESSOR_A = "Processor A";
    private static final String PROCESSOR_B = "Processor B";

    private static final String CREATED_STATE_KEY = "created";
    private static final String PASSWORD_STATE_KEY = "password";
    private static final String ROOT_PG = "root";

    @TempDir
    Path registryDirectory;

    /**
     * A and B request identical services, so the reuse lookup gives them one shared service on every instance.
     */
    @Test
    void testSharedServiceSurvivesLaggingConsumerUpdates() throws NiFiClientException, IOException, InterruptedException {
        verifySharedServiceSurvives("Shared Service", false);
    }

    /**
     * Case Z for X: the consumer first changes to V2, which was saved before the upgrade and so holds the legacy properties.
     */
    @Test
    void testSharedServiceSurvivesLaggingConsumerUpdatesThroughLegacyVersion() throws NiFiClientException, IOException, InterruptedException {
        verifySharedServiceSurvives("Shared Service Via Legacy V2", true);
    }

    private void verifySharedServiceSurvives(final String flowName, final boolean throughLegacyVersion)
            throws NiFiClientException, IOException, InterruptedException {
        final Scenario scenario = setUpBeforeUpgrade(flowName, null, null, null);
        upgradeRuntime();

        final Map<String, StoreSnapshot> authorStores = readStores(scenario.authorGroupId());
        saveFlowVersion(scenario.authorGroupId(), scenario.registryClient());

        final StoreSnapshot consumerStoreBefore = readStores(scenario.consumerGroupId()).get(PROCESSOR_A);
        changeToVersion3(scenario, throughLegacyVersion);
        final Map<String, StoreSnapshot> consumerStores = readStores(scenario.consumerGroupId());

        final List<Executable> checks = new ArrayList<>();
        checks.add(() -> assertEquals(authorStores.get(PROCESSOR_A).serviceId(), authorStores.get(PROCESSOR_B).serviceId(), "author processors share one service"));
        addPreservationChecks(checks, consumerStoreBefore, consumerStores.get(PROCESSOR_A));
        checks.add(() -> assertEquals(consumerStores.get(PROCESSOR_A).serviceId(), consumerStores.get(PROCESSOR_B).serviceId(), "consumer processors share one service"));
        checks.add(() -> assertEquals(authorStores.get(PROCESSOR_A).versionedId(), consumerStores.get(PROCESSOR_A).versionedId(), "consumer service versioned ID equals author's"));
        assertAll(checks);
    }

    /**
     * A and B request services that differ only in a secret, so each processor has its own service, and the services tie once
     * the secret is left out of the versioned ID.
     */
    @Test
    void testServicesWithDifferentSecretsSurviveLaggingConsumerUpdates() throws NiFiClientException, IOException, InterruptedException {
        verifyServicesWithDifferentSecretsSurvive("Different Secrets", false);
    }

    /**
     * Case Z for Y: the consumer first changes to V2, which was saved before the upgrade and so holds the legacy properties.
     */
    @Test
    void testServicesWithDifferentSecretsSurviveLaggingConsumerUpdatesThroughLegacyVersion() throws NiFiClientException, IOException, InterruptedException {
        verifyServicesWithDifferentSecretsSurvive("Different Secrets Via Legacy V2", true);
    }

    private void verifyServicesWithDifferentSecretsSurvive(final String flowName, final boolean throughLegacyVersion)
            throws NiFiClientException, IOException, InterruptedException {
        final Scenario scenario = setUpBeforeUpgrade(flowName, "author-secret-a", "author-secret-b", "consumer-secret-a");
        upgradeRuntime();

        final Map<String, StoreSnapshot> authorStores = readStores(scenario.authorGroupId());
        saveFlowVersion(scenario.authorGroupId(), scenario.registryClient());

        final StoreSnapshot consumerStoreBefore = readStores(scenario.consumerGroupId()).get(PROCESSOR_A);
        changeToVersion3(scenario, throughLegacyVersion);
        final Map<String, StoreSnapshot> consumerStores = readStores(scenario.consumerGroupId());

        final List<Executable> checks = new ArrayList<>();
        checks.add(() -> assertEquals("author-secret-a", authorStores.get(PROCESSOR_A).password(), "author A's service has A's secret"));
        checks.add(() -> assertEquals("author-secret-b", authorStores.get(PROCESSOR_B).password(), "author B's service has B's secret"));
        addPreservationChecks(checks, consumerStoreBefore, consumerStores.get(PROCESSOR_A));
        checks.add(() -> assertEquals("consumer-secret-a", consumerStores.get(PROCESSOR_A).password(), "consumer A's service still has consumer A's secret"));
        checks.add(() -> assertEquals(authorStores.get(PROCESSOR_A).versionedId(), consumerStores.get(PROCESSOR_A).versionedId(), "consumer A's service versioned ID equals author A's"));
        checks.add(() -> assertEquals(authorStores.get(PROCESSOR_B).versionedId(), consumerStores.get(PROCESSOR_B).versionedId(), "consumer B's service versioned ID equals author B's"));
        assertAll(checks);
    }

    private void changeToVersion3(final Scenario scenario, final boolean throughLegacyVersion) throws NiFiClientException, IOException, InterruptedException {
        if (throughLegacyVersion) {
            getClientUtil().changeFlowVersion(scenario.consumerGroupId(), VERSION_2, true);
            readStores(scenario.consumerGroupId());
        }
        getClientUtil().changeFlowVersion(scenario.consumerGroupId(), VERSION_3, true);
    }

    private void addPreservationChecks(final List<Executable> checks, final StoreSnapshot before, final StoreSnapshot after) {
        checks.add(() -> assertEquals(before.serviceId(), after.serviceId(), "consumer A keeps the same service instance"));
        checks.add(() -> assertEquals(before.created(), after.created(), "consumer A's service keeps its state"));
    }

    /**
     * Builds, with the pre-upgrade extensions, an author flow saved as V1 (A) and V2 (A and B), and a consumer that imports V1.
     */
    private Scenario setUpBeforeUpgrade(final String flowName, final String authorPasswordA, final String authorPasswordB, final String consumerPasswordA)
            throws NiFiClientException, IOException {
        Files.createDirectories(registryDirectory.resolve(TEST_FLOWS_BUCKET));
        final FlowRegistryClientEntity registryClient = registerClient(registryDirectory.toFile());

        final ProcessGroupEntity authorGroup = getClientUtil().createProcessGroup(flowName, ROOT_PG);
        createMigratingProcessor(authorGroup.getId(), PROCESSOR_A, authorPasswordA);
        final VersionControlInformationEntity vci = getClientUtil().startVersionControl(authorGroup, registryClient, TEST_FLOWS_BUCKET, flowName);
        final String flowId = vci.getVersionControlInformation().getFlowId();

        createMigratingProcessor(authorGroup.getId(), PROCESSOR_B, authorPasswordB);
        saveFlowVersion(authorGroup.getId(), registryClient);

        final ProcessGroupEntity consumerGroup = getClientUtil().importFlowFromRegistry(ROOT_PG, registryClient.getId(), TEST_FLOWS_BUCKET, flowId, VERSION_1);
        if (consumerPasswordA != null) {
            final ProcessorEntity consumerProcessorA = findProcessor(consumerGroup.getId(), PROCESSOR_A);
            getClientUtil().updateProcessorProperties(consumerProcessorA, Map.of(STORE_PASSWORD_PROPERTY, consumerPasswordA));
        }

        return new Scenario(registryClient, authorGroup.getId(), consumerGroup.getId());
    }

    private void createMigratingProcessor(final String groupId, final String name, final String password) throws NiFiClientException, IOException {
        final ProcessorEntity processor = getClientUtil().createProcessor(MIGRATING_PROCESSOR_NAME, groupId);
        processor.getComponent().setName(name);
        final ProcessorEntity renamed = getNifiClient().getProcessorClient().updateProcessor(processor);

        final Map<String, String> properties = new HashMap<>();
        properties.put(STORE_NAME_PROPERTY, STORE_NAME);
        if (password != null) {
            properties.put(STORE_PASSWORD_PROPERTY, password);
        }

        final ProcessorEntity configured = getClientUtil().updateProcessorProperties(renamed, properties);
        getClientUtil().setAutoTerminatedRelationships(configured, "success");
    }

    private void upgradeRuntime() throws IOException {
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);
    }

    private void saveFlowVersion(final String groupId, final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException {
        getClientUtil().saveFlowVersion(getNifiClient().getProcessGroupClient().getProcessGroup(groupId), registryClient,
                getNifiClient().getVersionsClient().getVersionControlInfo(groupId));
    }

    /**
     * Reads, for each migrating processor in the group, the service it references, keyed by processor name.
     */
    private Map<String, StoreSnapshot> readStores(final String groupId) throws NiFiClientException, IOException, InterruptedException {
        final Map<String, StoreSnapshot> stores = new HashMap<>();
        for (final ProcessorEntity processor : getNifiClient().getFlowClient().getProcessGroup(groupId).getProcessGroupFlow().getFlow().getProcessors()) {
            final String serviceId = processor.getComponent().getConfig().getProperties().get(STORE_SERVICE_PROPERTY);
            if (serviceId == null) {
                continue;
            }

            final ControllerServiceEntity service = getNifiClient().getControllerServicesClient().getControllerService(serviceId);
            Map<String, String> state = readState(serviceId);
            for (int attempt = 0; attempt < 300 && state.get(CREATED_STATE_KEY) == null; attempt++) {
                Thread.sleep(100L);
                state = readState(serviceId);
            }

            final StoreSnapshot snapshot = new StoreSnapshot(serviceId, service.getComponent().getVersionedComponentId(), state.get(CREATED_STATE_KEY), state.get(PASSWORD_STATE_KEY));
            stores.put(processor.getComponent().getName(), snapshot);
        }

        return stores;
    }

    private Map<String, String> readState(final String serviceId) throws NiFiClientException, IOException {
        final ComponentStateEntity stateEntity = getNifiClient().getControllerServicesClient().getControllerServiceState(serviceId);
        final ComponentStateDTO componentState = stateEntity.getComponentState();
        final Map<String, String> state = new HashMap<>();
        if (componentState != null && componentState.getLocalState() != null && componentState.getLocalState().getState() != null) {
            for (final StateEntryDTO entry : componentState.getLocalState().getState()) {
                state.put(entry.getKey(), entry.getValue());
            }
        }

        return state;
    }

    private ProcessorEntity findProcessor(final String groupId, final String name) throws NiFiClientException, IOException {
        return getNifiClient().getFlowClient().getProcessGroup(groupId).getProcessGroupFlow().getFlow().getProcessors().stream()
                .filter(processor -> name.equals(processor.getComponent().getName()))
                .findFirst()
                .orElseThrow();
    }

    private record Scenario(FlowRegistryClientEntity registryClient, String authorGroupId, String consumerGroupId) {
    }

    private record StoreSnapshot(String serviceId, String versionedId, String created, String password) {
    }
}
