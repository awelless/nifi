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
package org.apache.nifi.controller.scheduling;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.components.validation.VerifiableComponentFactory;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ExtensionBuilder;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.StandardProcessorNode;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.reporting.StandardReportingInitializationContext;
import org.apache.nifi.controller.reporting.StandardReportingTaskNode;
import org.apache.nifi.controller.repository.ActiveProcessSessionFactory;
import org.apache.nifi.controller.repository.WeakHashMapProcessSessionFactory;
import org.apache.nifi.controller.scheduling.processors.FailOnScheduledProcessor;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.controller.service.mock.MockProcessGroup;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.lifecycle.ProcessorStopLifecycleMethods;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.StandardValidationContextFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.SynchronousValidationTrigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class TestStandardProcessScheduler {

    private StandardProcessScheduler scheduler = null;
    private ReportingTaskNode taskNode = null;
    private TestReportingTask reportingTask = null;
    private final StateManagerProvider stateMgrProvider = Mockito.mock(StateManagerProvider.class);
    private FlowController controller;
    private FlowManager flowManager;
    private ProcessGroup rootGroup;
    private NiFiProperties nifiProperties;
    private Bundle systemBundle;
    private ExtensionDiscoveringManager extensionManager;
    private ControllerServiceProvider serviceProvider;

    private volatile String propsFile = TestStandardProcessScheduler.class.getResource("/standardprocessschedulertest.nifi.properties").getFile();

    @BeforeEach
    public void setup() throws InitializationException {
        final Map<String, String> overrideProperties = new HashMap<>();
        overrideProperties.put(NiFiProperties.ADMINISTRATIVE_YIELD_DURATION, "2 millis");
        overrideProperties.put(NiFiProperties.PROCESSOR_SCHEDULING_TIMEOUT, "10 millis");
        this.nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, overrideProperties);

        // load the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        scheduler = new StandardProcessScheduler(new FlowEngine(1, "Unit Test", true), Mockito.mock(FlowController.class),
            stateMgrProvider, nifiProperties, new StandardLifecycleStateManager());
        scheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, Mockito.mock(SchedulingAgent.class));

        reportingTask = new TestReportingTask();
        final ReportingInitializationContext config = new StandardReportingInitializationContext(UUID.randomUUID().toString(), "Test", SchedulingStrategy.TIMER_DRIVEN, "5 secs",
                Mockito.mock(ComponentLog.class), null, KerberosConfig.NOT_CONFIGURED, null);
        reportingTask.initialize(config);

        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(null);
        final TerminationAwareLogger logger = Mockito.mock(TerminationAwareLogger.class);
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final LoggableComponent<ReportingTask> loggableComponent = new LoggableComponent<>(reportingTask, systemBundle.getBundleDetails().getCoordinate(), logger);
        taskNode = new StandardReportingTaskNode(loggableComponent, UUID.randomUUID().toString(), Mockito.mock(FlowController.class), scheduler, validationContextFactory,
            reloadComponent, extensionManager, new SynchronousValidationTrigger());

        flowManager = Mockito.mock(FlowManager.class);
        controller = Mockito.mock(FlowController.class);
        when(controller.getFlowManager()).thenReturn(flowManager);
        Mockito.when(controller.getExtensionManager()).thenReturn(extensionManager);

        serviceProvider = new StandardControllerServiceProvider(scheduler, null, flowManager, extensionManager);

        final ConcurrentMap<String, ProcessorNode> processorMap = new ConcurrentHashMap<>();
        Mockito.doAnswer((Answer<ProcessorNode>) invocation -> {
            final String id = invocation.getArgument(0);
            return processorMap.get(id);
        }).when(flowManager).getProcessorNode(Mockito.anyString());

        Mockito.doAnswer((Answer<Object>) invocation -> {
            final ProcessorNode procNode = invocation.getArgument(0);
            processorMap.putIfAbsent(procNode.getIdentifier(), procNode);
            return null;
        }).when(flowManager).onProcessorAdded(any(ProcessorNode.class));

        when(controller.getControllerServiceProvider()).thenReturn(serviceProvider);

        rootGroup = new MockProcessGroup(flowManager);
        when(flowManager.getGroup(Mockito.anyString())).thenReturn(rootGroup);

        when(controller.getReloadComponent()).thenReturn(Mockito.mock(ReloadComponent.class));

        doAnswer((Answer<ControllerServiceNode>) invocation -> {
            final String type = invocation.getArgument(0);
            final String id = invocation.getArgument(1);
            final BundleCoordinate bundleCoordinate = invocation.getArgument(2);

            final ControllerServiceNode serviceNode = new ExtensionBuilder()
                .identifier(id)
                .type(type)
                .bundleCoordinate(bundleCoordinate)
                .controllerServiceProvider(serviceProvider)
                .processScheduler(Mockito.mock(ProcessScheduler.class))
                .nodeTypeProvider(Mockito.mock(NodeTypeProvider.class))
                .validationTrigger(Mockito.mock(ValidationTrigger.class))
                .reloadComponent(Mockito.mock(ReloadComponent.class))
                .verifiableComponentFactory(Mockito.mock(VerifiableComponentFactory.class))
                .stateManagerProvider(Mockito.mock(StateManagerProvider.class))
                .extensionManager(extensionManager)
                .buildControllerService();

            serviceProvider.onControllerServiceAdded(serviceNode);
            return serviceNode;
        }).when(flowManager).createControllerService(anyString(), anyString(), any(BundleCoordinate.class),
            AdditionalMatchers.or(anySet(), isNull()), anyBoolean(), anyBoolean(), nullable(String.class));
    }

    @AfterEach
    public void after() throws Exception {
        controller.shutdown(true);
        FileUtils.deleteDirectory(new File("./target/standardprocessschedulertest"));
    }

    /**
     * We have run into an issue where a Reporting Task is scheduled to run but
     * throws an Exception from a method with the @OnScheduled annotation. User
     * stops Reporting Task, updates configuration to fix the issue. Reporting
     * Task then finishes running @OnSchedule method and is then scheduled to
     * run. This unit test is intended to verify that we have this resolved.
     */
    @Test
    public void testReportingTaskDoesntKeepRunningAfterStop() throws InterruptedException {
        taskNode.performValidation();
        scheduler.schedule(taskNode);

        // Let it try to run a few times.
        Thread.sleep(25L);

        scheduler.unschedule(taskNode);

        final int attempts = reportingTask.onScheduleAttempts.get();
        // give it a sec to make sure that it's finished running.
        Thread.sleep(250L);
        final int attemptsAfterStop = reportingTask.onScheduleAttempts.get() - attempts;

        // allow 1 extra run, due to timing issues that could call it as it's being stopped.
        assertTrue(attemptsAfterStop <= 1,
                "After unscheduling Reporting Task, task ran an additional " + attemptsAfterStop + " times");
    }

    @Test
    @Timeout(60)
    public void testDisableControllerServiceWithProcessorTryingToStartUsingIt() throws InterruptedException, ExecutionException {
        final String uuid = UUID.randomUUID().toString();
        final Processor proc = new ServiceReferencingProcessor();
        proc.initialize(new StandardProcessorInitializationContext(uuid, null, null, null, KerberosConfig.NOT_CONFIGURED));

        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final VerifiableComponentFactory verifiableComponentFactory = Mockito.mock(VerifiableComponentFactory.class);

        final ControllerServiceNode service = flowManager.createControllerService(NoStartServiceImpl.class.getName(), "service",
                systemBundle.getBundleDetails().getCoordinate(), null, true, true, null);

        rootGroup.addControllerService(service);

        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);
        final ValidationContextFactory validationContextFactory = new StandardValidationContextFactory(serviceProvider);
        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, uuid, validationContextFactory, scheduler,
            serviceProvider, reloadComponent, verifiableComponentFactory, extensionManager, new SynchronousValidationTrigger());

        rootGroup.addProcessor(procNode);

        Map<String, String> procProps = new HashMap<>();
        procProps.put(ServiceReferencingProcessor.SERVICE_DESC.getName(), service.getIdentifier());
        procNode.setProperties(procProps);

        service.performValidation();
        scheduler.enableControllerService(service);

        procNode.performValidation();
        scheduler.startProcessor(procNode, true);

        Thread.sleep(25L);

        scheduler.stopProcessor(procNode, ProcessorStopLifecycleMethods.TRIGGER_ALL);
        assertTrue(service.isActive());
        assertSame(ControllerServiceState.ENABLING, service.getState());
        scheduler.disableControllerService(service).get();
        assertFalse(service.isActive());
        assertSame(ControllerServiceState.DISABLED, service.getState());
    }

    public class TestReportingTask extends AbstractReportingTask {

        private final AtomicBoolean failOnScheduled = new AtomicBoolean(true);
        private final AtomicInteger onScheduleAttempts = new AtomicInteger(0);
        private final AtomicInteger triggerCount = new AtomicInteger(0);

        @OnScheduled
        public void onScheduled() {
            onScheduleAttempts.incrementAndGet();

            if (failOnScheduled.get()) {
                throw new RuntimeException("Intentional Exception for testing purposes");
            }
        }

        @Override
        public void onTrigger(final ReportingContext context) {
            triggerCount.getAndIncrement();
        }
    }

    public static class ServiceReferencingProcessor extends AbstractProcessor {

        static final PropertyDescriptor SERVICE_DESC = new PropertyDescriptor.Builder()
                .name("service")
                .identifiesControllerService(NoStartService.class)
                .required(true)
                .build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            final List<PropertyDescriptor> properties = new ArrayList<>();
            properties.add(SERVICE_DESC);
            return properties;
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        }
    }

    /**
     * Validates the atomic nature of ControllerServiceNode.enable() method
     * which must only trigger @OnEnabled once, regardless of how many threads
     * may have a reference to the underlying ProcessScheduler and
     * ControllerServiceNode.
     */
    @Test
    public void validateServiceEnablementLogicHappensOnlyOnce() {
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(SimpleTestService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true, null);

        serviceNode.performValidation();

        assertFalse(serviceNode.isActive());
        final SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        final AtomicBoolean asyncFailed = new AtomicBoolean();

        try (final ExecutorService executor = Executors.newCachedThreadPool()) {
            for (int i = 0; i < 1000; i++) {
                executor.execute(() -> {
                    try {
                        scheduler.enableControllerService(serviceNode).get();
                        assertTrue(serviceNode.isActive());
                    } catch (final Exception e) {
                        asyncFailed.set(true);
                    }
                });
            }
        }

        assertFalse(asyncFailed.get());
        assertEquals(1, ts.enableInvocationCount());
    }

    /**
     * Validates the atomic nature of ControllerServiceNode.disable(..) method
     * which must never trigger @OnDisabled, regardless of how many threads may
     * have a reference to the underlying ProcessScheduler and
     * ControllerServiceNode.
     */
    @Test
    public void validateDisabledServiceCantBeDisabled() throws Exception {
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(SimpleTestService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true, null);
        final SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        final ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(() -> {
                try {
                    scheduler.disableControllerService(serviceNode);
                    assertFalse(serviceNode.isActive());
                } catch (final Exception e) {
                    asyncFailed.set(true);
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        assertFalse(asyncFailed.get());
        assertEquals(0, ts.disableInvocationCount());
    }

    /**
     * Validates the atomic nature of ControllerServiceNode.disable() method
     * which must only trigger @OnDisabled once, regardless of how many threads
     * may have a reference to the underlying ProcessScheduler and
     * ControllerServiceNode.
     */
    @Test
    public void validateEnabledServiceCanOnlyBeDisabledOnce() throws Exception {
        final StandardProcessScheduler scheduler = createScheduler();
        final ControllerServiceNode serviceNode = flowManager.createControllerService(SimpleTestService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true, null);

        assertSame(ValidationStatus.VALID, serviceNode.performValidation());

        final SimpleTestService ts = (SimpleTestService) serviceNode.getControllerServiceImplementation();
        scheduler.enableControllerService(serviceNode).get();
        assertTrue(serviceNode.isActive());
        final ExecutorService executor = Executors.newCachedThreadPool();

        final AtomicBoolean asyncFailed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            executor.execute(() -> {
                try {
                    scheduler.disableControllerService(serviceNode);
                    assertFalse(serviceNode.isActive());
                } catch (final Exception e) {
                    asyncFailed.set(true);
                }
            });
        }
        // need to sleep a while since we are emulating async invocations on
        // method that is also internally async
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS); // change to seconds.
        assertFalse(asyncFailed.get());
        assertEquals(1, ts.disableInvocationCount());
    }

    @Test
    public void validateDisablingOfTheFailedService() {
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(FailingService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true, null);
        serviceNode.performValidation();

        final Future<?> future = scheduler.enableControllerService(serviceNode);
        try {
            future.get();
        } catch (final Exception ignored) {
            // Expected behavior because the FailingService throws Exception when attempting to enable
        }

        scheduler.shutdown();

        /*
         * Because it was never disabled it will remain active since its
         * enabling is being retried. This may actually be a bug in the
         * scheduler since it probably has to shut down all components (disable
         * services, shut down processors etc) before shutting down itself
         */
        assertTrue(serviceNode.isActive());
        assertSame(ControllerServiceState.ENABLING, serviceNode.getState());
    }

    /**
     * Validates that service that is infinitely blocking in @OnEnabled can
     * still have DISABLE operation initiated. The service itself will be set to
     * DISABLING state at which point UI and all will know that such service can
     * not be transitioned any more into any other state until it finishes
     * enabling (which will never happen in our case thus should be addressed by
     * user). However, regardless of user's mistake NiFi will remain
     * functioning.
     */
    @Test
    public void validateNeverEnablingServiceCanStillBeDisabled() throws Exception {
        final StandardProcessScheduler scheduler = createScheduler();

        final ControllerServiceNode serviceNode = flowManager.createControllerService(LongEnablingService.class.getName(),
                "1", systemBundle.getBundleDetails().getCoordinate(), null, false, true, null);

        final LongEnablingService ts = (LongEnablingService) serviceNode.getControllerServiceImplementation();
        ts.setLimit(Long.MAX_VALUE);

        serviceNode.performValidation();
        scheduler.enableControllerService(serviceNode);

        assertTrue(serviceNode.isActive());
        final long maxTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ts.enableInvocationCount() != 1 && System.nanoTime() <= maxTime) {
            Thread.sleep(1L);
        }
        assertEquals(1, ts.enableInvocationCount());

        scheduler.disableControllerService(serviceNode);
        assertFalse(serviceNode.isActive());
        assertEquals(ControllerServiceState.DISABLING, serviceNode.getState());
        assertEquals(0, ts.disableInvocationCount());
    }

    @Test
    @Timeout(10)
    public void testEnableControllerServiceWithConfigurationContext() throws Exception {
        final ControllerServiceNode serviceNode = flowManager.createControllerService(PropertyTrackingService.class.getName(),
            "property-tracking-service", systemBundle.getBundleDetails().getCoordinate(), null, false, true, null);

        rootGroup.addControllerService(serviceNode);
        serviceNode.setProperties(Map.of(PropertyTrackingService.TRACKING_PROPERTY.getName(), "original-value"));
        serviceNode.performValidation();

        final ConfigurationContext overrideContext = new StandardConfigurationContext(
            serviceNode, Map.of(PropertyTrackingService.TRACKING_PROPERTY.getName(), "overridden-value"), null,
            rootGroup.getParameterContext(), serviceProvider, null);

        final CompletableFuture<Void> future = scheduler.enableControllerService(serviceNode, overrideContext);
        future.get(5, TimeUnit.SECONDS);

        final PropertyTrackingService service = (PropertyTrackingService) serviceNode.getControllerServiceImplementation();
        assertEquals(1, service.enableInvocationCount());
        assertEquals("overridden-value", service.getEnabledPropertyValue());
        assertEquals(ControllerServiceState.ENABLED, serviceNode.getState());
    }

    @Test
    @Timeout(10)
    public void testEnableControllerServiceWithConfigurationContextUsesOverriddenProperties() throws ExecutionException, InterruptedException, TimeoutException {
        final ControllerServiceNode serviceNode = flowManager.createControllerService(PropertyTrackingService.class.getName(),
            "property-tracking-service-2", systemBundle.getBundleDetails().getCoordinate(), null, false, true, null);

        rootGroup.addControllerService(serviceNode);
        serviceNode.performValidation();

        final ConfigurationContext validOverrideContext = new StandardConfigurationContext(
            serviceNode, Map.of(PropertyTrackingService.TRACKING_PROPERTY.getName(), "override-value"), null,
            rootGroup.getParameterContext(), serviceProvider, null);

        final CompletableFuture<Void> future = scheduler.enableControllerService(serviceNode, validOverrideContext);
        future.get(5, TimeUnit.SECONDS);

        final PropertyTrackingService service = (PropertyTrackingService) serviceNode.getControllerServiceImplementation();
        assertEquals(1, service.enableInvocationCount());
        assertEquals("override-value", service.getEnabledPropertyValue());
        assertEquals(ControllerServiceState.ENABLED, serviceNode.getState());
    }

    // Test that if processor throws Exception in @OnScheduled, it keeps getting scheduled
    @Test
    @Timeout(10)
    public void testProcessorThrowsExceptionOnScheduledRetry() throws InterruptedException {
        final FailOnScheduledProcessor proc = new FailOnScheduledProcessor();
        proc.setDesiredFailureCount(3);

        proc.initialize(new StandardProcessorInitializationContext(UUID.randomUUID().toString(), null, null, null, KerberosConfig.NOT_CONFIGURED));
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final VerifiableComponentFactory verifiableComponentFactory = Mockito.mock(VerifiableComponentFactory.class);
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);

        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, UUID.randomUUID().toString(),
            new StandardValidationContextFactory(serviceProvider), scheduler, serviceProvider, reloadComponent,
            verifiableComponentFactory, extensionManager, new SynchronousValidationTrigger());

        procNode.performValidation();
        rootGroup.addProcessor(procNode);

        scheduler.startProcessor(procNode, true);
        while (!proc.isSucceeded()) {
            Thread.sleep(5L);
        }

        assertEquals(3, proc.getOnScheduledInvocationCount());
    }

    // Test that if processor times out in the @OnScheduled but responds to interrupt, it keeps getting scheduled
    @Test
    @Timeout(10)
    public void testProcessorTimeOutRespondsToInterrupt() throws InterruptedException {
        final FailOnScheduledProcessor proc = new FailOnScheduledProcessor();
        proc.setDesiredFailureCount(0);
        proc.setOnScheduledSleepDuration(20, TimeUnit.MINUTES, true, 1);

        proc.initialize(new StandardProcessorInitializationContext(UUID.randomUUID().toString(), null, null, null, KerberosConfig.NOT_CONFIGURED));
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final VerifiableComponentFactory verifiableComponentFactory = Mockito.mock(VerifiableComponentFactory.class);
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);

        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, UUID.randomUUID().toString(),
            new StandardValidationContextFactory(serviceProvider),
            scheduler, serviceProvider, reloadComponent, verifiableComponentFactory, extensionManager, new SynchronousValidationTrigger());

        rootGroup.addProcessor(procNode);

        procNode.performValidation();
        scheduler.startProcessor(procNode, true);
        while (!proc.isSucceeded()) {
            Thread.sleep(5L);
        }

        // The first time that the processor's @OnScheduled method is called, it will sleep for 20 minutes. The scheduler should interrupt
        // that thread and then try again. The second time, the Processor will not sleep because setOnScheduledSleepDuration was called
        // above with iterations = 1
        assertEquals(2, proc.getOnScheduledInvocationCount());
    }

    // Test that if processor times out in the @OnScheduled and does not respond to interrupt, it is not scheduled again
    @Test
    @Timeout(10)
    public void testProcessorTimeOutNoResponseToInterrupt() throws InterruptedException {
        final FailOnScheduledProcessor proc = new FailOnScheduledProcessor();
        proc.setDesiredFailureCount(0);
        proc.setOnScheduledSleepDuration(20, TimeUnit.MINUTES, false, 1);

        proc.initialize(new StandardProcessorInitializationContext(UUID.randomUUID().toString(), null, null, null, KerberosConfig.NOT_CONFIGURED));
        final ReloadComponent reloadComponent = Mockito.mock(ReloadComponent.class);
        final VerifiableComponentFactory verifiableComponentFactory = Mockito.mock(VerifiableComponentFactory.class);
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(proc, systemBundle.getBundleDetails().getCoordinate(), null);

        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, UUID.randomUUID().toString(),
            new StandardValidationContextFactory(serviceProvider), scheduler, serviceProvider, reloadComponent,
            verifiableComponentFactory, extensionManager, new SynchronousValidationTrigger());

        rootGroup.addProcessor(procNode);

        procNode.performValidation();
        scheduler.startProcessor(procNode, true);

        while (proc.getOnScheduledInvocationCount() < 1) {
            Thread.sleep(100L);
        }
        assertEquals(1, proc.getOnScheduledInvocationCount());
        Thread.sleep(100L);
        assertEquals(1, proc.getOnScheduledInvocationCount());

        // Allow test to complete.
        proc.setAllowSleepInterrupt(true);
    }

    public static class FailingService extends AbstractControllerService {

        @OnEnabled
        public void enable(final ConfigurationContext context) {
            throw new RuntimeException("intentional");
        }
    }

    public static class RandomShortDelayEnablingService extends AbstractControllerService {
        private final Random random = new Random();

        @OnEnabled
        public void enable(final ConfigurationContext context) {
            try {
                Thread.sleep(random.nextInt(20));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static class SimpleTestService extends AbstractControllerService {
        private final AtomicInteger enableCounter = new AtomicInteger();
        private final AtomicInteger disableCounter = new AtomicInteger();

        @OnEnabled
        public void enable() {
            this.enableCounter.incrementAndGet();
        }

        @OnDisabled
        public void disable() {
            this.disableCounter.incrementAndGet();
        }

        public int enableInvocationCount() {
            return this.enableCounter.get();
        }

        public int disableInvocationCount() {
            return this.disableCounter.get();
        }
    }

    public static class PropertyTrackingService extends AbstractControllerService {
        public static final PropertyDescriptor TRACKING_PROPERTY = new PropertyDescriptor.Builder()
            .name("Tracking Property")
            .description("A property for tracking what value was used during enabling")
            .required(false)
            .defaultValue("default-value")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

        private volatile String enabledPropertyValue;
        private final AtomicInteger enableCounter = new AtomicInteger();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return List.of(TRACKING_PROPERTY);
        }

        @OnEnabled
        public void enable(final ConfigurationContext context) {
            this.enabledPropertyValue = context.getProperty(TRACKING_PROPERTY).getValue();
            this.enableCounter.incrementAndGet();
        }

        public String getEnabledPropertyValue() {
            return enabledPropertyValue;
        }

        public int enableInvocationCount() {
            return enableCounter.get();
        }
    }

    private StandardProcessScheduler createScheduler() {
        return new StandardProcessScheduler(new FlowEngine(1, "Unit Test", true), Mockito.mock(FlowController.class),
            stateMgrProvider, nifiProperties, new StandardLifecycleStateManager());
    }

    /**
     * Verifies that {@link StandardProcessScheduler#terminateProcessor(ProcessorNode)} causes any
     * {@link ActiveProcessSessionFactory} retained on the {@link LifecycleState} to have its active
     * sessions rolled back, even when no Processor thread is currently in flight.
     *
     * Reproduces the scenario in which a Processor that extends {@code AbstractSessionFactoryProcessor}
     * has cleanly returned from {@code onTrigger} but stashed the Session in a member field; the Session
     * remains unacknowledged on its incoming queue and offload of the node hangs unless terminate causes
     * a rollback through the LifecycleState's retained factories.
     */
    @Test
    @Timeout(30)
    public void testTerminateProcessorRollsBackRetainedSessionWhenNoActiveThreads() throws Exception {
        final TerminationTestHarness harness = createTerminationTestHarness();
        final ProcessorNode procNode = createSimpleProcessorNode(harness);

        final LifecycleState lifecycleState = harness.lifecycleStateManager().getOrRegisterLifecycleState(procNode.getIdentifier(), false, false);

        final ProcessSession retainedSession = Mockito.mock(ProcessSession.class);
        final ProcessSessionFactory delegateFactory = Mockito.mock(ProcessSessionFactory.class);
        when(delegateFactory.createSession()).thenReturn(retainedSession);

        final WeakHashMapProcessSessionFactory retainedFactory = new WeakHashMapProcessSessionFactory(delegateFactory);
        lifecycleState.incrementActiveThreadCount(retainedFactory);
        final ProcessSession sessionWrapper = retainedFactory.createSession();
        lifecycleState.decrementActiveThreadCount();

        assertEquals(0, lifecycleState.getActiveThreadCount());
        assertEquals(ScheduledState.STOPPED, procNode.getScheduledState());

        harness.scheduler().terminateProcessor(procNode);

        Mockito.verify(retainedSession).rollback();
        // Keep the Session wrapper reachable through verification so that the factory's WeakHashMap
        // entry tracking it cannot be cleared by the GC before terminateActiveSessions() iterates it.
        Reference.reachabilityFence(sessionWrapper);
        Reference.reachabilityFence(retainedFactory);

        harness.scheduler().shutdown();
    }

    /**
     * Verifies that the stop background poll loop in {@code StandardProcessorNode.stop()} exits cleanly
     * once {@link LifecycleState#terminate()} has been invoked, instead of rescheduling itself every
     * 100ms forever in the component lifecycle thread pool.
     *
     * Without the fix, {@code LifecycleState.terminate()} resets the active thread count to zero, which
     * the poll loop interprets as "still waiting for threads to drain" (it is comparing against 1, which
     * represents the stop background thread itself), so it keeps rescheduling and leaks one polling task
     * per terminated processor.
     */
    @Test
    @Timeout(30)
    public void testStopBackgroundPollLoopExitsAfterLifecycleStateTerminated() throws Exception {
        final TerminationTestHarness harness = createTerminationTestHarness();
        final ProcessorNode procNode = createSimpleProcessorNode(harness);

        final LifecycleState lifecycleState = harness.lifecycleStateManager().getOrRegisterLifecycleState(procNode.getIdentifier(), false, false);
        lifecycleState.setScheduled(true);
        // Represents an in-flight onTrigger thread that is wedged and will not return on its own.
        lifecycleState.incrementActiveThreadCount(null);

        // The stop sequence requires the Processor to be in RUNNING; reflectively force it there since this
        // test does not run a real scheduling agent.
        forceScheduledState(procNode, ScheduledState.RUNNING);

        harness.scheduler().stopProcessor(procNode, ProcessorStopLifecycleMethods.TRIGGER_ONSTOPPED);

        // Allow the first poll iteration to run and reschedule itself at +100ms.
        Thread.sleep(50L);

        harness.scheduler().terminateProcessor(procNode);

        // Wait long enough for the previously rescheduled poll iteration to fire after termination, then
        // assert that the polling task is not continuing to reschedule itself in the executor queue.
        Thread.sleep(500L);

        final long deadline = System.currentTimeMillis() + 500L;
        while (System.currentTimeMillis() < deadline) {
            final int queueSize = harness.componentLifeCyclePool().getQueue().size();
            assertFalse(queueSize > 0, "Stop polling task continued to reschedule after LifecycleState termination; queue size = " + queueSize);
            Thread.sleep(20L);
        }

        harness.scheduler().shutdown();
    }

    /**
     * Reproduces the production race in which {@code StandardFlowService#offload()} calls
     * {@code stopProcessing()} and then, with no wait whatsoever, filters processors by
     * {@code getScheduledState() == ScheduledState.STOPPED} and calls {@code terminateProcessor()} on every
     * match. {@code ProcessorNode#getScheduledState()} intentionally maps the transient physical
     * {@code STOPPING} state to {@code STOPPED} for backward compatibility, so a Processor whose
     * {@code @OnUnscheduled}/{@code @OnStopped} lifecycle methods are still running satisfies that filter
     * immediately.
     *
     * <p>This variant covers the case where {@code terminateProcessor()} runs <em>after</em> the stop
     * background thread has already reached {@code activateThread()} (i.e. it is already executing
     * {@code @OnUnscheduled} and is therefore tracked in {@code StandardProcessorNode.activeThreads}). In
     * this window, {@code terminate()} finds and interrupts that thread -- mirroring the sibling Kinesis
     * processors in the incident (e.g. {@code dc633c02}) that logged "Terminated 1 threads" and "Failed to
     * shutdown Kinesis Scheduler gracefully" as a direct result of being interrupted mid-{@code onTrigger}/
     * lifecycle-method, and which subsequently did shut down. See
     * {@link #testTerminateProcessorOrphansStopThreadBeforeItIsTrackedAsActive()} for the *other* window --
     * the one actually responsible for the permanently orphaned KCL Scheduler ("Terminated 0 threads").
     */
    @Test
    @Timeout(30)
    public void testTerminateProcessorInterruptsStopThreadAlreadyExecutingOnUnscheduled() throws Exception {
        final TerminationTestHarness harness = createTerminationTestHarness();
        final InfiniteOnUnscheduledProcessor processor = new InfiniteOnUnscheduledProcessor();
        final ProcessorNode procNode = createProcessorNode(harness, processor);

        final LifecycleState lifecycleState = harness.lifecycleStateManager().getOrRegisterLifecycleState(procNode.getIdentifier(), false, false);
        lifecycleState.setScheduled(true);

        // The stop sequence requires the Processor to be in RUNNING; reflectively force it there since this
        // test does not run a real scheduling agent.
        forceScheduledState(procNode, ScheduledState.RUNNING);

        // Mirrors the per-processor call that StandardFlowService#offload()'s stopProcessing() ultimately makes.
        final CompletableFuture<Void> stopFuture = harness.scheduler().stopProcessor(procNode, ProcessorStopLifecycleMethods.TRIGGER_ALL);

        // Deterministically wait until the background stop thread has actually entered the (infinitely
        // blocking) @OnUnscheduled method -- at this point activateThread() has already run, so this
        // thread IS tracked in StandardProcessorNode.activeThreads.
        assertTrue(processor.onUnscheduledEntered.await(5, TimeUnit.SECONDS), "@OnUnscheduled was never invoked");

        // The stop lifecycle has NOT completed: its Future is still pending, and the Processor's real
        // physical state is still STOPPING.
        assertFalse(stopFuture.isDone(), "stop() Future completed even though @OnUnscheduled is still blocked");
        assertEquals(ScheduledState.STOPPING, procNode.getPhysicalScheduledState());

        // Yet getScheduledState() -- exactly what offload()'s filter checks -- already reports STOPPED,
        // due to the intentional STOPPING -> STOPPED backward-compatibility mapping.
        assertEquals(ScheduledState.STOPPED, procNode.getScheduledState(),
            "getScheduledState() should report STOPPED while still physically STOPPING, satisfying offload's filter prematurely");

        // Reproduce offload()'s very next statement: terminate any processor whose getScheduledState() == STOPPED.
        // This must succeed (not throw) even though the stop lifecycle is still in flight.
        harness.scheduler().terminateProcessor(procNode);

        // The framework considers termination "successful" ...
        assertTrue(lifecycleState.isTerminated());

        // ... and, because the background thread was already tracked in activeThreads when terminate() ran,
        // it IS interrupted (unlike the "0 threads" orphan case) -- proving termination reaches into an
        // in-progress @OnUnscheduled invocation rather than waiting for it to finish on its own.
        assertTrue(processor.onUnscheduledInterrupted.await(5, TimeUnit.SECONDS),
            "terminate() should have interrupted the in-progress @OnUnscheduled invocation");
        assertFalse(processor.onUnscheduledReturned, "onUnscheduled() must not have returned normally");

        // Once LifecycleState.terminate() has run, activeThreadCount is permanently pinned to 0, so the stop
        // background thread's "allThreadsComplete" check (activeThreadCount == 1) can never again succeed;
        // it always falls into the isTerminated() branch instead, which completes the stop action WITHOUT
        // ever calling triggerOnStopped(). Poll for a bounded window to confirm @OnStopped is never invoked
        // (there is no positive completion signal to wait on for a "never happens" assertion).
        final long deadline = System.currentTimeMillis() + 500L;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(processor.onStoppedInvoked, "@OnStopped must never be invoked once the processor has been terminated");
            Thread.sleep(20L);
        }

        harness.scheduler().shutdown();
    }

    /**
     * Reproduces the exact production symptom for the ConsumeKinesis "spins" processor ({@code eed1498a}):
     * {@code terminateProcessor()} runs and logs "Terminated 0 threads" / "Successfully terminated with 0
     * active threads" -- yet the background stop thread runs {@code @OnUnscheduled} without ever being
     * interrupted by {@code terminate()}, because it had not yet reached {@code activateThread()} (i.e. it
     * is not yet tracked in {@code StandardProcessorNode.activeThreads}) at the moment {@code terminate()}
     * ran. In the real incident this is exactly what left the underlying KCL Scheduler background thread
     * permanently orphaned: nothing was ever left to signal it to shut down.
     *
     * <p>{@code StandardProcessorNode.stop()} increments {@link LifecycleState#getActiveThreadCount()}
     * synchronously (on the calling thread, before the background task is even submitted) but only calls
     * {@code activateThread()} -- which populates {@code StandardProcessorNode.activeThreads}, the map
     * {@code terminate()} actually iterates -- once the background thread reaches
     * {@code triggerLifecycleMethod()}. This test pins that narrow window open deterministically by making
     * the mocked {@link SchedulingAgent#unschedule} call (invoked by the background thread just before
     * {@code activateThread()}) block until released, so {@code terminateProcessor()} is guaranteed to run
     * while {@code activeThreads} is still empty.
     */
    @Test
    @Timeout(30)
    public void testTerminateProcessorOrphansStopThreadBeforeItIsTrackedAsActive() throws Exception {
        final TerminationTestHarness harness = createTerminationTestHarness();
        final LifecycleTrackingProcessor processor = new LifecycleTrackingProcessor();
        final ProcessorNode procNode = createProcessorNode(harness, processor);

        final LifecycleState lifecycleState = harness.lifecycleStateManager().getOrRegisterLifecycleState(procNode.getIdentifier(), false, false);
        lifecycleState.setScheduled(true);

        forceScheduledState(procNode, ScheduledState.RUNNING);

        final CountDownLatch unscheduleEntered = new CountDownLatch(1);
        final CountDownLatch releaseUnschedule = new CountDownLatch(1);
        final AtomicBoolean stopThreadInterrupted = new AtomicBoolean(false);

        final SchedulingAgent schedulingAgent = harness.scheduler().getSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN);
        Mockito.doAnswer(invocation -> {
            unscheduleEntered.countDown();
            try {
                releaseUnschedule.await();
            } catch (final InterruptedException e) {
                stopThreadInterrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return null;
        }).when(schedulingAgent).unschedule(Mockito.eq((Connectable) procNode), Mockito.any(LifecycleState.class));

        // Mirrors the per-processor call that StandardFlowService#offload()'s stopProcessing() ultimately makes.
        final CompletableFuture<Void> stopFuture = harness.scheduler().stopProcessor(procNode, ProcessorStopLifecycleMethods.TRIGGER_ALL);

        // Wait until the background stop thread is blocked inside schedulingAgent.unschedule() -- i.e. it has
        // NOT yet reached activateThread()/triggerOnUnscheduled(), so StandardProcessorNode.activeThreads is
        // still completely empty, exactly like the moment "Terminated 0 threads" was logged for eed1498a.
        assertTrue(unscheduleEntered.await(5, TimeUnit.SECONDS), "background stop thread never reached schedulingAgent.unschedule()");

        assertFalse(stopFuture.isDone(), "stop() Future completed even though the background thread is still blocked");
        assertEquals(ScheduledState.STOPPING, procNode.getPhysicalScheduledState());

        // Yet getScheduledState() -- exactly what offload()'s filter checks -- already reports STOPPED.
        assertEquals(ScheduledState.STOPPED, procNode.getScheduledState(),
            "getScheduledState() should report STOPPED while still physically STOPPING, satisfying offload's filter prematurely");

        // Reproduce offload()'s very next statement. activeThreads is empty, so this interrupts 0 threads --
        // exactly matching the production log lines "Terminated 0 threads" / "Successfully terminated ...
        // with 0 active threads".
        harness.scheduler().terminateProcessor(procNode);

        assertTrue(lifecycleState.isTerminated(), "framework must consider termination successful");

        // Give the still-blocked background thread a brief moment to notice any interrupt, then prove it was
        // NOT interrupted by terminate() -- it is genuinely orphaned, exactly like the ConsumeKinesis KCL
        // Scheduler background thread that outlived NiFi's "has completely stopped" declaration in production.
        Thread.sleep(200);
        assertFalse(stopThreadInterrupted.get(),
            "the stop-background-thread should be orphaned (left un-interrupted) by terminate(), matching the " +
            "'Terminated 0 threads' pattern observed in production");

        // Cleanup: release the blocked thread so it doesn't leak past the test.
        releaseUnschedule.countDown();

        // The released background thread proceeds to (unconditionally) invoke @OnUnscheduled, then checks
        // allThreadsComplete (activeThreadCount == 1). Because LifecycleState.terminate() already pinned
        // activeThreadCount to 0, that check can never succeed, so it falls into the isTerminated() branch
        // and completes the stop action WITHOUT ever calling triggerOnStopped(). Wait for the one real
        // synchronization point (@OnUnscheduled completing) then poll a bounded window to confirm @OnStopped
        // is never invoked -- exactly matching the eed1498a incident, where nothing was left to signal the
        // orphaned KCL Scheduler to shut down, not even via @OnStopped.
        assertTrue(processor.onUnscheduledCompleted.await(5, TimeUnit.SECONDS), "@OnUnscheduled was never invoked after release");
        final long deadline = System.currentTimeMillis() + 500L;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(processor.onStoppedInvoked, "@OnStopped must never be invoked once the processor has been terminated");
            Thread.sleep(20L);
        }

        harness.scheduler().shutdown();
    }

    private TerminationTestHarness createTerminationTestHarness() {
        final FlowController flowController = Mockito.mock(FlowController.class);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowController.getReloadComponent()).thenReturn(Mockito.mock(ReloadComponent.class));
        when(flowController.getControllerServiceProvider()).thenReturn(serviceProvider);

        final LifecycleStateManager lifecycleStateManager = new StandardLifecycleStateManager();
        final FlowEngine componentLifeCyclePool = new FlowEngine(2, "Termination Test", true);

        final StandardProcessScheduler localScheduler = new StandardProcessScheduler(componentLifeCyclePool, flowController,
            stateMgrProvider, nifiProperties, lifecycleStateManager);
        localScheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, Mockito.mock(SchedulingAgent.class));

        return new TerminationTestHarness(localScheduler, lifecycleStateManager, componentLifeCyclePool);
    }

    private ProcessorNode createSimpleProcessorNode(final TerminationTestHarness harness) {
        return createProcessorNode(harness, new NoOpProcessor());
    }

    private ProcessorNode createProcessorNode(final TerminationTestHarness harness, final Processor processor) {
        final String uuid = UUID.randomUUID().toString();
        processor.initialize(new StandardProcessorInitializationContext(uuid, null, null, null, KerberosConfig.NOT_CONFIGURED));

        final TerminationAwareLogger logger = Mockito.mock(TerminationAwareLogger.class);
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(processor, systemBundle.getBundleDetails().getCoordinate(), logger);
        final ProcessorNode procNode = new StandardProcessorNode(loggableComponent, uuid,
            new StandardValidationContextFactory(serviceProvider), harness.scheduler(), serviceProvider, Mockito.mock(ReloadComponent.class),
            Mockito.mock(VerifiableComponentFactory.class), extensionManager, new SynchronousValidationTrigger());
        rootGroup.addProcessor(procNode);
        return procNode;
    }

    private static void forceScheduledState(final ProcessorNode procNode, final ScheduledState targetState) throws Exception {
        final Field scheduledStateField = ProcessorNode.class.getDeclaredField("scheduledState");
        scheduledStateField.setAccessible(true);
        @SuppressWarnings("unchecked")
        final AtomicReference<ScheduledState> scheduledStateRef = (AtomicReference<ScheduledState>) scheduledStateField.get(procNode);
        scheduledStateRef.set(targetState);
    }

    public static class NoOpProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

    /**
     * A Processor whose {@code @OnUnscheduled} method blocks indefinitely until explicitly released,
     * simulating a component whose stop lifecycle (analogous to a KCL {@code Scheduler.startGracefulShutdown()}
     * call) never gets a chance to complete before the framework moves on. Used to reproduce the race in which
     * {@code StandardFlowService#offload()} calls {@code terminateProcessor()} immediately after
     * {@code stopProcessing()}, without waiting for the stop lifecycle to actually finish.
     *
     * <p>{@code onUnscheduledInterrupted} is counted down only if the blocking {@code await()} call is
     * actually interrupted (i.e. {@code terminate()} reached and interrupted this thread), which is caught
     * here explicitly since NiFi's {@code ReflectionUtils.quietlyInvokeMethodsWithAnnotation} otherwise
     * swallows the exception silently, making interruption unobservable from the outside.
     *
     * <p>{@code onStoppedInvoked} tracks whether {@code @OnStopped} was ever invoked. Once
     * {@code LifecycleState.terminate()} has run, {@code activeThreadCount} is permanently pinned to 0, so
     * {@code StandardProcessorNode.stop()}'s background runnable can never again see
     * {@code activeThreadCount == 1} ("all threads complete") and instead always takes the
     * {@code isTerminated()} branch, which completes the stop action WITHOUT ever calling
     * {@code triggerOnStopped()} -- meaning {@code @OnStopped} is skipped entirely, permanently, not merely
     * delayed.
     */
    public static class InfiniteOnUnscheduledProcessor extends AbstractProcessor {
        final CountDownLatch onUnscheduledEntered = new CountDownLatch(1);
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        final CountDownLatch onUnscheduledInterrupted = new CountDownLatch(1);
        volatile boolean onUnscheduledReturned = false;
        volatile boolean onStoppedInvoked = false;

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }

        @OnUnscheduled
        public void onUnscheduled() {
            onUnscheduledEntered.countDown();
            try {
                // Blocks until either the test releases it, or terminate() interrupts this thread.
                releaseLatch.await();
                onUnscheduledReturned = true;
            } catch (final InterruptedException e) {
                onUnscheduledInterrupted.countDown();
            }
        }

        @OnStopped
        public void onStopped() {
            onStoppedInvoked = true;
        }
    }

    /**
     * A Processor with non-blocking {@code @OnUnscheduled} and {@code @OnStopped} methods, used to verify
     * whether {@code @OnStopped} is invoked once {@code StandardProcessorNode.stop()}'s background runnable
     * resumes after being released. {@code onUnscheduledCompleted} is a reliable, non-racy synchronization
     * point: since nothing else blocks between {@code triggerOnUnscheduled()} returning and the
     * {@code allThreadsComplete}/{@code isTerminated()} decision being made on the very same thread, waiting
     * for it lets tests deterministically settle before asserting on {@code onStoppedInvoked}.
     */
    public static class LifecycleTrackingProcessor extends AbstractProcessor {
        final CountDownLatch onUnscheduledCompleted = new CountDownLatch(1);
        volatile boolean onStoppedInvoked = false;

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }

        @OnUnscheduled
        public void onUnscheduled() {
            onUnscheduledCompleted.countDown();
        }

        @OnStopped
        public void onStopped() {
            onStoppedInvoked = true;
        }
    }

    private record TerminationTestHarness(StandardProcessScheduler scheduler, LifecycleStateManager lifecycleStateManager, FlowEngine componentLifeCyclePool) {
    }
}
