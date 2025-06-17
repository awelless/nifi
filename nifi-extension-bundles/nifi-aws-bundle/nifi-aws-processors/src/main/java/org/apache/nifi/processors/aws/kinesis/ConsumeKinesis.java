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

package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.processors.aws.v2.RegionUtilV2;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.ByteBufferInputStream;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@Tags({"amazon", "aws", "kinesis", "consume", "stream", "record"})
@CapabilityDescription("Consumes records from Amazon Kinesis Streams. Uses DynamoDB for checkpointing and coordination.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.kinesis.partition.key", description = "The partition key of the Kinesis record"),
    @WritesAttribute(attribute = "aws.kinesis.sequence.number", description = "The sequence number of the Kinesis record"),
    @WritesAttribute(attribute = "aws.kinesis.shard.id", description = "The shard ID of the Kinesis record")
})
@DefaultSettings(yieldDuration = "10 millis")
public class ConsumeKinesis extends AbstractProcessor {

    // TODO: MAKE VERIFIABLE

    static final AllowableValue TRIM_HORIZON = new AllowableValue("Trim Horizon", "Trim Horizon",
        "Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard."
    );

    static final AllowableValue LATEST = new AllowableValue("Latest", "Latest",
        "Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard."
    );

    // Properties
    public static final PropertyDescriptor KINESIS_STREAM_NAME = new PropertyDescriptor.Builder()
        .name("Kinesis Stream Name")
        .description("The name of the Kinesis stream to consume from")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
        .name("Application Name")
        .description("The name of the Kinesis application. This is used for DynamoDB table naming and worker coordination.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
        .name("AWS Credentials Provider service")
        .displayName("AWS Credentials Provider Service")
        .description("The Controller Service that is used to obtain AWS credentials provider")
        .required(true)
        .identifiesControllerService(AWSCredentialsProviderService.class)
        .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
        .name("Region")
        .required(true)
        .allowableValues(RegionUtilV2.getAvailableRegions())
        .defaultValue(RegionUtilV2.createAllowableValue(Region.US_WEST_2).getValue())
        .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("Record Reader")
        .description("The Record Reader to use for parsing the data received from Kinesis")
        .required(false)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("Record Writer")
        .description("The Record Writer to use for serializing records")
        .required(true)
        .dependsOn(RECORD_READER)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();

    public static final PropertyDescriptor INITIAL_POSITION = new PropertyDescriptor.Builder()
        .name("Initial Position")
        .description("The position in the stream where the processor should start reading")
        .required(true)
        .allowableValues(TRIM_HORIZON, LATEST)
        .defaultValue(TRIM_HORIZON.getValue())
        .build();

    public static final PropertyDescriptor MAX_BYTES_TO_BUFFER = new PropertyDescriptor.Builder()
        .name("Max Bytes to Buffer")
        .description("The maximum size of Kinesis Records that can be buffered in memory when receiving from Kinesis is faster than processing records. "
            + "Using a larger value may improve throughput, but will do so at the expense of using additional heap.")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("100 MB")
        .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
        .name("Communications Timeout")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("30 secs")
        .build();

    public static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
        .name("Kinesis Endpoint Override URL")
        .description("Endpoint URL to use instead of the AWS default including scheme, host, port, and path. " +
                     "The AWS libraries select an endpoint URL based on the AWS region, but this property overrides " +
                     "the selected endpoint URL, allowing use with other S3-compatible endpoints.")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .addValidator(StandardValidators.URL_VALIDATOR)
        .build();

    public static final PropertyDescriptor DYNAMODB_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
        .name("Dynamo DB Endpoint Override")
        .description("An optional endpoint URL to use for DynamoDB. If not specified, the default AWS endpoint for the region will be used.")
        .addValidator(StandardValidators.URL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .build();

    public static final PropertyDescriptor REPORT_CLOUDWATCH_METRICS = new PropertyDescriptor.Builder()
        .name("Report Metrics to CloudWatch")
        .description("Whether to report Kinesis usage metrics to CloudWatch.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    public static final PropertyDescriptor CLOUDWATCH_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
        .name("CloudWatch Endpoint Override")
        .description("An optional endpoint URL to use for CloudWatch. If not specified, the default AWS endpoint for the region will be used.")
        .addValidator(StandardValidators.URL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .build();

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP, ProxySpec.HTTP_AUTH);

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that are created when records are successfully read from Kinesis and parsed")
        .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
        .name("parse.failure")
        .description("FlowFiles that failed to parse using the configured Record Reader")
        .build();

    private static final List<PropertyDescriptor> propertyDescriptors = List.of(
        KINESIS_STREAM_NAME,
        APPLICATION_NAME,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        REGION,
        RECORD_READER,
        RECORD_WRITER,
        INITIAL_POSITION,
        MAX_BYTES_TO_BUFFER,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        DYNAMODB_ENDPOINT_OVERRIDE,
        CLOUDWATCH_ENDPOINT_OVERRIDE,
        PROXY_CONFIGURATION_SERVICE,
        REPORT_CLOUDWATCH_METRICS
    );

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_PARSE_FAILURE);

    private volatile RecordBuffer recordBuffer;
    private volatile DynamoDbAsyncClient dynamoDbClient;
    private volatile CloudWatchAsyncClient cloudWatchClient;
    private volatile KinesisAsyncClient kinesisClient;
    private volatile Scheduler kinesisScheduler;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        final Region region = Region.of(context.getProperty(REGION).getValue().toUpperCase());
        final String streamName = context.getProperty(KINESIS_STREAM_NAME).getValue();
        final String applicationName = context.getProperty(APPLICATION_NAME).getValue();
        final String workerId = UUID.randomUUID().toString();
        final AwsCredentialsProvider credentialsProvider = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
            .asControllerService(AWSCredentialsProviderService.class).getAwsCredentialsProvider();

        final String dynamoDbEndpointOverride = context.getProperty(DYNAMODB_ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue();
        final URI dynamoDbEndpoint = dynamoDbEndpointOverride == null ? null : URI.create(dynamoDbEndpointOverride);

        final String kinesisEndpointOverride = context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue();
        final URI kinesisEndpoint = kinesisEndpointOverride == null ? null : URI.create(kinesisEndpointOverride);

        final String cloudwatchEndpointOverride = context.getProperty(CLOUDWATCH_ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue();
        final URI cloudWatchEndpoint = cloudwatchEndpointOverride == null ? null : URI.create(cloudwatchEndpointOverride);

        final SdkAsyncHttpClient httpClient = createHttpClient(context);

        dynamoDbClient = DynamoDbAsyncClient.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(dynamoDbEndpoint)
            .httpClient(httpClient)
            .build();
        kinesisClient = KinesisAsyncClient.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(kinesisEndpoint)
            .httpClient(httpClient)
            .build();
        cloudWatchClient = CloudWatchAsyncClient.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(cloudWatchEndpoint)
            .httpClient(httpClient)
            .build();

        recordBuffer = new RecordBuffer(context.getProperty(MAX_BYTES_TO_BUFFER).asDataSize(DataUnit.B).longValue());
        final ConsumeKinesisRecordProcessorFactory recordProcessorFactory = new ConsumeKinesisRecordProcessorFactory(recordBuffer, getLogger());
        final InitialPositionInStream initialPosition = context.getProperty(INITIAL_POSITION).getValue().equalsIgnoreCase(TRIM_HORIZON.getValue())
            ? InitialPositionInStream.TRIM_HORIZON : InitialPositionInStream.LATEST;
        final InitialPositionInStreamExtended initialPositionExtended = InitialPositionInStreamExtended.newInitialPosition(initialPosition);
        final SingleStreamTracker streamTracker = new SingleStreamTracker(streamName, initialPositionExtended);

        final ConfigsBuilder configsBuilder = new ConfigsBuilder(streamTracker, applicationName, kinesisClient, dynamoDbClient, cloudWatchClient, workerId, recordProcessorFactory);

        if (!context.getProperty(REPORT_CLOUDWATCH_METRICS).asBoolean()) {
            configsBuilder.metricsConfig().metricsFactory(null);
        }

        // Scheduler is necessary for automatic coordination of Shard IDs among workers
        kinesisScheduler = new Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            configsBuilder.leaseManagementConfig(),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig(),
            configsBuilder.retrievalConfig()
        );

        final Thread schedulerThread = new Thread(kinesisScheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    private SdkAsyncHttpClient createHttpClient(final ProcessContext context) {
        final Duration timeout = context.getProperty(TIMEOUT).asDuration();

        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder()
            .connectionTimeout(timeout)
            .readTimeout(timeout);

        final ProxyConfigurationService proxyConfigService = context.getProperty(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
        if (proxyConfigService != null) {
            final ProxyConfiguration proxyConfig = proxyConfigService.getConfiguration();

            final software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder proxyConfigBuilder = software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
                .host(proxyConfig.getProxyServerHost())
                .port(proxyConfig.getProxyServerPort());

            if (proxyConfig.hasCredential()) {
                proxyConfigBuilder.username(proxyConfig.getProxyUserName());
                proxyConfigBuilder.password(proxyConfig.getProxyUserPassword());
            }

            builder.proxyConfiguration(proxyConfigBuilder.build());
        }

        return builder.build();
    }


    @OnStopped
    public void onStopped() {
        if (recordBuffer != null) {
            recordBuffer.shutdown();
        }
        if (kinesisScheduler != null) {
            kinesisScheduler.shutdown();
        }
        if (kinesisClient != null) {
            kinesisClient.close();
        }
        if (cloudWatchClient != null) {
            cloudWatchClient.close();
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String shardId = recordBuffer.pollActiveShardId();
        if (shardId == null) {
            context.yield();
            return;
        }

        final ShardBuffer buffer = recordBuffer.getShardBuffer(shardId);
        final List<KinesisRecord> records = buffer.poll();
        if (records.isEmpty()) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        if (readerFactory == null) {
            final List<FlowFile> flowFiles = writeAsIndividualFlowFiles(session, records, shardId);
            session.transfer(flowFiles, REL_SUCCESS);
        } else {
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
            final List<FlowFile> flowFiles;
            try {
                flowFiles = writeAsRecords(session, readerFactory, writerFactory, records, shardId);
            } catch (final IOException e) {
                getLogger().error("Failed to consume Records from Kinesis", e);
                buffer.rollback(records);
                context.yield();
                return;
            }

            session.transfer(flowFiles, REL_SUCCESS);
        }

        session.commitAsync(
            () -> buffer.acknowledge(records),
            failureCause -> buffer.rollback(records));
    }

    private List<FlowFile> writeAsIndividualFlowFiles(final ProcessSession session, final List<KinesisRecord> kinesisRecords, final String shardId) {
        final List<FlowFile> flowFiles = new ArrayList<>();
        for (final KinesisRecord kinesisRecord : kinesisRecords) {
            if (!kinesisRecord.isValid()) {
                continue;
            }

            final FlowFile flowFile = session.create();
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
            attributes.put("aws.kinesis.partition.key", kinesisRecord.getClientRecord().partitionKey());
            attributes.put("aws.kinesis.sequenceNumber", kinesisRecord.getClientRecord().sequenceNumber());
            attributes.put("aws.kinesis.subsequenceNumber", Long.toString(kinesisRecord.getClientRecord().subSequenceNumber()));
            attributes.put("aws.kinesis.shard.id", shardId);

            session.write(flowFile, out -> {
                final ByteBuffer data = kinesisRecord.getClientRecord().data();

                final byte[] buffer = new byte[Math.min(8192, data.remaining())];
                while (data.hasRemaining()) {
                    final int length = Math.min(buffer.length, data.remaining());
                    data.get(buffer, 0, length);
                    out.write(buffer, 0, length);
                }
            });

            final FlowFile updatedFlowFile = session.putAllAttributes(flowFile, attributes);
            flowFiles.add(updatedFlowFile);
        }

        return flowFiles;
    }


    private List<FlowFile> writeAsRecords(final ProcessSession session, final RecordReaderFactory readerFactory,
                                    final RecordSetWriterFactory writerFactory, final List<KinesisRecord> kinesisRecords, final String shardId) throws IOException {

        final List<FlowFile> flowFiles = new ArrayList<>();

        FlowFile currentFlowFile = session.create();
        Map<String, String> currentAttributes = new HashMap<>();
        RecordSetWriter currentWriter = null;
        RecordSchema currentSchema = null;
        KinesisRecord lastRecordWritten = null;
        // TODO: Keep track of all Record Writers created and if any Exception occurs, close them all

        for (final KinesisRecord kinesisRecord : kinesisRecords) {
            if (!kinesisRecord.isValid()) {
                continue;
            }

            final ByteBuffer data = kinesisRecord.getClientRecord().data();
            try (final InputStream in = new ByteBufferInputStream(data);
                 final RecordReader reader = readerFactory.createRecordReader(Collections.emptyMap(), in, data.capacity(), getLogger())) {

                final RecordSchema schema = reader.getSchema();
                if (currentWriter == null || !Objects.equals(currentSchema, schema)) {
                    if (currentWriter != null) {
                        finalizeFlowFile(currentWriter, currentAttributes, lastRecordWritten);
                        session.putAllAttributes(currentFlowFile, currentAttributes);
                        flowFiles.add(currentFlowFile);
                    }

                    currentFlowFile = session.create();
                    final OutputStream out = session.write(currentFlowFile);
                    currentWriter = writerFactory.createWriter(getLogger(), schema, out, currentFlowFile);
                    currentWriter.beginRecordSet();

                    currentSchema = schema;
                    currentAttributes = new HashMap<>();
                    currentAttributes.put(CoreAttributes.MIME_TYPE.key(), currentWriter.getMimeType());
                    currentAttributes.put("aws.kinesis.partition.key", kinesisRecord.getClientRecord().partitionKey());
                    currentAttributes.put("aws.kinesis.sequenceNumber", kinesisRecord.getClientRecord().sequenceNumber());
                    currentAttributes.put("aws.kinesis.subsequenceNumber", Long.toString(kinesisRecord.getClientRecord().subSequenceNumber()));
                    currentAttributes.put("aws.kinesis.shard.id", shardId);
                }

                final Record record = reader.nextRecord();
                currentWriter.write(record);
                lastRecordWritten = kinesisRecord;
            } catch (final SchemaNotFoundException | MalformedRecordException e) {
                final List<FlowFile> parseFailureFlowFiles = writeAsIndividualFlowFiles(session, List.of(kinesisRecord), shardId);
                session.transfer(parseFailureFlowFiles, REL_PARSE_FAILURE);
            }
        }

        if (currentWriter != null) {
            finalizeFlowFile(currentWriter, currentAttributes, lastRecordWritten);
            session.putAllAttributes(currentFlowFile, currentAttributes);
            flowFiles.add(currentFlowFile);
        }

        return flowFiles;
    }

    private void finalizeFlowFile(final RecordSetWriter writer, final Map<String, String> currentAttributes, final KinesisRecord lastRecord) throws IOException {
        final WriteResult writeResult = writer.finishRecordSet();
        writer.close();

        currentAttributes.putAll(writeResult.getAttributes());
        currentAttributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
        currentAttributes.put("record.count", Integer.toString(writeResult.getRecordCount()));

        if (lastRecord != null) {
            currentAttributes.put("aws.kinesis.max.sequenceNumber", lastRecord.getClientRecord().sequenceNumber());
            currentAttributes.put("aws.kinesis.max.subsequenceNumber", Long.toString(lastRecord.getClientRecord().subSequenceNumber()));
        }
    }

    private static class ConsumeKinesisRecordProcessorFactory implements ShardRecordProcessorFactory {
        private final RecordBuffer recordBuffer;
        private final ComponentLog logger;

        public ConsumeKinesisRecordProcessorFactory(final RecordBuffer recordBuffer, final ComponentLog logger) {
            this.recordBuffer = recordBuffer;
            this.logger = logger;
        }

        @Override
        public ShardRecordProcessor shardRecordProcessor() {
            return new ConsumeKinesisRecordProcessor(recordBuffer, logger);
        }

        @Override
        public ShardRecordProcessor shardRecordProcessor(final StreamIdentifier streamIdentifier) {
            return new ConsumeKinesisRecordProcessor(recordBuffer, logger);
        }
    }

    static class RecordBuffer {
        private final long maxBytes;
        private final Map<String, ShardBuffer> shardBuffers = new ConcurrentHashMap<>();
        private final BlockingQueue<String> activeShardIds = new LinkedBlockingQueue<>();

        private final Object bufferSizeMutex = new Object();
        private long currentSize = 0L;

        private volatile boolean shutdown = false;

        public RecordBuffer(final long maxBytes) {
            this.maxBytes = maxBytes;
        }

        public void shutdown() {
            shutdown = true;
            shardBuffers.values().forEach(ShardBuffer::clear);
            shardBuffers.clear();
        }

        ShardBuffer getShardBuffer(final String shardId) {
            return shardBuffers.get(shardId);
        }

        String pollActiveShardId() {
            return activeShardIds.poll();
        }

        void onRecordsAcknowledged(final String shardId, final long dataSize, final boolean shardEmpty) {
            synchronized (bufferSizeMutex) {
                currentSize -= dataSize;
                bufferSizeMutex.notifyAll();
            }

            if (shardEmpty) {
                synchronized (activeShardIds) {
                    // shard was empty at the time that this method was called. However, that may have changed
                    // in a different thread, so we need to check again within the synchronized block
                    if (shardBuffers.get(shardId).isEmpty()) {
                        activeShardIds.remove(shardId);
                    }
                }
            }
        }

        void offer(final String shardId, final List<KinesisRecord> kinesisRecords) {
            if (shutdown) {
                throw new IllegalStateException("Consume Kinesis record processor has been shutdown");
            }

            final long dataSize = kinesisRecords.stream()
                .mapToLong(kinesisRecord -> kinesisRecord.getClientRecord().data().capacity())
                .sum();

            // Wait until there is space in the buffer
            synchronized (bufferSizeMutex) {
                while (currentSize >= maxBytes) {
                    if (shutdown) {
                        throw new IllegalStateException("Consume Kinesis record processor has been shutdown");
                    }

                    try {
                        bufferSizeMutex.wait(1_000L);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Consume Kinesis record processor has been interrupted");
                    }
                }

                currentSize += dataSize;
            }

            final ShardBuffer buffer = shardBuffers.computeIfAbsent(shardId, id -> new ShardBuffer(shardId, this));
            final boolean becameActive = buffer.offer(kinesisRecords, dataSize);
            if (becameActive) {
                // We need to synchronize on activeShardIds because we conditionally remove from it,
                // and we need that conditional removal to be atomic
                synchronized (activeShardIds) {
                    activeShardIds.add(shardId);
                }
            }
        }
    }

    private static class ShardBuffer {
        private final BlockingQueue<KinesisRecord> records = new LinkedBlockingQueue<>();
        private final AtomicLong dataSize = new AtomicLong(0L);

        private final String shardId;
        private final RecordBuffer recordBuffer;

        public ShardBuffer(final String shardId, final RecordBuffer recordBuffer) {
            this.shardId = shardId;
            this.recordBuffer = recordBuffer;
        }

        public List<KinesisRecord> poll() {
            final List<KinesisRecord> polled = new ArrayList<>();
            records.drainTo(polled);
            return polled;
        }

        public void acknowledge(final List<KinesisRecord> records) {
            final long dataSize = records.stream()
                .mapToLong(kinesisRecord -> kinesisRecord.getClientRecord().data().capacity())
                .sum();

            records.forEach(KinesisRecord::acknowledge);

            this.dataSize.addAndGet(-dataSize);
            recordBuffer.onRecordsAcknowledged(shardId, dataSize, records.isEmpty());
        }

        public synchronized void rollback(final List<KinesisRecord> records) {
            records.forEach(KinesisRecord::fail);
            final List<KinesisRecord> currentlyAvailable = new ArrayList<>();
            this.records.drainTo(currentlyAvailable);
            currentlyAvailable.forEach(KinesisRecord::fail);
        }

        /**
         * Adds the given records to the buffer and returns true if the buffer was empty before adding.
         * @param kinesisRecords the records to add to the buffer
         * @param dataSize the size of the data in bytes for the records being added
         * @return true if the buffer was empty before adding the records, false otherwise
         */
        public synchronized boolean offer(final List<KinesisRecord> kinesisRecords, final long dataSize) {
            final boolean empty = records.isEmpty();

            kinesisRecords.forEach(records::offer);
            this.dataSize.addAndGet(dataSize);

            return empty;
        }

        public boolean isEmpty() {
            return records.isEmpty();
        }

        public void clear() {
            records.clear();
        }
    }

    static class KinesisRecord {
        private final BooleanSupplier validationFunction;
        private final KinesisClientRecord clientRecord;
        private final CountDownLatch processedLatch;
        private volatile boolean acknowledged = false;
        private volatile boolean failed = false;

        public KinesisRecord(final KinesisClientRecord clientRecord, final BooleanSupplier validationFunction, final CountDownLatch processedLatch) {
            this.clientRecord = clientRecord;
            this.validationFunction = validationFunction;
            this.processedLatch = processedLatch;
        }

        public boolean isValid() {
            return validationFunction.getAsBoolean();
        }

        public KinesisClientRecord getClientRecord() {
            return clientRecord;
        }

        public void acknowledge() {
            if (acknowledged || failed) {
                return;
            }

            acknowledged = true;
            processedLatch.countDown();
        }

        public void fail() {
            if (acknowledged || failed) {
                return;
            }

            failed = true;
            processedLatch.countDown();
        }

        public boolean isFailed() {
            return failed;
        }
    }

    private static class ConsumeKinesisRecordProcessor implements ShardRecordProcessor {
        private final RecordBuffer recordBuffer;
        private final ComponentLog logger;

        private volatile InitializationInput initializationInput;
        private volatile boolean valid = true;

        public ConsumeKinesisRecordProcessor(final RecordBuffer recordBuffer, final ComponentLog logger) {
            this.recordBuffer = recordBuffer;
            this.logger = logger;
        }

        private boolean isValid() {
            return valid;
        }

        @Override
        public void initialize(final InitializationInput initializationInput) {
            this.initializationInput = initializationInput;
        }

        @Override
        public void processRecords(final ProcessRecordsInput processRecordsInput) {
            final CountDownLatch countDownLatch = new CountDownLatch(processRecordsInput.records().size());
            final List<KinesisRecord> kinesisRecords = processRecordsInput.records().stream()
                .map(record -> new KinesisRecord(record, this::isValid, countDownLatch))
                .toList();

            recordBuffer.offer(initializationInput.shardId(), kinesisRecords);

            // TODO: Do not wait indefinitely but instead periodically check if the processor has been stopped and if so return.
            try {
                countDownLatch.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (!isValid()) {
                logger.info("{} no longer valid, skipping checkpoint", this);
                return;
            }

            final boolean success = kinesisRecords.stream().noneMatch(KinesisRecord::isFailed);
            if (success) {
                try {
                    processRecordsInput.checkpointer().checkpoint();
                } catch (final InvalidStateException e) {
                    logger.error("Failed to checkpoint {} records for shard {} due to invalid state", processRecordsInput.records().size(), initializationInput.shardId(), e);
                } catch (final ShutdownException e) {
                    logger.warn("Failed to checkpoint {} records for shard {} due to Shutdown", processRecordsInput.records().size(), initializationInput.shardId(), e);
                }
            }
        }

        @Override
        public void leaseLost(final LeaseLostInput leaseLostInput) {
            if (!valid) {
                return;
            }

            logger.info("{} no longer valid because lease lost", this);
            valid = false;
        }

        @Override
        public void shardEnded(final ShardEndedInput shardEndedInput) {
            // TODO: How do we handle this?
        }

        @Override
        public void shutdownRequested(final ShutdownRequestedInput shutdownRequestedInput) {
            logger.info("{} no longer valid due to shutdown request", this);
            valid = false;
        }
    }
}
