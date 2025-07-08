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
package org.apache.nifi.processors.aws.kinesis.stream.consumev2;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.processors.aws.kinesis.stream.consumev2.KinesisRecordProcessor.GeneratedFile;
import org.apache.nifi.processors.aws.kinesis.stream.consumev2.RecordBuffer.ShardBufferId;
import org.apache.nifi.processors.aws.kinesis.stream.consumev2.RecordBuffer.ShardBufferLease;
import org.apache.nifi.processors.aws.v2.RegionUtilV2;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.net.URI;
import java.nio.channels.Channels;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisV2Attributes.APPROXIMATE_ARRIVAL_TIMESTAMP;
import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisV2Attributes.PARTITION_KEY;
import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisV2Attributes.RECORD_COUNT;
import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisV2Attributes.RECORD_ERROR_MESSAGE;
import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisV2Attributes.SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisV2Attributes.SHARD_ID;
import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisV2Attributes.SUB_SEQUENCE_NUMBER;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@Tags({"amazon", "aws", "kinesis", "consume", "stream", "record"})
@CapabilityDescription("Consumes records from Amazon Kinesis Streams. Uses DynamoDB for checkpointing and coordination.")
@WritesAttributes({
        @WritesAttribute(attribute = SHARD_ID,
                description = "Shard ID from which all Kinesis Records in the Flow File were read"),
        @WritesAttribute(attribute = PARTITION_KEY,
                description = "Partition key of the last Kinesis Record in the Flow File"),
        @WritesAttribute(attribute = SEQUENCE_NUMBER,
                description = "A Sequence Number of the last Kinesis Record in the Flow File"),
        @WritesAttribute(attribute = SUB_SEQUENCE_NUMBER,
                description = "A SubSequence Number of the last Kinesis Record in the Flow File. Generated by KPL when aggregating records into a single Kinesis Record"),
        @WritesAttribute(attribute = APPROXIMATE_ARRIVAL_TIMESTAMP,
                description = "Approximate arrival timestamp of the last Kinesis Record in the Flow File"),
        @WritesAttribute(attribute = "mime.type",
                description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer (if configured)"),
        @WritesAttribute(attribute = RECORD_COUNT,
                description = "Number of records written to the FlowFiles by the Record Writer (if configured)"),
        @WritesAttribute(attribute = RECORD_ERROR_MESSAGE,
                description = "This attribute provides on failure the error message encountered by the Record Reader or Record Writer (if configured)")
})
@DefaultSettings(yieldDuration = "10 millis")
public class ConsumeKinesisV2 extends AbstractProcessor {

    public enum InitialPosition implements DescribedValue {
        TRIM_HORIZON("Trim Horizon", "Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard."),
        LATEST("Latest", "Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard.");

        private final String displayName;
        private final String description;

        InitialPosition(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }

        InitialPositionInStream toAwsSdkPosition() {
            return switch (this) {
                case TRIM_HORIZON -> InitialPositionInStream.TRIM_HORIZON;
                case LATEST -> InitialPositionInStream.LATEST;
            };
        }
    }

    static final PropertyDescriptor KINESIS_STREAM_NAME = new PropertyDescriptor.Builder()
            .name("Kinesis Stream Name")
            .description("The name of the Kinesis stream to consume from")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .name("Application Name")
            .description("The name of the Kinesis application. This is used for DynamoDB table naming and worker coordination.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider service")
            .displayName("AWS Credentials Provider Service")
            .description("The Controller Service that is used to obtain AWS credentials provider")
            .required(true)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .required(true)
            .allowableValues(RegionUtilV2.getAvailableRegions())
            .defaultValue(RegionUtilV2.createAllowableValue(Region.US_WEST_2).getValue())
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for parsing the data received from Kinesis")
            .required(false)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer to use for serializing records")
            .required(true)
            .dependsOn(RECORD_READER)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    static final PropertyDescriptor INITIAL_POSITION = new PropertyDescriptor.Builder()
            .name("Initial Position")
            .description("The position in the stream where the processor should start reading")
            .required(true)
            .allowableValues(InitialPosition.class)
            .defaultValue(InitialPosition.TRIM_HORIZON)
            .build();

    static final PropertyDescriptor MAX_BYTES_TO_BUFFER = new PropertyDescriptor.Builder()
            .name("Max Bytes to Buffer")
            .description("The maximum size of Kinesis Records that can be buffered in memory when receiving from Kinesis is faster than processing records. "
                    + "Using a larger value may improve throughput, but will do so at the expense of using additional heap.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("100 MB")
            .build();

    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Kinesis Endpoint Override URL")
            .description("Endpoint URL to use instead of the AWS default including scheme, host, port, and path. " +
                    "The AWS libraries select an endpoint URL based on the AWS region, but this property overrides " +
                    "the selected endpoint URL, allowing use with other S3-compatible endpoints.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor DYNAMODB_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Dynamo DB Endpoint Override")
            .description("An optional endpoint URL to use for DynamoDB. If not specified, the default AWS endpoint for the region will be used.")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();

    static final PropertyDescriptor REPORT_CLOUDWATCH_METRICS = new PropertyDescriptor.Builder()
            .name("Report Metrics to CloudWatch")
            .description("Whether to report Kinesis usage metrics to CloudWatch.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final PropertyDescriptor CLOUDWATCH_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .name("CloudWatch Endpoint Override")
            .description("An optional endpoint URL to use for CloudWatch. If not specified, the default AWS endpoint for the region will be used.")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();

    static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP, ProxySpec.HTTP_AUTH);

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

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are created when records are successfully read from Kinesis and parsed")
            .build();

    static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("FlowFiles that failed to parse using the configured Record Reader")
            .build();

    private static final Set<Relationship> RAW_FILE_PROPERTIES = Set.of(REL_SUCCESS);
    private static final Set<Relationship> RECORD_FILE_PROPERTIES = Set.of(REL_SUCCESS, REL_PARSE_FAILURE);

    private volatile DynamoDbAsyncClient dynamoDbClient;
    private volatile CloudWatchAsyncClient cloudWatchClient;
    private volatile KinesisAsyncClient kinesisClient;
    private volatile Scheduler kinesisScheduler;

    private volatile RecordBuffer recordBuffer;
    private volatile KinesisRecordProcessor recordProcessor;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return recordProcessor == null ? RAW_FILE_PROPERTIES : RECORD_FILE_PROPERTIES;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        if (recordReaderFactory != null) {
            final RecordSetWriterFactory recordWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
            recordProcessor = new KinesisRecordProcessor(recordReaderFactory, recordWriterFactory, getLogger());
        }

        final Region region = Region.of(context.getProperty(REGION).getValue());
        final AwsCredentialsProvider credentialsProvider = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AWSCredentialsProviderService.class).getAwsCredentialsProvider();

        final String kinesisEndpointOverride = context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue();
        final URI kinesisEndpoint = kinesisEndpointOverride == null ? null : URI.create(kinesisEndpointOverride);
        kinesisClient = KinesisAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(kinesisEndpoint)
                .httpClient(createKinesisHttpClientBuilder(context))
                .build();

        final String dynamoDbEndpointOverride = context.getProperty(DYNAMODB_ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue();
        final URI dynamoDbEndpoint = dynamoDbEndpointOverride == null ? null : URI.create(dynamoDbEndpointOverride);
        dynamoDbClient = DynamoDbAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(dynamoDbEndpoint)
                .httpClient(createHttpClientBuilder(context).build())
                .build();

        final String cloudwatchEndpointOverride = context.getProperty(CLOUDWATCH_ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue();
        final URI cloudWatchEndpoint = cloudwatchEndpointOverride == null ? null : URI.create(cloudwatchEndpointOverride);
        cloudWatchClient = CloudWatchAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(cloudWatchEndpoint)
                .httpClient(createHttpClientBuilder(context).build())
                .build();

        final String streamName = context.getProperty(KINESIS_STREAM_NAME).getValue();
        final InitialPosition initialPosition = context.getProperty(INITIAL_POSITION).asAllowableValue(InitialPosition.class);
        final InitialPositionInStreamExtended initialPositionExtended = InitialPositionInStreamExtended.newInitialPosition(initialPosition.toAwsSdkPosition());
        final SingleStreamTracker streamTracker = new SingleStreamTracker(streamName, initialPositionExtended);

        recordBuffer = new RecordBuffer(getLogger(), context.getProperty(MAX_BYTES_TO_BUFFER).asDataSize(DataUnit.B).longValue());
        final ShardRecordProcessorFactory recordProcessorFactory = () -> new ConsumeKinesisRecordProcessor(recordBuffer);

        final String applicationName = context.getProperty(APPLICATION_NAME).getValue();
        final String workerId = UUID.randomUUID().toString();
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

    private static SdkAsyncHttpClient createKinesisHttpClientBuilder(final ProcessContext context) {
        return createHttpClientBuilder(context)
                // As per KinesisClientUtil.adjustKinesisClientBuilder.
                .maxConcurrency(Integer.MAX_VALUE) // todo - is it a good idea?
                .http2Configuration(Http2Configuration.builder()
                        .initialWindowSize(512 * 1024) // 512 KB
                        .healthCheckPingPeriod(Duration.ofMinutes(1))
                        .build())
                .protocol(Protocol.HTTP2)
                .build();
    }

    private static NettyNioAsyncHttpClient.Builder createHttpClientBuilder(final ProcessContext context) {
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

        return builder;
    }

    @OnStopped
    public void onStopped() {
        if (kinesisScheduler != null) {
            kinesisScheduler.shutdown();
        }
        if (kinesisClient != null) {
            kinesisClient.close();
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
        if (cloudWatchClient != null) {
            cloudWatchClient.close();
        }

        recordBuffer = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Optional<ShardBufferLease> maybeLease = recordBuffer.acquireBufferLease();

        maybeLease.ifPresentOrElse(
                lease -> processRecordsFromBuffer(session, lease),
                context::yield
        );
    }

    private void processRecordsFromBuffer(final ProcessSession session, final ShardBufferLease lease) {
        try {
            final List<KinesisClientRecord> records = recordBuffer.consumeRecords(lease);

            if (records.isEmpty()) {
                recordBuffer.returnBufferLease(lease);
                return;
            }

            final String shardId = lease.shardId();

            if (recordProcessor == null) {
                processRecordsAsRaw(session, shardId, records);
            } else {
                processRecordsWithRecordProcessor(recordProcessor, session, shardId, records);
            }

            session.adjustCounter("Records Processed", records.size(), false);

            session.commitAsync(
                    () -> {
                        recordBuffer.commitConsumedRecords(lease);
                        recordBuffer.returnBufferLease(lease);
                    },
                    __ -> {
                        recordBuffer.rollbackConsumedRecords(lease);
                        recordBuffer.returnBufferLease(lease);
                    }
            );
        } catch (final RuntimeException e) {
            getLogger().error("Failed to process records from Kinesis stream: {}", e.getMessage());
            recordBuffer.rollbackConsumedRecords(lease);
            recordBuffer.returnBufferLease(lease);
            throw e;
        }
    }

    private static void processRecordsAsRaw(final ProcessSession session, final String shardId, final List<KinesisClientRecord> records) {
        for (final KinesisClientRecord record : records) {
            final FlowFile flowFile = session.create();
            session.putAllAttributes(flowFile, ConsumeKinesisV2Attributes.forKinesisRecord(shardId, record));

            session.write(flowFile, out -> Channels.newChannel(out).write(record.data()));

            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private static void processRecordsWithRecordProcessor(final KinesisRecordProcessor recordProcessor,
                                                          final ProcessSession session,
                                                          final String shardId,
                                                          final List<KinesisClientRecord> records) {
        final List<GeneratedFile> result = recordProcessor.processRecords(shardId, session, records);

        for (final GeneratedFile file : result) {
            final Relationship relationship = switch (file.status()) {
                case SUCCESS -> REL_SUCCESS;
                case PARSE_FAILURE -> REL_PARSE_FAILURE;
            };
            session.transfer(file.flowFile(), relationship);
        }
    }

    /**
     * An adapter between Kinesis Consumer Library and {@link RecordBuffer}.
     */
    private static class ConsumeKinesisRecordProcessor implements ShardRecordProcessor {

        private final RecordBuffer recordBuffer;
        private volatile ShardBufferId bufferId;

        ConsumeKinesisRecordProcessor(final RecordBuffer recordBuffer) {
            this.recordBuffer = recordBuffer;
        }

        @Override
        public void initialize(final InitializationInput initializationInput) {
            bufferId = recordBuffer.createBuffer(initializationInput.shardId());
        }

        @Override
        public void processRecords(final ProcessRecordsInput processRecordsInput) {
            if (bufferId == null) {
                throw new IllegalStateException("Tried to process records before initialize.");
            }
            recordBuffer.addRecords(bufferId, processRecordsInput.records(), processRecordsInput.checkpointer());
        }

        @Override
        public void leaseLost(final LeaseLostInput leaseLostInput) {
            if (bufferId != null) {
                recordBuffer.consumerLeaseLost(bufferId);
            }
        }

        @Override
        public void shardEnded(final ShardEndedInput shardEndedInput) {
            if (bufferId != null) {
                recordBuffer.finishConsumption(bufferId, shardEndedInput.checkpointer());
            }
        }

        @Override
        public void shutdownRequested(final ShutdownRequestedInput shutdownRequestedInput) {
            if (bufferId != null) {
                recordBuffer.finishConsumption(bufferId, shutdownRequestedInput.checkpointer());
            }
        }
    }
}
