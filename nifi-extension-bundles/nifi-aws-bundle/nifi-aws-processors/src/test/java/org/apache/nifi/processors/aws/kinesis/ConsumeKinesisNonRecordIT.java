/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Consume data from Kinesis without using the Record Reader and Record Writer.
 * We structure the integration tests this way because Kinesis takes about 2 minutes to coordinate consumers
 * and we want to pay this cost only once.
 */
// TODO: Move to abstract class
// TODO: Create record-oriented one
// TODO: Test with multiple partitions
// TODO: Test with multiple shards
// TODO: Test that many records are batched together with Records
// TODO: Test rollback
public class ConsumeKinesisNonRecordIT {
    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:latest");
    private static final String GREETING_HELLO = """
            {"greeting":"hello"}
            """;
    private static final String STREAM_NAME = "test-kinesis-stream";

    private static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
        .withServices(Service.KINESIS, Service.DYNAMODB, Service.CLOUDWATCH);

    private static KinesisClient kinesisClient;
    private static TestRunner runner;


    @BeforeAll
    public static void oneTimeSetup() throws InterruptedException, InitializationException {
        localstack.start();

        kinesisClient = KinesisClient.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.KINESIS))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
            ))
            .region(Region.of(localstack.getRegion()))
            .build();

        // Create a test stream
        kinesisClient.createStream(CreateStreamRequest.builder()
            .streamName(STREAM_NAME)
            .shardCount(1)
            .build());

        // Wait for the stream to become active before proceeding
        waitForStreamActive(STREAM_NAME);

        runner = TestRunners.newTestRunner(ConsumeKinesis.class);

        // Configure AWS credentials and endpoint
        final AwsCredentialsProviderService credentialsService = new AWSCredentialsProviderControllerService();
        runner.addControllerService("credentials", credentialsService);
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, localstack.getAccessKey());
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.SECRET_KEY, localstack.getSecretKey());
        runner.enableControllerService(credentialsService);

        runner.setProperty(ConsumeKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials");
        runner.setProperty(ConsumeKinesis.APPLICATION_NAME, "ConsumeKinesisIT");
        runner.setProperty(ConsumeKinesis.REGION, localstack.getRegion());
        runner.setProperty(ConsumeKinesis.ENDPOINT_OVERRIDE, localstack.getEndpointOverride(LocalStackContainer.Service.KINESIS).toString());
        runner.setProperty(ConsumeKinesis.DYNAMODB_ENDPOINT_OVERRIDE, localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString());
        runner.setProperty(ConsumeKinesis.CLOUDWATCH_ENDPOINT_OVERRIDE, localstack.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH).toString());

        runner.setProperty(ConsumeKinesis.REPORT_CLOUDWATCH_METRICS, "false");
        runner.setProperty(ConsumeKinesis.KINESIS_STREAM_NAME, STREAM_NAME);
        runner.setProperty(ConsumeKinesis.INITIAL_POSITION, ConsumeKinesis.TRIM_HORIZON);

        pushDataToKinesis("p1", GREETING_HELLO);

        // Initialize the processor
        runner.run(1, false, true);
    }

    @AfterAll
    public static void tearDown() {
        runner.run(1, true, false);

        if (kinesisClient != null) {
            kinesisClient.close();
        }
        if (localstack != null) {
            localstack.stop();
        }
    }

    @BeforeEach
    public void clearStream() {
        // Clear the stream by consuming all records
        GetRecordsResponse getRecordsResponse = kinesisClient.getRecords(GetRecordsRequest.builder()
            .shardIterator(kinesisClient.getShardIterator(req -> req.streamName(STREAM_NAME)
                .shardId("shardId-000000000000")
                .shardIteratorType("TRIM_HORIZON")).shardIterator())
            .build());

        while (getRecordsResponse.hasRecords() && !getRecordsResponse.records().isEmpty()) {
            getRecordsResponse = kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(getRecordsResponse.nextShardIterator())
                .build());
        }

        runner.clearTransferState();
    }

    @Test
    void testConsumesRecordPushedBeforeAndAfterStart() throws InterruptedException {
        final String greetingWorld = """
            {"greeting":"world"}
            """;

        while (runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty()) {
            Thread.sleep(10L);
            runner.run(1, false, false);
        }

        // Verify the results
        runner.assertAllFlowFilesTransferred(ConsumeKinesis.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst().assertContentEquals(GREETING_HELLO);

        // Reset state and push another record after starting the processor
        runner.clearTransferState();
        pushDataToKinesis("p1", greetingWorld);

        while (runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty()) {
            Thread.sleep(10L);
            runner.run(1, false, false);
        }

        // Verify the second record
        runner.assertAllFlowFilesTransferred(ConsumeKinesis.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst().assertContentEquals(greetingWorld);
    }

    @Test
    void testRollback() throws InterruptedException {
        final ConsumeKinesis processor = (ConsumeKinesis) runner.getProcessor();

        // First message - during rollback shouldn't be sent anywhere.
        boolean waitingForMessage = true;
        while (waitingForMessage) {
            Thread.sleep(10L);
            final MockProcessSession session = createFailingSession(processor);
            try {
                processor.onTrigger(runner.getProcessContext(), session);
            } catch (final MockProcessSession.TestSessionRollbackException e) {
                // Waiting for a rollback to happen.
                waitingForMessage = false;
                session.assertAllFlowFilesTransferred(ConsumeKinesis.REL_SUCCESS, 0);
            }
        }

        // Second message - shouldn't be send before the first one.
        final String newMessage = """
            {"greeting":"world2"}
            """;

        runner.clearTransferState();
        pushDataToKinesis("p1", newMessage);

        while (runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty()) {
            Thread.sleep(10L);
            runner.run(1, false, false);
        }
        runner.assertAllFlowFilesTransferred(ConsumeKinesis.REL_SUCCESS, 2);
        final List<MockFlowFile> files = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        files.getFirst().assertContentEquals(GREETING_HELLO);
        files.getLast().assertContentEquals(newMessage);
    }

    public static void pushDataToKinesis(final String partitionKey, final String data) {
        final PutRecordRequest request = PutRecordRequest.builder()
            .streamName(STREAM_NAME)
            .partitionKey(partitionKey)
            .data(SdkBytes.fromString(data, StandardCharsets.UTF_8))
            .build();

        kinesisClient.putRecord(request);
    }

    private static void waitForStreamActive(final String streamName) throws InterruptedException {
        final int maxRetries = 50;
        final long retryIntervalMs = 100;

        for (int i = 0; i < maxRetries; i++) {
            final DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(req -> req.streamName(streamName));
            final StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();

            if (streamStatus == StreamStatus.ACTIVE) {
                return;
            }

            Thread.sleep(retryIntervalMs);
        }

        throw new RuntimeException("Timed out waiting for Kinesis stream '" + streamName + "' to become active");
    }

    private static MockProcessSession createFailingSession(final Processor processor) {
        return new MockProcessSession(
                new SharedSessionState(processor, new AtomicLong()),
                processor,
                true,
                new MockStateManager(processor),
                false,
                false,
                true
        );
    }
}


