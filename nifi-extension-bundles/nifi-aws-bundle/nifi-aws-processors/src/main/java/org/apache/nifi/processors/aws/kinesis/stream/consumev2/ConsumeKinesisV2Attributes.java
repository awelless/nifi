package org.apache.nifi.processors.aws.kinesis.stream.consumev2;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.Map;

final class ConsumeKinesisV2Attributes {
    private static final String PREFIX = "aws.kinesis.";

    // AWS Kinesis attributes.
    static final String SHARD_ID = PREFIX + "shard.id";
    static final String SEQUENCE_NUMBER = PREFIX + "sequence.number";
    static final String SUB_SEQUENCE_NUMBER = PREFIX + "subsequence.number";

    static final String PARTITION_KEY = PREFIX + "partition.key";
    static final String APPROXIMATE_ARRIVAL_TIMESTAMP = PREFIX + "approximate.arrival.timestamp";

    // Record attributes.
    static final String RECORD_COUNT = "record.count";
    static final String RECORD_ERROR_MESSAGE = "record.error.message";

    static Map<String, String> forKinesisRecord(
            final String shardId,
            final KinesisClientRecord record) {
        return Map.of(
                SHARD_ID, shardId,
                SEQUENCE_NUMBER, record.sequenceNumber(),
                SUB_SEQUENCE_NUMBER, String.valueOf(record.subSequenceNumber()),
                PARTITION_KEY, record.partitionKey(),
                APPROXIMATE_ARRIVAL_TIMESTAMP, record.approximateArrivalTimestamp().toString()
        );
    }

    private ConsumeKinesisV2Attributes() {
    }
}
