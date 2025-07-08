package org.apache.nifi.processors.aws.kinesis.stream.consumev2;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisStreamV2Attributes.RECORD_COUNT;
import static org.apache.nifi.processors.aws.kinesis.stream.consumev2.ConsumeKinesisStreamV2Attributes.RECORD_ERROR_MESSAGE;

final class KinesisRecordProcessor {

    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final ComponentLog logger;

    KinesisRecordProcessor(final RecordReaderFactory readerFactory, final RecordSetWriterFactory writerFactory, final ComponentLog logger) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.logger = logger;
    }

    // todo garbage

    /**
     * Processes the Kinesis records using the configured RecordReader and RecordWriter.
     * Records that are successfully parsed are written to successful FlowFiles.
     * Records that fail to parse are written to failed FlowFiles as raw data.
     * 
     * @return ProcessingResult containing successful and failed FlowFiles with their associated records
     */
    List<GeneratedFile> processRecords(final String shardId, final ProcessSession session, final List<KinesisClientRecord> records) {
        final List<GeneratedFile> flowFiles = new ArrayList<>();

        RecordSetWriter writer = null;
        OutputStream outputStream = null;
        FlowFile currentFlowFile = null;
        KinesisClientRecord lastRecord = null;

        // todo wait for https://github.com/apache/nifi/pull/10053
        // todo this is horrible
        try {
            for (final KinesisClientRecord kinesisRecord : records) {
                try {
                    final ByteBuffer dataBuffer = kinesisRecord.data();
                    final byte[] data = new byte[dataBuffer.remaining()];
                    dataBuffer.get(data);
                    
                    try (final InputStream in = new ByteArrayInputStream(data);
                         final RecordReader reader = readerFactory.createRecordReader(Collections.emptyMap(), in, data.length, logger)) {

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            // Create new FlowFile if needed
                            if (currentFlowFile == null) {
                                currentFlowFile = session.create();

                                 // Initialize writer
                                 final RecordSchema writeSchema = writerFactory.getSchema(Collections.emptyMap(), record.getSchema());
                                 outputStream = session.write(currentFlowFile);
                                 writer = writerFactory.createWriter(logger, writeSchema, outputStream, currentFlowFile);
                                 writer.beginRecordSet();
                            }
                            
                            // Write a record.
                            writer.write(record);
                            lastRecord = kinesisRecord;
                        }
                    }
                } catch (final MalformedRecordException | IOException | SchemaNotFoundException e) {
                    // Handle parse failure - create raw FlowFile for failed record
                    flowFiles.add(handleParseFailure(session, shardId, kinesisRecord, e));
                }
            }
            
            // Complete the current FlowFile if it exists.
            if (currentFlowFile != null && writer != null) {
                try {
                    final WriteResult writeResult = writer.finishRecordSet();
                    session.putAllAttributes(currentFlowFile, ConsumeKinesisStreamV2Attributes.forKinesisRecord(shardId, lastRecord));

                    session.putAttribute(currentFlowFile, CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    session.putAttribute(currentFlowFile, RECORD_COUNT, String.valueOf(writeResult.getRecordCount()));
                    session.putAllAttributes(currentFlowFile, writeResult.getAttributes());

                    flowFiles.add(new GeneratedFile(currentFlowFile, WriteStatus.SUCCESS));
                } finally {
                    try {
                        writer.close();
                    } catch (final IOException ignored) {
                        // Ignore close errors
                    }
                    try {
                        outputStream.close();
                    } catch (final IOException ignored) {
                        // Ignore close errors
                    }
                }
            }
            
        } catch (final Exception e) {
            // Clean up on error.
            if (writer != null) {
                try {
                    writer.close();
                } catch (final IOException ignored) {
                    // Ignore close errors.
                }
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (final IOException ignored) {
                    // Ignore close errors.
                }
            }
            if (currentFlowFile != null) {
                session.remove(currentFlowFile);
            }
            throw new RuntimeException("Failed to process records", e);
        }

        return flowFiles;
    }

    private GeneratedFile handleParseFailure(
            final ProcessSession session,
            final String shardId,
            final KinesisClientRecord record,
            final Exception e) {
        final FlowFile flowFile = session.create();
        session.putAllAttributes(flowFile, ConsumeKinesisStreamV2Attributes.forKinesisRecord(shardId, record));
        session.putAttribute(flowFile, RECORD_ERROR_MESSAGE, e.getLocalizedMessage());

        // Write raw record data
        session.write(flowFile, out -> Channels.newChannel(out).write(record.data()));

        return new GeneratedFile(
            flowFile,
            WriteStatus.PARSE_FAILURE
        );
    }

    record GeneratedFile(
            FlowFile flowFile,
            WriteStatus status) {
    }

    enum WriteStatus {
        SUCCESS,
        PARSE_FAILURE,
    }
}
