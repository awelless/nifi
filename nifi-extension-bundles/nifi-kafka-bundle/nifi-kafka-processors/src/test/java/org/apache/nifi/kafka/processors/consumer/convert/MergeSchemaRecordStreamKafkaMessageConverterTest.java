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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.transform.InjectMetadataRecordTransform;
import org.apache.nifi.kafka.processors.consumer.transform.InjectOffsetRecordTransform;
import org.apache.nifi.kafka.processors.consumer.transform.KafkaRecordTransform;
import org.apache.nifi.kafka.processors.consumer.transform.UseValueRecordTransform;
import org.apache.nifi.kafka.processors.consumer.transform.UseWrapperRecordTransform;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MergeSchemaRecordStreamKafkaMessageConverterTest {

    private static final String TOPIC = "topic1";

    private static final RecordSchema SCHEMA_A = new SimpleRecordSchema(List.of(
            new RecordField("fieldA", RecordFieldType.STRING.getDataType())));

    private static final RecordSchema SCHEMA_B = new SimpleRecordSchema(List.of(
            new RecordField("fieldB", RecordFieldType.INT.getDataType())));

    private static final Record RECORD_A = new MapRecord(SCHEMA_A, Map.of("fieldA", "hello"));
    private static final Record RECORD_B = new MapRecord(SCHEMA_B, Map.of("fieldB", 42));

    private final QueuedRecordReaderFactory readerFactory = new QueuedRecordReaderFactory();
    private final PassThroughSchemaRecordWriter writerFactory = new PassThroughSchemaRecordWriter();

    private MockProcessSession session;
    private ComponentLog logger;

    @BeforeEach
    void setUp() throws InitializationException {
        readerFactory.clear();

        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafka.class);
        runner.addControllerService("reader", readerFactory);
        runner.enableControllerService(readerFactory);
        runner.addControllerService("writer", writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty("Record Reader", "reader");

        final Processor processor = runner.getProcessor();
        session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0)), processor);
        logger = runner.getLogger();
    }

    @Test
    void testUseValueMergedSchemasProduceOneFlowFile() {
        final MergeSchemaRecordStreamKafkaMessageConverter converter = mergeConverter(useValueTransform());
        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_B);

        converter.toFlowFiles(session, List.of(
                byteRecord(0, 0, 1000L),
                byteRecord(0, 1, 2000L)).iterator());

        final List<MockFlowFile> successFlowFiles = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, successFlowFiles.size());
        assertEquals(0, session.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE).size());

        final MockFlowFile flowFile = successFlowFiles.getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOPIC, TOPIC);
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_PARTITION, "0");
        flowFile.assertAttributeEquals("record.count", "2");

        final String content = flowFile.getContent();
        assertTrue(content.contains("hello"));
        assertTrue(content.contains("42"));
    }

    @Test
    void testCreateNewFlowFileSplitsOnDifferentSchemas() {
        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory,
                value -> new String(value, StandardCharsets.UTF_8),
                Pattern.compile(".*"),
                KeyEncoding.UTF8, true, new OffsetTracker(), logger, "brokerUri",
                useValueTransform());

        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_B);

        converter.toFlowFiles(session, List.of(
                byteRecord(0, 0, 1000L),
                byteRecord(0, 1, 2000L)).iterator());

        assertEquals(2, session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size());
    }

    @Test
    void testDifferentPartitionsProduceSeparateFlowFiles() {
        final MergeSchemaRecordStreamKafkaMessageConverter converter = mergeConverter(useValueTransform());
        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_A);

        converter.toFlowFiles(session, List.of(
                byteRecord(0, 0, 1000L),
                byteRecord(1, 0, 2000L)).iterator());

        assertEquals(2, session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size());
    }

    @Test
    void testParseFailureRoutedToParseFailureRelationship() {
        final MergeSchemaRecordStreamKafkaMessageConverter converter = mergeConverter(useValueTransform());
        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueueFailure(new MalformedRecordException("parse error"));
        readerFactory.enqueue(RECORD_B);

        final ByteRecord invalidByteRecord = byteRecord(0, 1, 2000L, "invalid-payload");
        converter.toFlowFiles(session, List.of(
                byteRecord(0, 0, 1000L),
                invalidByteRecord,
                byteRecord(0, 2, 3000L)).iterator());

        final List<MockFlowFile> successFlowFiles = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, successFlowFiles.size());
        successFlowFiles.getFirst().assertAttributeEquals("record.count", "2");

        final List<MockFlowFile> parseFailureFlowFiles = session.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
        assertEquals(1, parseFailureFlowFiles.size());
        parseFailureFlowFiles.getFirst().assertContentEquals("invalid-payload");
    }

    @Test
    void testOffsetTrackingWithMergedRecords() {
        final MergeSchemaRecordStreamKafkaMessageConverter converter = mergeConverter(useValueTransform());
        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_B);

        converter.toFlowFiles(session, List.of(
                byteRecord(0, 5, 500L),
                byteRecord(0, 10, 100L)).iterator());

        final MockFlowFile flowFile = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_OFFSET, "5");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, "10");
        flowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TIMESTAMP, "100");
    }

    @Test
    void testMergedSchemaNormalizedThroughWriterFactoryGetSchema() throws InitializationException {
        final TrackingSchemaRecordWriter trackingWriter = new TrackingSchemaRecordWriter();
        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafka.class);
        runner.addControllerService("reader", readerFactory);
        runner.enableControllerService(readerFactory);
        runner.addControllerService("trackingWriter", trackingWriter);
        runner.enableControllerService(trackingWriter);

        final Processor processor = runner.getProcessor();
        final MockProcessSession trackingSession =
                new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0)), processor);
        final ComponentLog trackingLogger = runner.getLogger();

        final MergeSchemaRecordStreamKafkaMessageConverter converter = new MergeSchemaRecordStreamKafkaMessageConverter(
                readerFactory, trackingWriter,
                value -> new String(value, StandardCharsets.UTF_8),
                Pattern.compile(".*"),
                KeyEncoding.UTF8, true, new OffsetTracker(), trackingLogger, "brokerUri",
                new InjectOffsetRecordTransform());

        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_B);

        converter.toFlowFiles(trackingSession, List.of(
                byteRecord(0, 0, 1000L),
                byteRecord(0, 1, 2000L)).iterator());

        assertEquals(1, trackingSession.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).size());
        assertTrue(trackingWriter.lastGetSchemaInput.getField("fieldA").isPresent());
        assertTrue(trackingWriter.lastGetSchemaInput.getField("fieldB").isPresent());
        assertTrue(trackingWriter.lastGetSchemaInput.getField("kafkaOffset").isPresent());
        assertTrue(trackingWriter.schemaPassedToCreateWriter.getField(TrackingSchemaRecordWriter.MARKER).isPresent());
    }

    @Test
    void testInjectOffsetMergedSchemasProduceOneFlowFile() {
        final MergeSchemaRecordStreamKafkaMessageConverter converter = mergeConverter(new InjectOffsetRecordTransform());
        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_B);

        converter.toFlowFiles(session, List.of(
                byteRecord(0, 0, 1000L),
                byteRecord(0, 1, 2000L)).iterator());

        final MockFlowFile flowFile = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertAttributeEquals("record.count", "2");
        final String content = flowFile.getContent();
        assertTrue(content.contains("hello"));
        assertTrue(content.contains("42"));
        assertTrue(content.contains("0"));
        assertTrue(content.contains("1"));
    }

    @Test
    void testUseWrapperMergedSchemasProduceOneFlowFile() {
        final MergeSchemaRecordStreamKafkaMessageConverter converter = mergeConverter(
                new UseWrapperRecordTransform(null,
                        value -> new String(value, StandardCharsets.UTF_8), KeyFormat.STRING, KeyEncoding.UTF8, logger));

        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_B);

        converter.toFlowFiles(session, List.of(
                byteRecord(0, 0, 1000L),
                byteRecord(0, 1, 2000L)).iterator());

        final MockFlowFile flowFile = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertAttributeEquals("record.count", "2");
        final String content = flowFile.getContent();
        assertTrue(content.contains("hello"));
        assertTrue(content.contains("42"));
    }

    @Test
    void testInjectMetadataMergedSchemasProduceOneFlowFile() {
        final MergeSchemaRecordStreamKafkaMessageConverter converter = mergeConverter(
                new InjectMetadataRecordTransform(null,
                        value -> new String(value, StandardCharsets.UTF_8), KeyFormat.STRING, KeyEncoding.UTF8, logger));

        readerFactory.enqueue(RECORD_A);
        readerFactory.enqueue(RECORD_B);

        converter.toFlowFiles(session, List.of(
                byteRecord(0, 0, 1000L),
                byteRecord(0, 1, 2000L)).iterator());

        final MockFlowFile flowFile = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS).getFirst();
        flowFile.assertAttributeEquals("record.count", "2");
        final String content = flowFile.getContent();
        assertTrue(content.contains("hello"));
        assertTrue(content.contains("42"));
    }

    private UseValueRecordTransform useValueTransform() {
        return new UseValueRecordTransform(value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"));
    }

    private MergeSchemaRecordStreamKafkaMessageConverter mergeConverter(final KafkaRecordTransform transform) {
        return new MergeSchemaRecordStreamKafkaMessageConverter(
                readerFactory, writerFactory,
                value -> new String(value, StandardCharsets.UTF_8),
                Pattern.compile(".*"),
                KeyEncoding.UTF8, true, new OffsetTracker(), logger, "brokerUri", transform);
    }

    private ByteRecord byteRecord(final int partition, final long offset, final long timestamp) {
        return byteRecord(partition, offset, timestamp, "ignored");
    }

    private ByteRecord byteRecord(final int partition, final long offset, final long timestamp, final String value) {
        return new ByteRecord(TOPIC, partition, offset, timestamp, List.of(), null, value.getBytes(StandardCharsets.UTF_8), 0L);
    }

    private static final class QueuedRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {
        private final Queue<Object> outcomes = new ArrayDeque<>();

        void clear() {
            outcomes.clear();
        }

        void enqueue(final Record record) {
            outcomes.add(record);
        }

        void enqueueFailure(final Exception failure) {
            outcomes.add(failure);
        }

        @Override
        public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in,
                final long inputLength, final ComponentLog logger) {
            final Object outcome = outcomes.remove();
            return new RecordReader() {
                private boolean consumed;

                @Override
                public Record nextRecord(final boolean coerceTypes, final boolean dropUnknown)
                        throws IOException, MalformedRecordException {
                    if (outcome instanceof IOException ioException) {
                        throw ioException;
                    }
                    if (outcome instanceof MalformedRecordException malformedRecordException) {
                        throw malformedRecordException;
                    }
                    if (outcome instanceof Exception exception) {
                        throw new IOException(exception);
                    }
                    if (consumed) {
                        return null;
                    }
                    consumed = true;
                    return (Record) outcome;
                }

                @Override
                public RecordSchema getSchema() {
                    return ((Record) outcome).getSchema();
                }

                @Override
                public void close() throws IOException {
                    in.close();
                }
            };
        }
    }

    private static final class PassThroughSchemaRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final MockRecordWriter writer = new MockRecordWriter(null, false);

        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out,
                final Map<String, String> variables) throws IOException {
            return writer.createWriter(logger, schema, out, variables);
        }
    }

    private static final class TrackingSchemaRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        static final String MARKER = "writerNormalized";

        private final MockRecordWriter writer = new MockRecordWriter(null, false);
        private RecordSchema lastGetSchemaInput;
        private RecordSchema schemaPassedToCreateWriter;

        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            lastGetSchemaInput = readSchema;
            final List<RecordField> fields = new ArrayList<>(readSchema.getFields());
            fields.add(new RecordField(MARKER, RecordFieldType.STRING.getDataType()));
            return new SimpleRecordSchema(fields);
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out,
                final Map<String, String> variables) throws IOException {
            schemaPassedToCreateWriter = schema;
            return writer.createWriter(logger, schema, out, variables);
        }
    }
}
