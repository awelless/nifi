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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.common.HeaderValueConverter;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.transform.KafkaRecordTransform;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Shared reader loop, parse-failure handling, and composed {@link KafkaRecordTransform} for record-stream converters.
 * Subclasses implement grouping and FlowFile write timing only.
 */
public abstract class AbstractRecordStreamKafkaMessageConverter implements KafkaMessageConverter {

    protected static final RecordSchema EMPTY_SCHEMA = new SimpleRecordSchema(List.of());

    protected final RecordReaderFactory readerFactory;
    protected final RecordSetWriterFactory writerFactory;
    protected final HeaderValueConverter headerValueConverter;
    protected final Pattern headerNamePattern;
    protected final KeyEncoding keyEncoding;
    protected final boolean commitOffsets;
    protected final OffsetTracker offsetTracker;
    protected final ComponentLog logger;
    protected final String brokerUri;
    protected final KafkaRecordTransform recordTransform;

    protected AbstractRecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final HeaderValueConverter headerValueConverter,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final ComponentLog logger,
            final String brokerUri,
            final KafkaRecordTransform recordTransform) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.headerValueConverter = headerValueConverter;
        this.headerNamePattern = headerNamePattern;
        this.keyEncoding = keyEncoding;
        this.commitOffsets = commitOffsets;
        this.offsetTracker = offsetTracker;
        this.logger = logger;
        this.brokerUri = brokerUri;
        this.recordTransform = recordTransform;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final Iterator<ByteRecord> consumerRecords) {
        while (consumerRecords.hasNext()) {
            final ByteRecord consumerRecord = consumerRecords.next();
            final String topic = consumerRecord.getTopic();
            final int partition = consumerRecord.getPartition();
            final byte[] value = consumerRecord.getValue();

            final Map<String, String> attributes = KafkaUtils.toAttributes(
                    consumerRecord, keyEncoding, headerNamePattern, headerValueConverter, commitOffsets);

            final Map<String, String> extraAttrs = recordTransform.extractHeaders(consumerRecord);

            try (final InputStream in = new ByteArrayInputStream(value);
                    final RecordReader reader = readerFactory.createRecordReader(attributes, in, value.length, logger)) {

                Record record;
                while ((record = reader.nextRecord()) != null) {
                    processRecord(session, consumerRecord, record, attributes, extraAttrs, topic, partition);
                }
            } catch (final MalformedRecordException | IOException | SchemaNotFoundException e) {
                logger.debug("Reader or Writer failed to process Kafka Record with Topic [{}] Partition [{}] Offset [{}]",
                        consumerRecord.getTopic(), consumerRecord.getPartition(), consumerRecord.getOffset(), e);
                handleParseFailure(session, consumerRecord, attributes, value);
                offsetTracker.update(consumerRecord);
                continue;
            } catch (final Exception e) {
                throw new RuntimeException("Failed to process Kafka message", e);
            }

            offsetTracker.update(consumerRecord);
        }

        finishGroups(session);
    }

    protected abstract void processRecord(
            ProcessSession session,
            ByteRecord consumerRecord,
            Record record,
            Map<String, String> attributes,
            Map<String, String> extraAttrs,
            String topic,
            int partition) throws Exception;

    protected abstract void finishGroups(ProcessSession session);

    protected void handleParseFailure(final ProcessSession session, final ByteRecord consumerRecord, final Map<String, String> attributes, final byte[] value) {
        FlowFile flowFile = session.create();
        flowFile = session.putAllAttributes(flowFile, attributes);
        flowFile = session.write(flowFile, out -> out.write(value));
        session.transfer(flowFile, ConsumeKafka.PARSE_FAILURE);
        session.adjustCounter("Records Received from " + consumerRecord.getTopic(), 1, false);
    }
}
