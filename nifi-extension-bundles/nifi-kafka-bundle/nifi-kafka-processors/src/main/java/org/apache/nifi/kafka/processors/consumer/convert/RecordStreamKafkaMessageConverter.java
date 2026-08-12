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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.common.HeaderValueConverter;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.transform.KafkaRecordTransform;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Create New FlowFile strategy: groups by write schema, topic, partition, and header attributes,
 * streaming records into an open writer per group.
 */
public class RecordStreamKafkaMessageConverter extends AbstractRecordStreamKafkaMessageConverter {

    private final Map<RecordGroupCriteria, RecordGroup> recordGroups = new HashMap<>();

    public RecordStreamKafkaMessageConverter(
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
        super(readerFactory, writerFactory, headerValueConverter, headerNamePattern, keyEncoding, commitOffsets, offsetTracker, logger, brokerUri,
                recordTransform);
    }

    @Override
    protected void processRecord(
            final ProcessSession session,
            final ByteRecord consumerRecord,
            final Record record,
            final Map<String, String> attributes,
            final Map<String, String> extraAttrs,
            final String topic,
            final int partition) throws Exception {
        final RecordSchema inputSchema = record == null ? EMPTY_SCHEMA : record.getSchema();
        final RecordSchema recordSchema = recordTransform.convertRecordSchema(inputSchema, consumerRecord, attributes);
        final RecordSchema writeSchema = writerFactory.getSchema(attributes, recordSchema);

        final RecordGroupCriteria criteria = new RecordGroupCriteria(writeSchema, extraAttrs, topic, partition);
        RecordGroup group = recordGroups.get(criteria);
        if (group == null) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, Map.of(
                    KafkaFlowFileAttribute.KAFKA_TOPIC, topic,
                    KafkaFlowFileAttribute.KAFKA_PARTITION, String.valueOf(partition)));

            final OutputStream out = session.write(flowFile);
            final RecordSetWriter writer;
            try {
                writer = writerFactory.createWriter(logger, writeSchema, out, attributes);
                writer.beginRecordSet();
            } catch (final Exception ex) {
                out.close();
                throw ex;
            }

            final long offset = consumerRecord.getOffset();
            group = new RecordGroup(flowFile, writer, new AtomicLong(offset), new AtomicLong(offset), new AtomicLong(consumerRecord.getTimestamp()));
            recordGroups.put(criteria, group);
        } else {
            final long recordOffset = consumerRecord.getOffset();
            final AtomicLong maxOffset = group.maxOffset();
            if (recordOffset > maxOffset.get()) {
                maxOffset.set(recordOffset);
            }

            final AtomicLong minOffset = group.minOffset();
            if (recordOffset < minOffset.get()) {
                minOffset.set(recordOffset);
            }

            final long recordTimestamp = consumerRecord.getTimestamp();
            final AtomicLong minTimestamp = group.minTimestamp();
            if (recordTimestamp < minTimestamp.get()) {
                minTimestamp.set(recordTimestamp);
            }
        }

        final Record toWrite = recordTransform.convertRecord(consumerRecord, record, attributes);
        if (toWrite != null) {
            group.writer().write(toWrite);
        }
    }

    @Override
    protected void finishGroups(final ProcessSession session) {
        for (final Map.Entry<RecordGroupCriteria, RecordGroup> entry : recordGroups.entrySet()) {
            final RecordGroupCriteria criteria = entry.getKey();
            final RecordGroup group = entry.getValue();

            final Map<String, String> resultAttrs = new HashMap<>();
            final int recordCount;
            try (final RecordSetWriter writer = group.writer()) {
                final WriteResult writeResult = writer.finishRecordSet();
                resultAttrs.putAll(writeResult.getAttributes());
                resultAttrs.put("record.count", String.valueOf(writeResult.getRecordCount()));
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_COUNT, String.valueOf(writeResult.getRecordCount()));
                resultAttrs.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(group.maxOffset().get()));
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(group.minOffset().get()));
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_TIMESTAMP, Long.toString(group.minTimestamp().get()));
                resultAttrs.putAll(criteria.extraAttributes());
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));
                recordCount = writeResult.getRecordCount();
            } catch (final Exception ex) {
                throw new ProcessException("Failed to write Kafka records to FlowFile", ex);
            }

            FlowFile flowFile = group.flowFile();
            flowFile = session.putAllAttributes(flowFile, resultAttrs);
            session.getProvenanceReporter().receive(flowFile, brokerUri + "/" + criteria.topic());
            session.adjustCounter("Records Received from " + criteria.topic(), recordCount, false);
            session.transfer(flowFile, ConsumeKafka.SUCCESS);
        }
        recordGroups.clear();
    }

    private record RecordGroupCriteria(RecordSchema schema, Map<String, String> extraAttributes, String topic, int partition) {
    }

    private record RecordGroup(FlowFile flowFile, RecordSetWriter writer, AtomicLong maxOffset, AtomicLong minOffset, AtomicLong minTimestamp) {
    }
}
