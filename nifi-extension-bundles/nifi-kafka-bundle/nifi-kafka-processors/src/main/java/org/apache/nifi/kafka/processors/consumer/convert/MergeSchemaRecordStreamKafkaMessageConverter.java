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
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Continue with Merged Schema strategy: groups by topic, partition, and header attributes,
 * merging per-record write schemas and writing once per group.
 */
public class MergeSchemaRecordStreamKafkaMessageConverter extends AbstractRecordStreamKafkaMessageConverter {

    private final Map<MergeGroupKey, MergeGroup> mergeGroups = new HashMap<>();

    public MergeSchemaRecordStreamKafkaMessageConverter(
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
        final Record toWrite = recordTransform.convertRecord(consumerRecord, record, attributes);

        final MergeGroupKey key = new MergeGroupKey(extraAttrs, topic, partition);
        final MergeGroup group = mergeGroups.computeIfAbsent(key, ignored -> new MergeGroup(attributes));
        group.add(toWrite, recordSchema, consumerRecord);
    }

    @Override
    protected void finishGroups(final ProcessSession session) {
        for (final Map.Entry<MergeGroupKey, MergeGroup> entry : mergeGroups.entrySet()) {
            final MergeGroupKey key = entry.getKey();
            final MergeGroup group = entry.getValue();

            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, Map.of(
                    KafkaFlowFileAttribute.KAFKA_TOPIC, key.topic(),
                    KafkaFlowFileAttribute.KAFKA_PARTITION, String.valueOf(key.partition())));

            final OutputStream out = session.write(flowFile);
            final Map<String, String> resultAttrs = new HashMap<>();
            final int recordCount;

            try {
                final RecordSchema writerSchema = writerFactory.getSchema(group.attributes, group.mergedRecordSchema);
                try (final RecordSetWriter writer = writerFactory.createWriter(logger, writerSchema, out, group.attributes)) {
                    writer.beginRecordSet();
                    for (final Record record : group.records) {
                        writer.write(record);
                    }
                    final WriteResult writeResult = writer.finishRecordSet();
                    resultAttrs.putAll(writeResult.getAttributes());
                    resultAttrs.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    resultAttrs.put(KafkaFlowFileAttribute.KAFKA_COUNT, String.valueOf(writeResult.getRecordCount()));
                    resultAttrs.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    recordCount = writeResult.getRecordCount();
                }
            } catch (final Exception e) {
                throw new ProcessException("Failed to write Kafka records to FlowFile", e);
            }

            resultAttrs.put(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(group.maxOffset));
            resultAttrs.put(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(group.minOffset));
            resultAttrs.put(KafkaFlowFileAttribute.KAFKA_TIMESTAMP, Long.toString(group.minTimestamp));
            resultAttrs.putAll(key.extraAttributes());
            resultAttrs.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));

            flowFile = session.putAllAttributes(flowFile, resultAttrs);
            session.getProvenanceReporter().receive(flowFile, brokerUri + "/" + key.topic());
            session.adjustCounter("Records Received from " + key.topic(), recordCount, false);
            session.transfer(flowFile, ConsumeKafka.SUCCESS);
        }
        mergeGroups.clear();
    }

    private record MergeGroupKey(Map<String, String> extraAttributes, String topic, int partition) {
    }

    private static final class MergeGroup {
        final Map<String, String> attributes;
        final List<Record> records = new ArrayList<>();
        RecordSchema mergedRecordSchema;
        long maxOffset = Long.MIN_VALUE;
        long minOffset = Long.MAX_VALUE;
        long minTimestamp = Long.MAX_VALUE;

        MergeGroup(final Map<String, String> attributes) {
            this.attributes = attributes;
        }

        void add(final Record record, final RecordSchema recordSchema, final ByteRecord consumerRecord) {
            if (record != null) {
                records.add(record);
            }
            mergedRecordSchema = DataTypeUtils.merge(mergedRecordSchema, recordSchema);
            maxOffset = Math.max(maxOffset, consumerRecord.getOffset());
            minOffset = Math.min(minOffset, consumerRecord.getOffset());
            minTimestamp = Math.min(minTimestamp, consumerRecord.getTimestamp());
        }
    }
}
