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
package org.apache.nifi.kafka.processors.consumer.transform;

import org.apache.nifi.kafka.processors.common.HeaderValueConverter;
import org.apache.nifi.kafka.processors.consumer.wrapper.ConsumeWrapperRecord;
import org.apache.nifi.kafka.processors.consumer.wrapper.WrapperRecordKeyReader;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Output Strategy USE_WRAPPER: wraps value, key, headers, and metadata in a wrapper record.
 */
public class UseWrapperRecordTransform implements KafkaRecordTransform {

    private final RecordReaderFactory keyReaderFactory;
    private final HeaderValueConverter headerValueConverter;
    private final KeyFormat keyFormat;
    private final KeyEncoding keyEncoding;
    private final ComponentLog logger;

    public UseWrapperRecordTransform(
            final RecordReaderFactory keyReaderFactory,
            final HeaderValueConverter headerValueConverter,
            final KeyFormat keyFormat,
            final KeyEncoding keyEncoding,
            final ComponentLog logger) {
        this.keyReaderFactory = keyReaderFactory;
        this.headerValueConverter = headerValueConverter;
        this.keyFormat = keyFormat;
        this.keyEncoding = keyEncoding;
        this.logger = logger;
    }

    @Override
    public RecordSchema convertRecordSchema(final RecordSchema inputSchema, final ByteRecord consumerRecord, final Map<String, String> attributes)
            throws IOException {
        try {
            final Tuple<RecordField, Object> recordKey = toRecordKey(consumerRecord, attributes);
            return WrapperRecord.toWrapperSchema(recordKey.getKey(), inputSchema);
        } catch (final IOException | SchemaNotFoundException | MalformedRecordException e) {
            throw new IOException("Unable to get schema for wrapper record", e);
        }
    }

    @Override
    public Record convertRecord(final ByteRecord consumerRecord, final Record record, final Map<String, String> attributes) throws IOException {
        try {
            final Tuple<RecordField, Object> recordKey = toRecordKey(consumerRecord, attributes);
            return new ConsumeWrapperRecord(headerValueConverter).toWrapperRecord(consumerRecord, record, recordKey);
        } catch (final IOException | SchemaNotFoundException | MalformedRecordException e) {
            throw new IOException("Unable to convert record", e);
        }
    }

    private Tuple<RecordField, Object> toRecordKey(final ByteRecord consumerRecord, final Map<String, String> attributes)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final WrapperRecordKeyReader keyReader = new WrapperRecordKeyReader(keyFormat, keyReaderFactory, keyEncoding, logger);
        return keyReader.toWrapperRecordKey(consumerRecord.getKey().orElse(null), attributes);
    }
}
