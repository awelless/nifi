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

import org.apache.nifi.kafka.processors.producer.wrapper.InjectMetadataRecord;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class InjectMetadataRecordTransformTest {

    private static final RecordSchema SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("fieldA", RecordFieldType.STRING.getDataType())));

    private static final Record RECORD = new MapRecord(SCHEMA, Map.of("fieldA", "hello"));

    private InjectMetadataRecordTransform transform;

    @BeforeEach
    void setUp() {
        final ComponentLog logger = mock(ComponentLog.class);
        transform = new InjectMetadataRecordTransform(
                null,
                value -> new String(value, StandardCharsets.UTF_8),
                KeyFormat.STRING, KeyEncoding.UTF8, logger);
    }

    @Test
    void testGetRecordSchemaInjectsKafkaMetadataKeepingValueFields() throws Exception {
        final ByteRecord consumerRecord = byteRecord();
        final RecordSchema recordSchema = transform.convertRecordSchema(SCHEMA, consumerRecord, Map.of());

        assertTrue(recordSchema.getField("fieldA").isPresent());
        assertTrue(recordSchema.getField(InjectMetadataRecord.METADATA).isPresent());
    }

    @Test
    void testConvertRecordInjectsKafkaMetadataAtRoot() throws Exception {
        final ByteRecord consumerRecord = byteRecord();
        final Record converted = transform.convertRecord(consumerRecord, RECORD, Map.of());

        assertEquals("hello", converted.getValue("fieldA"));
        assertNotNull(converted.getValue(InjectMetadataRecord.METADATA));

        @SuppressWarnings("unchecked")
        final Map<String, Object> metadata = (Map<String, Object>) converted.getValue(InjectMetadataRecord.METADATA);
        assertEquals("topic", metadata.get(InjectMetadataRecord.TOPIC));
        assertEquals(0, metadata.get(InjectMetadataRecord.PARTITION));
        assertEquals(10L, metadata.get(InjectMetadataRecord.OFFSET));
        assertEquals("my-key", metadata.get(InjectMetadataRecord.KEY));

        @SuppressWarnings("unchecked")
        final Map<String, String> headers = (Map<String, String>) metadata.get(InjectMetadataRecord.HEADERS);
        assertEquals("h-value", headers.get("h1"));
    }

    @Test
    void testExtractHeadersDefaultsEmpty() {
        assertTrue(transform.extractHeaders(byteRecord()).isEmpty());
    }

    private static ByteRecord byteRecord() {
        return new ByteRecord("topic", 0, 10L, 1000L,
                List.of(new RecordHeader("h1", "h-value".getBytes(StandardCharsets.UTF_8))),
                "my-key".getBytes(StandardCharsets.UTF_8),
                "payload".getBytes(StandardCharsets.UTF_8), 0L);
    }
}
