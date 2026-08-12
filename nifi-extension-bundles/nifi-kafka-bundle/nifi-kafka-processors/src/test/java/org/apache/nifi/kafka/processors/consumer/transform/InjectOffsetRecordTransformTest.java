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

import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InjectOffsetRecordTransformTest {

    private static final RecordSchema SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("fieldA", RecordFieldType.STRING.getDataType())));

    private static final Record RECORD = new MapRecord(SCHEMA, Map.of("fieldA", "hello"));

    private final InjectOffsetRecordTransform transform = new InjectOffsetRecordTransform();

    @Test
    void testGetRecordSchemaAddsKafkaOffset() {
        final ByteRecord consumerRecord = byteRecord(42L);
        final RecordSchema recordSchema = transform.convertRecordSchema(SCHEMA, consumerRecord, Map.of());

        assertEquals(2, recordSchema.getFields().size());
        assertTrue(recordSchema.getField("fieldA").isPresent());
        assertTrue(recordSchema.getField("kafkaOffset").isPresent());
        assertEquals(RecordFieldType.LONG, recordSchema.getField("kafkaOffset").get().getDataType().getFieldType());
    }

    @Test
    void testConvertRecordIncludesOffsetValue() {
        final ByteRecord consumerRecord = byteRecord(42L);
        final Record converted = transform.convertRecord(consumerRecord, RECORD, Map.of());

        assertEquals("hello", converted.getValue("fieldA"));
        assertEquals(42L, converted.getValue("kafkaOffset"));
        assertNotNull(converted.getSchema().getField("kafkaOffset").orElse(null));
    }

    private static ByteRecord byteRecord(final long offset) {
        return new ByteRecord("topic", 0, offset, 1000L, List.of(), null, "payload".getBytes(StandardCharsets.UTF_8), 0L);
    }
}
