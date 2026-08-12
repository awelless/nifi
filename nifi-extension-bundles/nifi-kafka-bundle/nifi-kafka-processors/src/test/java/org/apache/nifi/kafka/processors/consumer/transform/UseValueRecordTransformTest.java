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

import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
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
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UseValueRecordTransformTest {

    private static final RecordSchema SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("fieldA", RecordFieldType.STRING.getDataType())));

    private static final Record RECORD = new MapRecord(SCHEMA, Map.of("fieldA", "hello"));

    private UseValueRecordTransform transform;

    @BeforeEach
    void setUp() {
        transform = new UseValueRecordTransform(
                value -> new String(value, StandardCharsets.UTF_8), Pattern.compile("match-.*"));
    }

    @Test
    void testGetRecordSchemaPassesThroughInputSchema() {
        final ByteRecord consumerRecord = byteRecord(List.of());
        final RecordSchema recordSchema = transform.convertRecordSchema(SCHEMA, consumerRecord, Map.of());
        assertSame(SCHEMA, recordSchema);
    }

    @Test
    void testConvertRecordReturnsIdentity() {
        final ByteRecord consumerRecord = byteRecord(List.of());
        final Record converted = transform.convertRecord(consumerRecord, RECORD, Map.of());
        assertSame(RECORD, converted);
    }

    @Test
    void testExtractHeadersPromotesMatchingAndIgnoresNonMatching() {
        final ByteRecord consumerRecord = byteRecord(List.of(
                new RecordHeader("match-one", "a".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("skip-me", "b".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("match-two", "c".getBytes(StandardCharsets.UTF_8))));

        final Map<String, String> headers = transform.extractHeaders(consumerRecord);
        assertEquals(2, headers.size());
        assertEquals("a", headers.get("match-one"));
        assertEquals("c", headers.get("match-two"));
        assertFalse(headers.containsKey("skip-me"));
    }

    @Test
    void testExtractHeadersEmptyWhenPatternNull() {
        final UseValueRecordTransform noPattern = new UseValueRecordTransform(
                value -> new String(value, StandardCharsets.UTF_8), null);
        final ByteRecord consumerRecord = byteRecord(List.of(
                new RecordHeader("any", "x".getBytes(StandardCharsets.UTF_8))));
        assertTrue(noPattern.extractHeaders(consumerRecord).isEmpty());
    }

    private static ByteRecord byteRecord(final List<RecordHeader> headers) {
        return new ByteRecord("topic", 0, 10L, 1000L, headers, null, "payload".getBytes(StandardCharsets.UTF_8), 0L);
    }
}
