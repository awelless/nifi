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
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Output Strategy USE_VALUE: pass the value record through and promote matching headers for grouping.
 */
public class UseValueRecordTransform implements KafkaRecordTransform {

    private final HeaderValueConverter headerValueConverter;
    private final Pattern headerNamePattern;

    public UseValueRecordTransform(final HeaderValueConverter headerValueConverter, final Pattern headerNamePattern) {
        this.headerValueConverter = headerValueConverter;
        this.headerNamePattern = headerNamePattern;
    }

    @Override
    public RecordSchema convertRecordSchema(final RecordSchema inputSchema, final ByteRecord consumerRecord, final Map<String, String> attributes) {
        return inputSchema;
    }

    @Override
    public Record convertRecord(final ByteRecord consumerRecord, final Record record, final Map<String, String> attributes) {
        return record;
    }

    @Override
    public Map<String, String> extractHeaders(final ByteRecord consumerRecord) {
        if (headerNamePattern == null || consumerRecord == null) {
            return Map.of();
        }

        final Map<String, String> headers = new HashMap<>();
        for (final RecordHeader header : consumerRecord.getHeaders()) {
            final String name = header.key();
            if (headerNamePattern.matcher(name).matches()) {
                headers.put(name, headerValueConverter.convert(header.value()));
            }
        }
        return headers;
    }
}
