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
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.Map;

/**
 * Single-message transform that shapes a Kafka value record according to a ConsumeKafka Output Strategy.
 * Converters are responsible for obtaining the Record Writer schema via {@code RecordSetWriterFactory#getSchema}.
 */
public interface KafkaRecordTransform {

    /**
     * Converts the reader schema into the strategy-shaped record schema derived from the reader schema (for example, with
     * {@code kafkaOffset} injected or wrapped). This is not the Record Writer schema.
     */
    RecordSchema convertRecordSchema(RecordSchema inputSchema, ByteRecord consumerRecord, Map<String, String> attributes) throws IOException;

    Record convertRecord(ByteRecord consumerRecord, Record record, Map<String, String> attributes) throws IOException;

    /**
     * Header attributes to include in FlowFile grouping. Only {@link UseValueRecordTransform}
     * promotes matching headers; other strategies return an empty map.
     */
    default Map<String, String> extractHeaders(ByteRecord consumerRecord) {
        return Map.of();
    }
}
