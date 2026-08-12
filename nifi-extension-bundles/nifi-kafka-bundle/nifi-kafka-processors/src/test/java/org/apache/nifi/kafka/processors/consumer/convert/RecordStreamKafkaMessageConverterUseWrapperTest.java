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
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.transform.UseWrapperRecordTransform;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class RecordStreamKafkaMessageConverterUseWrapperTest {

    private final RecordSetWriterFactory writerFactory = mock(RecordSetWriterFactory.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));
    private final RecordReaderFactory keyReaderFactory = mock(RecordReaderFactory.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));
    private final ComponentLog logger = mock(ComponentLog.class);
    private final ProcessSession session = mock(ProcessSession.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));

    private final OffsetTracker offsetTracker = new OffsetTracker();

    @Test
    void testGroupingOfMessagesByTopicAndPartition() throws Exception {
        final MockRecordParser readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("field1", RecordFieldType.STRING);

        readerFactory.addRecord("value1");
        readerFactory.addRecord("value2");
        readerFactory.addRecord("value3");
        readerFactory.addRecord("value4");

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory,
                writerFactory,
                value -> new String(value, StandardCharsets.UTF_8),
                Pattern.compile(".*"),
                KeyEncoding.UTF8,
                true,
                offsetTracker,
                logger,
                "brokerUri",
                new UseWrapperRecordTransform(keyReaderFactory,
                        value -> new String(value, StandardCharsets.UTF_8), KeyFormat.STRING, KeyEncoding.UTF8, logger)
        );

        final ByteRecord group1Record1 = new ByteRecord("topic1", 0, 0, 0L, List.of(), null, "value1".getBytes(), 0L);
        final ByteRecord group1Record2 = new ByteRecord("topic1", 0, 3, 0L, List.of(), null, "value4".getBytes(), 0L);
        final ByteRecord group2Record1 = new ByteRecord("topic1", 1, 1, 0L, List.of(), null, "value2".getBytes(), 0L);
        final ByteRecord group3Record1 = new ByteRecord("topic2", 0, 2, 0L, List.of(), null, "value3".getBytes(), 0L);

        final Iterator<ByteRecord> consumerRecords = List.of(group1Record1, group2Record1, group3Record1, group1Record2).iterator();
        final FlowFile flowFile1 = new MockFlowFile(1);
        final FlowFile flowFile2 = new MockFlowFile(2);
        final FlowFile flowFile3 = new MockFlowFile(3);
        final FlowFile flowFile4 = new MockFlowFile(4);
        when(session.create()).thenReturn(flowFile1, flowFile2, flowFile3, flowFile4);
        when(session.write(any(FlowFile.class))).thenReturn(mock(OutputStream.class));
        when(writerFactory.getSchema(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));

        converter.toFlowFiles(session, consumerRecords);

        final ArgumentCaptor<Map<String, String>> attributesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(session, atLeastOnce()).putAllAttributes(any(FlowFile.class), attributesCaptor.capture());

        final List<Map<String, String>> capturedAttributes = attributesCaptor.getAllValues();
        final List<Map<String, String>> createdGroupAttributes = capturedAttributes.stream()
                .filter(attrs -> attrs.containsKey(KafkaFlowFileAttribute.KAFKA_TOPIC)
                        && attrs.containsKey(KafkaFlowFileAttribute.KAFKA_PARTITION)
                        && attrs.size() == 2)
                .toList();
        assertEquals(3, createdGroupAttributes.size());

        assertEquals("topic1", createdGroupAttributes.get(0).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", createdGroupAttributes.get(0).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        assertEquals("topic1", createdGroupAttributes.get(1).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("1", createdGroupAttributes.get(1).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        assertEquals("topic2", createdGroupAttributes.get(2).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", createdGroupAttributes.get(2).get(KafkaFlowFileAttribute.KAFKA_PARTITION));
    }
}
