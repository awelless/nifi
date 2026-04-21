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
package org.apache.nifi.kafka.service.ssl;

import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SslContextSslEngineFactoryTest {

    private SslContextSslEngineFactory factory;

    @BeforeEach
    void setUp() {
        factory = new SslContextSslEngineFactory();
    }

    @Test
    void testConfigureWithoutSslContextThrows() {
        final Map<String, Object> configs = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> factory.configure(configs));
    }

    @Test
    void testConfigureWithWrongSslContextTypeThrows() {
        final Map<String, Object> configs = Map.of(SslContextSslEngineFactory.PROPERTY_KEY_NIFI_SSL_CONTEXT, "not-an-SSLContext");
        assertThrows(IllegalArgumentException.class, () -> factory.configure(configs));
    }

    @Test
    void testCreateClientSslEngine() throws NoSuchAlgorithmException {
        final SSLContext sslContext = SSLContext.getDefault();

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SslContextSslEngineFactory.PROPERTY_KEY_NIFI_SSL_CONTEXT, sslContext);
        factory.configure(configs);

        final SSLEngine engine = factory.createClientSslEngine("localhost", 9093, null);

        assertTrue(engine.getUseClientMode());
        assertEquals("localhost", engine.getPeerHost());
        assertEquals(9093, engine.getPeerPort());
    }

    @Test
    void testCreateClientSslEngineWithCipherSuites() throws NoSuchAlgorithmException {
        final SSLContext sslContext = SSLContext.getDefault();
        final String[] supportedCiphers = sslContext.getDefaultSSLParameters().getCipherSuites();
        final String firstCipher = supportedCiphers[0];

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SslContextSslEngineFactory.PROPERTY_KEY_NIFI_SSL_CONTEXT, sslContext);
        configs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, List.of(firstCipher));
        factory.configure(configs);

        final SSLEngine engine = factory.createClientSslEngine("localhost", 9093, null);

        assertArrayEquals(new String[]{firstCipher}, engine.getSSLParameters().getCipherSuites());
    }

    @Test
    void testCreateClientSslEngineWithEnabledProtocols() throws NoSuchAlgorithmException {
        final SSLContext sslContext = SSLContext.getDefault();

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SslContextSslEngineFactory.PROPERTY_KEY_NIFI_SSL_CONTEXT, sslContext);
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, List.of("TLSv1.2"));
        factory.configure(configs);

        final SSLEngine engine = factory.createClientSslEngine("localhost", 9093, null);

        assertArrayEquals(new String[]{"TLSv1.2"}, engine.getSSLParameters().getProtocols());
    }

    @Test
    void testCreateClientSslEngineWithEndpointIdentification() throws NoSuchAlgorithmException {
        final SSLContext sslContext = SSLContext.getDefault();

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SslContextSslEngineFactory.PROPERTY_KEY_NIFI_SSL_CONTEXT, sslContext);
        factory.configure(configs);

        final SSLEngine engine = factory.createClientSslEngine("localhost", 9093, "HTTPS");

        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    void testCreateClientSslEngineWithoutEndpointIdentificationPassesThrough() throws NoSuchAlgorithmException {
        final SSLContext sslContext = SSLContext.getDefault();

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SslContextSslEngineFactory.PROPERTY_KEY_NIFI_SSL_CONTEXT, sslContext);
        factory.configure(configs);

        final SSLEngine engine = factory.createClientSslEngine("localhost", 9093, null);

        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    void testCreateServerSslEngineThrows() throws NoSuchAlgorithmException {
        final SSLContext sslContext = SSLContext.getDefault();

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SslContextSslEngineFactory.PROPERTY_KEY_NIFI_SSL_CONTEXT, sslContext);
        factory.configure(configs);

        assertThrows(UnsupportedOperationException.class, () -> factory.createServerSslEngine("localhost", 9093));
    }

    @Test
    void testShouldBeRebuiltReturnsFalse() {
        assertFalse(factory.shouldBeRebuilt(Collections.emptyMap()));
    }

    @Test
    void testReconfigurableConfigsEmpty() {
        assertTrue(factory.reconfigurableConfigs().isEmpty());
    }

    @Test
    void testKeystoreReturnsNull() {
        assertNull(factory.keystore());
    }

    @Test
    void testTruststoreReturnsNull() {
        assertNull(factory.truststore());
    }
}
