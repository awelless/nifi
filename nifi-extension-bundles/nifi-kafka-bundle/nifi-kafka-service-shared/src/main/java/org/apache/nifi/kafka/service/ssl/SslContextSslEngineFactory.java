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
import org.apache.kafka.common.security.auth.SslEngineFactory;

import java.security.KeyStore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * Kafka {@link SslEngineFactory} that creates client-mode {@link SSLEngine} instances from a pre-built
 * {@link SSLContext} provided by a NiFi {@code SSLContextProvider}.
 *
 * <p>The {@link SSLContext} object is passed through the Kafka configuration map under the
 * {@link #PROPERTY_KEY_NIFI_SSL_CONTEXT} key, following the same object-passing pattern used by
 * NiFi's OAuth integration (which passes an {@code OAuth2AccessTokenProvider} through Kafka config).
 *
 * <p>Server-side engine creation ({@link #createServerSslEngine}) is unsupported because NiFi
 * operates exclusively as a Kafka client. The {@link #keystore()} and {@link #truststore()} methods
 * return {@code null} since the underlying key/trust material is encapsulated within the provided
 * {@link SSLContext}. Engine configuration (cipher suites, enabled protocols, and endpoint
 * identification) follows the same contract as Kafka's {@code DefaultSslEngineFactory}.
 *
 * <p>This factory does not support dynamic reconfiguration ({@link #shouldBeRebuilt} returns
 * {@code false}). SSL context lifecycle is managed by the NiFi controller service.
 *
 * @see org.apache.nifi.kafka.service.security.OAuthBearerLoginCallbackHandler
 */
public final class SslContextSslEngineFactory implements SslEngineFactory {

    public static final String PROPERTY_KEY_NIFI_SSL_CONTEXT = "nifi.ssl.context";

    private SSLContext sslContext;
    private String[] cipherSuites;
    private String[] enabledProtocols;

    @Override
    public void configure(final Map<String, ?> configs) {
        final Object contextObject = configs.get(PROPERTY_KEY_NIFI_SSL_CONTEXT);
        if (contextObject instanceof SSLContext configuredContext) {
            sslContext = configuredContext;
        } else {
            throw new IllegalArgumentException("SSLContext must be provided via [%s] property in Kafka configuration".formatted(PROPERTY_KEY_NIFI_SSL_CONTEXT));
        }

        cipherSuites = getConfiguredValues(configs, SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        enabledProtocols = getConfiguredValues(configs, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
    }

    @Override
    public SSLEngine createClientSslEngine(final String peerHost, final int peerPort, final String endpointIdentification) {
        final SSLEngine engine = sslContext.createSSLEngine(peerHost, peerPort);
        if (cipherSuites != null) {
            engine.setEnabledCipherSuites(cipherSuites);
        }
        if (enabledProtocols != null) {
            engine.setEnabledProtocols(enabledProtocols);
        }
        engine.setUseClientMode(true);

        final SSLParameters params = engine.getSSLParameters();
        params.setEndpointIdentificationAlgorithm(endpointIdentification);
        engine.setSSLParameters(params);

        return engine;
    }

    @Override
    public SSLEngine createServerSslEngine(final String peerHost, final int peerPort) {
        throw new UnsupportedOperationException("Server SSLEngine creation is not supported");
    }

    @Override
    public boolean shouldBeRebuilt(final Map<String, Object> nextConfigs) {
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    @Override
    public KeyStore keystore() {
        return null;
    }

    @Override
    public KeyStore truststore() {
        return null;
    }

    @Override
    public void close() {
    }

    private static String[] getConfiguredValues(final Map<String, ?> configs, final String key) {
        final Object value = configs.get(key);
        if (value instanceof List<?> list && !list.isEmpty()) {
            return list.stream()
                    .map(Object::toString)
                    .toArray(String[]::new);
        }
        return null;
    }
}
