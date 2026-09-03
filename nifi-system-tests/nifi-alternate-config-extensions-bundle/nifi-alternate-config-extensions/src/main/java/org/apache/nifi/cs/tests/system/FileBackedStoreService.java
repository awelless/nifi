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

package org.apache.nifi.cs.tests.system;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * File-backed store whose store and removal-marker files are named after the service identifier
 * so that two services never share a file and a substituted service is distinguishable from a preserved one.
 */
public class FileBackedStoreService extends AbstractControllerService implements StoreService {

    static final PropertyDescriptor STORE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Store Directory")
            .required(true)
            .defaultValue("store")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(STORE_DIRECTORY);

    private volatile File storeFile;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws IOException {
        final String directoryName = context.getProperty(STORE_DIRECTORY).getValue();
        final File directory = new File(directoryName);
        Files.createDirectories(directory.toPath());
        storeFile = new File(directory, "store-" + getIdentifier() + ".log");
        if (!storeFile.exists()) {
            Files.createFile(storeFile.toPath());
        }
    }

    @OnRemoved
    public void onRemoved(final ConfigurationContext context) throws IOException {
        final String directoryName = context.getProperty(STORE_DIRECTORY).getValue();
        final File directory = new File(directoryName);
        // Removing the service destroys the store, which is what makes accidental removal during a flow upgrade observable to tests.
        final File markerFile = new File(directory, "removed-" + getIdentifier() + ".log");
        Files.writeString(markerFile.toPath(), String.valueOf(System.currentTimeMillis()));
        Files.deleteIfExists(new File(directory, "store-" + getIdentifier() + ".log").toPath());
    }

    @Override
    public synchronized void append(final String row) {
        try {
            Files.writeString(storeFile.toPath(), row + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
