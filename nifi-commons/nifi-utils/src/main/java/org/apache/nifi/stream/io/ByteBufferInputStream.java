/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.stream.io;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
    private final ByteBuffer buffer;
    private int mark = 0;

    public ByteBufferInputStream(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() {
        if (!buffer.hasRemaining()) {
            return -1; // End of stream
        }
        return buffer.get() & 0xFF; // Return unsigned byte
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) {
        if (!this.buffer.hasRemaining()) {
            return -1; // End of stream
        }

        int bytesRead = Math.min(length, this.buffer.remaining());
        this.buffer.get(buffer, offset, bytesRead);
        return bytesRead;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public long skip(final long n) {
        if (n <= 0 || !buffer.hasRemaining()) {
            return 0; // No bytes to skip
        }

        int bytesToSkip = (int) Math.min(n, buffer.remaining());
        buffer.position(buffer.position() + bytesToSkip);
        return bytesToSkip;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(final int readlimit) {
        mark = buffer.position();
    }

    @Override
    public void reset() {
        buffer.position(mark);
    }

}
