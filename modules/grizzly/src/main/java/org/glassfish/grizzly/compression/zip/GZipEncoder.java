/*
 * Copyright (c) 2010, 2020 Oracle and/or its affiliates. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.grizzly.compression.zip;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

import org.glassfish.grizzly.AbstractTransformer;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.TransformationException;
import org.glassfish.grizzly.TransformationResult;
import org.glassfish.grizzly.attributes.AttributeStorage;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.memory.ByteBufferArray;
import org.glassfish.grizzly.memory.MemoryManager;

/**
 * This class implements a {@link org.glassfish.grizzly.Transformer} which encodes plain data to the GZIP format.
 *
 * @author Alexey Stashok
 */
public class GZipEncoder extends AbstractTransformer<Buffer, Buffer> {
    private static final int GZIP_MAGIC = 0x8b1f;

    /*
     * Trailer size in bytes.
     */
    private static final int TRAILER_SIZE = 8;

    private final int bufferSize;
    private static int compressionLevel;
    private static int compressionStrategy;

    private static final Buffer header;

    static {
        header = MemoryManager.DEFAULT_MEMORY_MANAGER.allocate(10);
        header.put((byte) GZIP_MAGIC); // Magic number (short)
        header.put((byte) (GZIP_MAGIC >> 8)); // Magic number (short)
        header.put((byte) Deflater.DEFLATED); // Compression method (CM)
        header.put((byte) 0); // Flags (FLG)
        header.put((byte) 0); // Modification time MTIME (int)
        header.put((byte) 0); // Modification time MTIME (int)
        header.put((byte) 0); // Modification time MTIME (int)
        header.put((byte) 0); // Modification time MTIME (int)
        header.put((byte) 0); // Extra flags (XFLG)
        header.put((byte) 0); // Operating system (OS)

        header.flip();
    }

    public GZipEncoder() {
        this(512);
    }

    public GZipEncoder(int bufferSize) {
        this(bufferSize, Deflater.DEFAULT_COMPRESSION, Deflater.DEFAULT_STRATEGY);
    }

    public GZipEncoder(int bufferSize, int compressionLevel, int compressionStrategy) {
        this.bufferSize = bufferSize;
        this.compressionLevel = compressionLevel;
        this.compressionStrategy = compressionStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return "gzip-encoder";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasInputRemaining(AttributeStorage storage, Buffer input) {
        return input.hasRemaining();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GZipOutputState createStateObject() {
        return new GZipOutputState();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TransformationResult<Buffer, Buffer> transformImpl(AttributeStorage storage, Buffer input) throws TransformationException {

        final MemoryManager memoryManager = obtainMemoryManager(storage);
        final GZipOutputState state = (GZipOutputState) obtainStateObject(storage);

        if (!state.isInitialized) {
            state.initialize();
        }

        Buffer encodedBuffer = null;
        if (input != null && input.hasRemaining()) {
            encodedBuffer = encodeBuffer(input, state, memoryManager);
        }

        if (encodedBuffer == null) {
            return TransformationResult.createIncompletedResult(null);
        }

        // Put GZIP header if needed
        if (!state.isHeaderWritten) {
            state.isHeaderWritten = true;

            encodedBuffer = Buffers.appendBuffers(memoryManager, getHeader(), encodedBuffer);
        }

        return TransformationResult.createCompletedResult(encodedBuffer, null);
    }

    /**
     * Finishes to compress data to the output stream without closing the underlying stream. Use this method when applying
     * multiple filters in succession to the same output stream.
     *
     * @return {@link Buffer} with the last GZIP data to be sent.
     */
    public Buffer finish(AttributeStorage storage) {
        final MemoryManager memoryManager = obtainMemoryManager(storage);
        final GZipOutputState state = (GZipOutputState) obtainStateObject(storage);

        Buffer resultBuffer = null;

        if (state.isInitialized) {
            final Deflater deflater = state.deflater;
            if (!deflater.finished()) {
                deflater.finish();

                while (!deflater.finished()) {
                    resultBuffer = Buffers.appendBuffers(memoryManager, resultBuffer, deflate(deflater, memoryManager));
                }

                // Put GZIP header if needed
                if (!state.isHeaderWritten) {
                    state.isHeaderWritten = true;

                    resultBuffer = Buffers.appendBuffers(memoryManager, getHeader(), resultBuffer);
                }

                // Put GZIP member trailer
                final Buffer trailer = memoryManager.allocate(TRAILER_SIZE);
                final CRC32 crc32 = state.crc32;
                putUInt(trailer, (int) crc32.getValue());
                putUInt(trailer, deflater.getTotalIn());
                trailer.flip();

                resultBuffer = Buffers.appendBuffers(memoryManager, resultBuffer, trailer);
            }

            state.reset();
        }

        return resultBuffer;
    }

    private Buffer getHeader() {
        final Buffer headerToWrite = header.duplicate();
        headerToWrite.allowBufferDispose(false);
        return headerToWrite;
    }

    private Buffer encodeBuffer(Buffer buffer, GZipOutputState state, MemoryManager memoryManager) {
        final CRC32 crc32 = state.crc32;
        final Deflater deflater = state.deflater;

        if (deflater.finished()) {
            throw new IllegalStateException("write beyond end of stream");
        }

        // Deflate no more than stride bytes at a time. This avoids
        // excess copying in deflateBytes (see Deflater.c)
        int stride = bufferSize;
        Buffer resultBuffer = null;
        final ByteBufferArray byteBufferArray = buffer.toByteBufferArray();
        final ByteBuffer[] buffers = byteBufferArray.getArray();
        final int size = byteBufferArray.size();

        for (int i = 0; i < size; i++) {
            final ByteBuffer byteBuffer = buffers[i];
            final int len = byteBuffer.remaining();
            if (len > 0) {
                final byte[] buf;
                final int off;
                if (byteBuffer.hasArray()) {
                    buf = byteBuffer.array();
                    off = byteBuffer.arrayOffset() + byteBuffer.position();
                } else {
                    // @TODO allocate byte array via MemoryUtils
                    buf = new byte[len];
                    off = 0;
                    byteBuffer.get(buf);
                    byteBuffer.position(byteBuffer.position() - len);
                }

                for (int j = 0; j < len; j += stride) {
                    deflater.setInput(buf, off + j, Math.min(stride, len - j));
                    while (!deflater.needsInput()) {
                        final Buffer deflated = deflate(deflater, memoryManager);
                        if (deflated != null) {
                            resultBuffer = Buffers.appendBuffers(memoryManager, resultBuffer, deflated);
                        }
                    }
                }

                crc32.update(buf, off, len);
            }
        }

        byteBufferArray.restore();
        byteBufferArray.recycle();

        buffer.position(buffer.limit());

        return resultBuffer;
    }

    /**
     * Writes next block of compressed data to the output stream.
     */
    protected Buffer deflate(Deflater deflater, MemoryManager memoryManager) {
        final Buffer buffer = memoryManager.allocate(bufferSize);
        final ByteBuffer byteBuffer = buffer.toByteBuffer();
        final byte[] array = byteBuffer.array();
        final int offset = byteBuffer.arrayOffset() + byteBuffer.position();

        int len = deflater.deflate(array, offset, bufferSize);
        if (len <= 0) {
            buffer.dispose();
            return null;
        }

        buffer.position(len);
        buffer.trim();

        return buffer;
    }

    /*
     * Writes integer in Intel byte order to a byte array, starting at a given offset.
     */
    private static void putUInt(Buffer buffer, int value) {
        putUShort(buffer, value & 0xffff);
        putUShort(buffer, value >> 16 & 0xffff);
    }

    /*
     * Writes short integer in Intel byte order to a byte array, starting at a given offset
     */
    private static void putUShort(Buffer buffer, int value) {
        buffer.put((byte) (value & 0xff));
        buffer.put((byte) (value >> 8 & 0xff));
    }

    protected static final class GZipOutputState extends LastResultAwareState<Buffer, Buffer> {
        private boolean isInitialized;
        private boolean isHeaderWritten;

        /**
         * CRC-32 of uncompressed data.
         */
        private CRC32 crc32;

        /**
         * Compressor for this stream.
         */
        private Deflater deflater;

        private void initialize() {
            final Deflater newDeflater = new Deflater(compressionLevel, true);
            newDeflater.setStrategy(compressionStrategy);
            final CRC32 newCrc32 = new CRC32();
            newCrc32.reset();
            deflater = newDeflater;
            crc32 = newCrc32;
            isInitialized = true;
        }

        private void reset() {
            isInitialized = false;
            isHeaderWritten = false;
            deflater.end(); // ensure we don't leak memory in native compression library
            crc32 = null;
            deflater = null;
        }
    }
}
