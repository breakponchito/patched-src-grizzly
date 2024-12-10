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

package org.glassfish.grizzly.http;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.TransformationResult;
import org.glassfish.grizzly.compression.zip.GZipDecoder;
import org.glassfish.grizzly.compression.zip.GZipEncoder;
import org.glassfish.grizzly.memory.Buffers;

import java.util.zip.Deflater;

/**
 * GZip {@link ContentEncoding} implementation, which compresses/decompresses HTTP content using gzip algorithm.
 *
 * @author Alexey Stashok
 */
public class GZipContentEncoding implements ContentEncoding {
    public static final int DEFAULT_IN_BUFFER_SIZE = 512;
    public static final int DEFAULT_OUT_BUFFER_SIZE = 512;

    private static final String[] ALIASES = { "gzip", "deflate" };

    public static final String NAME = "gzip";

    private final GZipDecoder decoder;
    private final GZipEncoder encoder;

    private final EncodingFilter encoderFilter;

    /**
     * Construct <tt>GZipContentEncoding</tt> using default buffer sizes.
     */
    public GZipContentEncoding() {
        this(DEFAULT_IN_BUFFER_SIZE, DEFAULT_OUT_BUFFER_SIZE);
    }

    /**
     * Construct <tt>GZipContentEncoding</tt> using specific buffer sizes.
     * 
     * @param inBufferSize input buffer size
     * @param outBufferSize output buffer size
     */
    public GZipContentEncoding(int inBufferSize, int outBufferSize) {
        this(inBufferSize, outBufferSize, null);
    }

    /**
     * Construct <tt>GZipContentEncoding</tt> using specific buffer sizes, with default compression level and strategy.
     * @param inBufferSize input buffer size
     * @param outBufferSize output buffer size
     * @param encoderFilter {@link EncodingFilter}, which will decide if
     * <tt>GZipContentEncoding</tt> should be applied to encode specific
     *                 {@link HttpHeader} packet.
     */
    public GZipContentEncoding(int inBufferSize, int outBufferSize, EncodingFilter encoderFilter) {
        this(inBufferSize, outBufferSize, Deflater.DEFAULT_COMPRESSION, Deflater.DEFAULT_STRATEGY, encoderFilter);
    }

    /**
     * Construct <tt>GZipContentEncoding</tt> using specific buffer sizes, compression level and strategy.
     * @param inBufferSize input buffer size
     * @param outBufferSize output buffer size
     * @param compressionLevel the compression level used by the GZipEncoder
     * @param compressionStrategy the compression strategy used by the GZipEncoder
     * @param encoderFilter {@link EncodingFilter}, which will decide if
     * <tt>GZipContentEncoding</tt> should be applied to encode specific
     *                 {@link HttpHeader} packet.
     */
    public GZipContentEncoding(int inBufferSize, int outBufferSize, int compressionLevel, int compressionStrategy,
                               EncodingFilter encoderFilter) {

        this.decoder = new GZipDecoder(inBufferSize);
        this.encoder = new GZipEncoder(outBufferSize, compressionLevel, compressionStrategy);
        if (encoderFilter != null) {
            this.encoderFilter = encoderFilter;
        } else {
            this.encoderFilter = new EncodingFilter() {
                @Override
                public boolean applyEncoding(final HttpHeader httpPacket) {
                    return false;
                }

                @Override
                public boolean applyDecoding(final HttpHeader httpPacket) {
                    return true;
                }
            };
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String[] getAliases() {
        return ALIASES.clone();
    }

    public static String[] getGzipAliases() {
        return ALIASES.clone();
    }

    @Override
    public final boolean wantDecode(final HttpHeader header) {
        return encoderFilter.applyDecoding(header);
    }

    @Override
    public final boolean wantEncode(final HttpHeader header) {
        return encoderFilter.applyEncoding(header);
    }

    @Override
    public ParsingResult decode(final Connection connection, final HttpContent httpContent) {
        final HttpHeader httpHeader = httpContent.getHttpHeader();

        final Buffer input = httpContent.getContent();
        final TransformationResult<Buffer, Buffer> result = decoder.transform(httpHeader, input);

        Buffer remainder = result.getExternalRemainder();

        if (remainder == null || !remainder.hasRemaining()) {
            input.tryDispose();
            remainder = null;
        } else {
            input.shrink();
        }

        try {
            switch (result.getStatus()) {
            case COMPLETE: {
                httpContent.setContent(result.getMessage());
                return ParsingResult.create(httpContent, remainder);
            }

            case INCOMPLETE: {
                return ParsingResult.create(null, remainder);
            }

            case ERROR: {
                throw new IllegalStateException("GZip decode error. Code: " + result.getErrorCode() + " Description: " + result.getErrorDescription());
            }

            default:
                throw new IllegalStateException("Unexpected status: " + result.getStatus());
            }
        } finally {
            result.recycle();
        }
    }

    @Override
    public HttpContent encode(Connection connection, HttpContent httpContent) {
        final HttpHeader httpHeader = httpContent.getHttpHeader();

        final Buffer input = httpContent.getContent();

        final boolean isLast = httpContent.isLast();
        if (!(isLast || input.hasRemaining())) {
            // the content is empty and is not last
            return httpContent;
        }

        final TransformationResult<Buffer, Buffer> result = encoder.transform(httpHeader, input);

        input.tryDispose();

        try {
            switch (result.getStatus()) {
            case COMPLETE:
            case INCOMPLETE: {
                Buffer encodedBuffer = result.getMessage();
                if (isLast) {
                    final Buffer finishBuffer = encoder.finish(httpHeader);
                    encodedBuffer = Buffers.appendBuffers(connection.getMemoryManager(), encodedBuffer, finishBuffer);
                }
                if (encodedBuffer != null) {
                    httpContent.setContent(encodedBuffer);
                    return httpContent;
                } else {
                    return null;
                }
            }

            case ERROR: {
                throw new IllegalStateException("GZip decode error. Code: " + result.getErrorCode() + " Description: " + result.getErrorDescription());
            }

            default:
                throw new IllegalStateException("Unexpected status: " + result.getStatus());
            }
        } finally {
            result.recycle();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GZipContentEncoding other = (GZipContentEncoding) obj;
        return getName().equals(other.getName());

    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + getName().hashCode();
        return hash;
    }
}
