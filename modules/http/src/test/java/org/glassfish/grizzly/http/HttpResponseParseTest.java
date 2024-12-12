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

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.StandaloneProcessor;
import org.glassfish.grizzly.WriteHandler;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.impl.FutureImpl;
import org.glassfish.grizzly.impl.SafeFutureImpl;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.nio.NIOConnection;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.streams.StreamWriter;
import org.glassfish.grizzly.utils.ChunkingFilter;
import org.glassfish.grizzly.utils.Pair;

import junit.framework.TestCase;

/**
 * Testing HTTP response parsing
 *
 * @author Alexey Stashok
 */
public class HttpResponseParseTest extends TestCase {
    private static final Logger logger = Grizzly.logger(HttpResponseParseTest.class);

    public static final int PORT = 19021;

    public void testHeaderlessResponseLine() throws Exception {
        doHttpResponseTest("HTTP/1.0", 200, "OK", Collections.<String, Pair<String, String>>emptyMap(), "\r\n");
    }

    public void testSimpleHeaders() throws Exception {
        Map<String, Pair<String, String>> headers = new HashMap<>();
        headers.put("Header1", new Pair<>("localhost", "localhost"));
        headers.put("Content-length", new Pair<>("2345", "2345"));
        doHttpResponseTest("HTTP/1.0", 200, "ALL RIGHT", headers, "\r\n");
    }

    public void testMultiLineHeaders() throws Exception {
        Map<String, Pair<String, String>> headers = new HashMap<>();
        headers.put("Header1", new Pair<>("localhost", "localhost"));
        headers.put("Multi-line", new Pair<>("first\r\n          second\r\n       third", "first seconds third"));
        headers.put("Content-length", new Pair<>("2345", "2345"));
        doHttpResponseTest("HTTP/1.0", 200, "DONE", headers, "\r\n");
    }

    public void testHeadersN() throws Exception {
        Map<String, Pair<String, String>> headers = new HashMap<>();
        headers.put("Header1", new Pair<>("localhost", "localhost"));
        headers.put("Multi-line", new Pair<>("first\n          second\n       third", "first seconds third"));
        headers.put("Content-length", new Pair<>("2345", "2345"));
        doHttpResponseTest("HTTP/1.0", 200, "DONE", headers, "\r\n");
    }

    public void testDecoder100continueThen200() {
        try {
            doTestDecoder("HTTP/1.1 100 Continue\n\nHTTP/1.1 200 OK\n\n", 4096);
            assertTrue(true);
        } catch (IllegalStateException e) {
            logger.log(Level.SEVERE, "exception", e);
            assertTrue("Unexpected exception", false);
        }
    }

    public void testDecoderOK() {
        try {
            doTestDecoder("HTTP/1.0 404 Not found\n\n", 4096);
            assertTrue(true);
        } catch (IllegalStateException e) {
            logger.log(Level.SEVERE, "exception", e);
            assertTrue("Unexpected exception", false);
        }
    }

    public void testDecoderOverflowProtocol() {
        try {
            doTestDecoder("HTTP/1.0 404 Not found\n\n", 2);
            assertTrue("Overflow exception had to be thrown", false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testDecoderOverflowCode() {
        try {
            doTestDecoder("HTTP/1.0 404 Not found\n\n", 11);
            assertTrue("Overflow exception had to be thrown", false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testDecoderOverflowPhrase() {
        try {
            doTestDecoder("HTTP/1.0 404 Not found\n\n", 19);
            assertTrue("Overflow exception had to be thrown", false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    public void testDecoderOverflowHeader() {
        try {
            doTestDecoder("HTTP/1.0 404 Not found\nHeader1: somevalue\n\n", 30);
            assertTrue("Overflow exception had to be thrown", false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private HttpPacket doTestDecoder(String response, int limit) {

        MemoryManager mm = MemoryManager.DEFAULT_MEMORY_MANAGER;
        Buffer input = Buffers.wrap(mm, response);

        HttpClientFilter filter = new HttpClientFilter(limit);
        FilterChainContext ctx = FilterChainContext.create(new StandaloneConnection());
        ctx.setMessage(input);

        try {
            filter.handleRead(ctx);
            return (HttpPacket) ctx.getMessage();
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    private void doHttpResponseTest(String protocol, int code, String phrase, Map<String, Pair<String, String>> headers, String eol) throws Exception {

        final FutureImpl<Boolean> parseResult = SafeFutureImpl.create();

        Connection<SocketAddress> connection = null;
        StreamWriter writer;

        FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();
        filterChainBuilder.add(new TransportFilter());
        filterChainBuilder.add(new ChunkingFilter(2));
        filterChainBuilder.add(new HttpClientFilter());
        filterChainBuilder.add(new HTTPResponseCheckFilter(parseResult, protocol, code, phrase, Collections.<String, Pair<String, String>>emptyMap()));

        TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
        transport.setProcessor(filterChainBuilder.build());

        try {
            transport.bind(PORT);
            transport.start();

            Future<Connection> future = transport.connect("localhost", PORT);
            connection = future.get(10, TimeUnit.SECONDS);
            assertTrue(connection != null);

            connection.configureStandalone(true);

            StringBuilder sb = new StringBuilder();

            sb.append(protocol).append(" ").append(Integer.toString(code)).append(" ").append(phrase).append(eol);

            for (Entry<String, Pair<String, String>> entry : headers.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue().getFirst()).append(eol);
            }

            sb.append(eol);

            byte[] message = sb.toString().getBytes();

            writer = StandaloneProcessor.INSTANCE.getStreamWriter(connection);

            writer.writeByteArray(message);
            Future<Integer> writeFuture = writer.flush();

            assertTrue("Write timeout", writeFuture.isDone());
            assertEquals(message.length, (int) writeFuture.get());

            assertTrue(parseResult.get(10, TimeUnit.SECONDS));
        } finally {
            if (connection != null) {
                connection.closeSilently();
            }

            transport.shutdownNow();
        }
    }

    public static class HTTPResponseCheckFilter extends BaseFilter {
        private final FutureImpl<Boolean> parseResult;
        private final String protocol;
        private final int code;
        private final String phrase;
        private final Map<String, Pair<String, String>> headers;

        public HTTPResponseCheckFilter(FutureImpl<Boolean> parseResult, String protocol, int code, String phrase, Map<String, Pair<String, String>> headers) {
            this.parseResult = parseResult;
            this.protocol = protocol;
            this.code = code;
            this.phrase = phrase;
            this.headers = headers;
        }

        @Override
        public NextAction handleRead(FilterChainContext ctx) throws IOException {
            HttpContent httpContent = ctx.getMessage();
            HttpResponsePacket httpResponse = (HttpResponsePacket) httpContent.getHttpHeader();

            try {
                assertEquals(protocol, httpResponse.getProtocol().getProtocolString());
                assertEquals(code, httpResponse.getStatus());
                assertEquals(phrase, httpResponse.getReasonPhrase());

                for (Entry<String, Pair<String, String>> entry : headers.entrySet()) {
                    assertEquals(entry.getValue().getSecond(), httpResponse.getHeader(entry.getKey()));
                }

                parseResult.result(Boolean.TRUE);
            } catch (Throwable e) {
                parseResult.failure(e);
            }

            return ctx.getStopAction();
        }
    }

    protected static final class StandaloneConnection extends NIOConnection {
        public StandaloneConnection() {
            super(TCPNIOTransportBuilder.newInstance().build());
        }

        @Override
        protected void preClose() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public SocketAddress getPeerAddress() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public SocketAddress getLocalAddress() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getReadBufferSize() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setReadBufferSize(int readBufferSize) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getWriteBufferSize() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setWriteBufferSize(int writeBufferSize) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void notifyCanWrite(WriteHandler handler) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void notifyCanWrite(WriteHandler handler, int length) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean canWrite() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean canWrite(int length) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

}
