package org.glassfish.grizzly.http2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.SocketConnectorHandler;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.HttpResponsePacket;
import org.glassfish.grizzly.http.HttpTrailer;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.Protocol;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.nio.transport.TCPNIOConnectorHandler;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Streams for requests that carry a body (END_STREAM on a DATA frame, or on a trailing HEADERS frame,
 * rather than on the initial HEADERS frame) must reach
 * {@link Http2Stream.State#CLOSED} once the exchange completes, so that the periodic cleanup in
 * {@link Http2Session#deregisterStream()} can remove them from the session's streams map. Otherwise the
 * map grows by one entry per request for the lifetime of the connection.
 */
public class Http2StreamLeakTest extends AbstractHttp2Test {

    private static final int PORT = 18895;
    private static final int MAX_CONCURRENT_STREAMS = 10;
    private static final int REQUEST_COUNT = 40;

    private HttpServer httpServer;

    private final AtomicReference<Http2Session> serverSession = new AtomicReference<>();
    private volatile CountDownLatch responseLatch;
    private volatile int lastStatus;

    @Before
    public void before() throws Exception {
        final Http2Configuration configuration = Http2Configuration.builder()
                .disableCipherCheck(true)
                .maxConcurrentStreams(MAX_CONCURRENT_STREAMS) // high-water mark = 10 * 0.5 = 5 streams
                .build();
        // Run the closed-stream cleanup on every stream deregistration.
        configuration.setCleanFrequencyCheck(0);

        httpServer = HttpServer.createSimpleServer(null, PORT);
        final NetworkListener listener = httpServer.getListener("grizzly");
        listener.setSendFileEnabled(false);
        listener.getFileCache().setEnabled(false);
        listener.getKeepAlive().setIdleTimeoutInSeconds(-1);
        listener.registerAddOn(new Http2AddOn(configuration));

        httpServer.getServerConfiguration().addHttpHandler(new HttpHandler() {
            @Override
            public void service(final Request request, final Response response) throws Exception {
                serverSession.compareAndSet(null, Http2Stream.getStreamFor(request.getRequest()).getHttp2Session());

                // Consume the whole body, as a well-behaved handler would.
                final InputStream in = request.getInputStream();
                // noinspection StatementWithEmptyBody
                while (in.read() != -1) {
                }

                response.setContentType("text/plain");
                response.getWriter().write("ok");
            }
        }, "/path");

        httpServer.start();
    }

    @After
    public void after() throws Exception {
        httpServer.shutdownNow();
    }

    @Test
    public void testBodiedRequestStreamsAreRemovedFromStreamsMap() throws Exception {
        final Connection<?> connection = getConnection();

        for (int i = 0; i < REQUEST_COUNT; i++) {
            responseLatch = new CountDownLatch(1);
            connection.write(createRequest(PORT, "POST", "request-body-" + i, null));
            assertTrue("No response to request " + i, responseLatch.await(10, TimeUnit.SECONDS));
            assertEquals(200, lastStatus);
        }

        assertCompletedStreamsCleanedUp();
    }

    @Test
    public void testTrailerTerminatedRequestStreamsAreRemovedFromStreamsMap() throws Exception {
        final Connection<?> connection = getConnection();
        final MemoryManager memoryManager = MemoryManager.DEFAULT_MEMORY_MANAGER;

        for (int i = 0; i < REQUEST_COUNT; i++) {
            responseLatch = new CountDownLatch(1);
            final HttpRequestPacket request = HttpRequestPacket.builder().method(Method.POST).uri("/path")
                    .protocol(Protocol.HTTP_2_0).host("localhost:" + PORT).build();
            // HEADERS, then DATA without END_STREAM, then trailing HEADERS with END_STREAM.
            connection.write(HttpContent.builder(request).content(Buffers.wrap(memoryManager, "request-body-" + i)).last(false).build());
            connection.write(HttpTrailer.builder(request).content(Buffers.EMPTY_BUFFER).last(true)
                    .header("trailer-a", "value-" + i).build());
            assertTrue("No response to request " + i, responseLatch.await(10, TimeUnit.SECONDS));
            assertEquals(200, lastStatus);
        }

        assertCompletedStreamsCleanedUp();
    }

    // -------------------------------------------------------- Private Methods

    private void assertCompletedStreamsCleanedUp() throws Exception {
        // Let the server finish closing the last stream(s).
        Thread.sleep(500);

        final Http2Session session = serverSession.get();
        assertNotNull(session);
        final int retained = streamsMapSize(session);
        assertTrue("Server retained " + retained + " of " + REQUEST_COUNT
                + " completed streams in Http2Session.streamsMap; closed streams are not being cleaned up",
                retained <= MAX_CONCURRENT_STREAMS);
    }

    private static int streamsMapSize(final Http2Session session) throws Exception {
        final Field field = Http2Session.class.getDeclaredField("streamsMap");
        field.setAccessible(true);
        // Read after the connection has gone idle, so no lock is needed.
        return ((Map<?, ?>) field.get(session)).size();
    }

    private Connection<?> getConnection() throws Exception {
        final BaseFilter responseCollector = new BaseFilter() {
            @Override
            public NextAction handleRead(final FilterChainContext ctx) throws IOException {
                final HttpContent content = ctx.getMessage();
                if (content.isLast()) {
                    lastStatus = ((HttpResponsePacket) content.getHttpHeader()).getStatus();
                    responseLatch.countDown();
                }
                return ctx.getStopAction();
            }
        };

        final FilterChain clientChain = createClientFilterChainAsBuilder(false, true, responseCollector).build();
        final TCPNIOTransport transport = httpServer.getListener("grizzly").getTransport();
        final SocketConnectorHandler connectorHandler = TCPNIOConnectorHandler.builder(transport).processor(clientChain).build();
        final Future<Connection> connectFuture = connectorHandler.connect("localhost", PORT);
        return connectFuture.get(10, TimeUnit.SECONDS);
    }
}
