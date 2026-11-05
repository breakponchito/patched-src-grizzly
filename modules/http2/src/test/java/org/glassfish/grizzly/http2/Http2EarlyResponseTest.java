package org.glassfish.grizzly.http2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.SocketConnectorHandler;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.HttpResponsePacket;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.Protocol;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
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
 * The server responds to a request with a body before the client has finished uploading it
 * (for example a 401/413, or a handler that ignores the body).
 * <p>
 * Once the server has written its response and dropped its input, the stream is finished locally, but in
 * protocol terms (RFC 7540 section 5.1) it is only half-closed (local). The client may still be sending
 * DATA, including the final DATA frame with END_STREAM. The server must accept those frames on that
 * stream and must not treat them as a connection error. Treating them as one would send GOAWAY and
 * break every other stream on the connection.
 */
public class Http2EarlyResponseTest extends AbstractHttp2Test {

    private static final int PORT = 18894;

    private HttpServer httpServer;

    private final Map<String, CountDownLatch> responseLatches = new ConcurrentHashMap<>();
    private final Map<String, Integer> responseStatuses = new ConcurrentHashMap<>();

    @Before
    public void before() throws Exception {
        httpServer = createServer(null, PORT, false, true);
        httpServer.getListener("grizzly").getKeepAlive().setIdleTimeoutInSeconds(-1);

        // Responds immediately without reading the request body.
        httpServer.getServerConfiguration().addHttpHandler(new HttpHandler() {
            @Override
            public void service(final Request request, final Response response) throws Exception {
                response.setStatus(413);
                response.setContentType("text/plain");
                response.getWriter().write("too large");
            }
        }, "/early");

        httpServer.getServerConfiguration().addHttpHandler(new HttpHandler() {
            @Override
            public void service(final Request request, final Response response) throws Exception {
                response.setContentType("text/plain");
                response.getWriter().write("pong");
            }
        }, "/ping");

        httpServer.start();
    }

    @After
    public void after() throws Exception {
        httpServer.shutdownNow();
    }

    @Test
    public void testFinalDataFrameAfterEarlyResponseDoesNotKillConnection() throws Exception {
        responseLatches.put("/early", new CountDownLatch(1));
        responseLatches.put("/ping", new CountDownLatch(1));

        final Connection<?> connection = getConnection();
        final MemoryManager memoryManager = MemoryManager.DEFAULT_MEMORY_MANAGER;

        // 1. Start an upload: HEADERS + first DATA chunk, no END_STREAM.
        final HttpRequestPacket upload = HttpRequestPacket.builder().method(Method.POST).uri("/early")
                .protocol(Protocol.HTTP_2_0).host("localhost:" + PORT).contentType("application/octet-stream").build();
        connection.write(HttpContent.builder(upload).content(Buffers.wrap(memoryManager, "first-chunk")).last(false).build());

        // 2. The server answers without consuming the body.
        assertTrue("No response to /early", responseLatches.get("/early").await(10, TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(413), responseStatuses.get("/early"));

        // Give the server time to finish closing the stream locally (both input and output finalized).
        Thread.sleep(500);

        // 3. The client finishes its upload: final DATA frame with END_STREAM. This is legal: the server
        //    sent END_STREAM but no RST_STREAM, so the stream is only half-closed (local) on the server side.
        connection.write(HttpContent.builder(upload).content(Buffers.wrap(memoryManager, "last-chunk")).last(true).build());

        // Let the server process the final DATA frame.
        Thread.sleep(500);

        final Http2Session clientSession = Http2Session.get(connection);
        assertFalse("Server sent GOAWAY after receiving the final DATA frame of an early-answered upload",
                clientSession.isGoingAway());

        // 4. The connection must still be usable for new streams.
        final HttpRequestPacket ping = HttpRequestPacket.builder().method(Method.GET).uri("/ping")
                .protocol(Protocol.HTTP_2_0).host("localhost:" + PORT).build();
        connection.write(HttpContent.builder(ping).content(Buffers.EMPTY_BUFFER).last(true).build());

        assertTrue("Connection unusable after early response: no response to follow-up /ping request",
                responseLatches.get("/ping").await(10, TimeUnit.SECONDS));
        assertEquals(Integer.valueOf(200), responseStatuses.get("/ping"));
    }

    // -------------------------------------------------------- Private Methods

    private Connection<?> getConnection() throws Exception {
        final BaseFilter responseCollector = new BaseFilter() {
            @Override
            public NextAction handleRead(final FilterChainContext ctx) throws IOException {
                final HttpContent content = ctx.getMessage();
                final HttpResponsePacket response = (HttpResponsePacket) content.getHttpHeader();
                if (content.isLast()) {
                    final String uri = response.getRequest().getRequestURI();
                    responseStatuses.put(uri, response.getStatus());
                    final CountDownLatch latch = responseLatches.get(uri);
                    if (latch != null) {
                        latch.countDown();
                    }
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
