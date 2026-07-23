/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.otel;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for OTLP/HTTP export against an authenticated receiver.
 *
 * <p>The embedded HTTP server plays the role of a SaaS OTLP endpoint (Datadog-style): it rejects
 * any request without the expected {@code Authorization} header with 401 and accepts correctly
 * authenticated requests with 200. This directly demonstrates the FLINK-38309 defect (before the
 * headers option existed, the reporter could never authenticate) and the fix (with {@code
 * exporter.http-headers} configured, metrics are delivered).
 */
class OpenTelemetryMetricReporterAuthTest {

    private static final String EXPECTED_AUTH = "Basic dXNlcjpwYXNz";
    private static final String API_KEY_HEADER = "X-Api-Key";
    private static final String EXPECTED_API_KEY = "test-api-key-123";

    private HttpServer server;
    private String endpoint;
    private final ConcurrentLinkedQueue<Map<String, List<String>>> acceptedRequestHeaders =
            new ConcurrentLinkedQueue<>();
    private final AtomicInteger unauthorizedCount = new AtomicInteger();
    private final AtomicInteger acceptedCount = new AtomicInteger();

    @BeforeEach
    void startAuthenticatedOtlpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/v1/metrics",
                exchange -> {
                    String auth = exchange.getRequestHeaders().getFirst("Authorization");
                    String apiKey = exchange.getRequestHeaders().getFirst(API_KEY_HEADER);
                    boolean authorized =
                            EXPECTED_AUTH.equals(auth) && EXPECTED_API_KEY.equals(apiKey);
                    byte[] body;
                    if (authorized) {
                        acceptedCount.incrementAndGet();
                        acceptedRequestHeaders.add(exchange.getRequestHeaders());
                        // minimal valid ExportMetricsServiceResponse: empty proto message
                        body = new byte[0];
                        exchange.getResponseHeaders().add("Content-Type", "application/x-protobuf");
                        exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
                    } else {
                        unauthorizedCount.incrementAndGet();
                        body = "unauthorized".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(401, body.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(body);
                        }
                    }
                    exchange.close();
                });
        server.start();
        endpoint = String.format("http://127.0.0.1:%d/v1/metrics", server.getAddress().getPort());
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private OpenTelemetryMetricReporter reportOneCounter(MetricConfig config) {
        OpenTelemetryMetricReporter reporter = new OpenTelemetryMetricReporter();
        reporter.open(config);
        Counter counter = new SimpleCounter();
        counter.inc(42);
        reporter.notifyOfAddedMetric(counter, "e2eCounter", new EndToEndTestMetricGroup());
        reporter.report();
        reporter.close(); // close() flushes and waits for the last report
        return reporter;
    }

    private MetricConfig baseConfig() {
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.protocol", "http");
        config.setProperty("exporter.endpoint", endpoint);
        config.setProperty("exporter.timeout", "10s");
        return config;
    }

    @Test
    void beforeFixWithoutHeadersReceiverRejectsExport() {
        // Simulates the pre-FLINK-38309 world: the reporter has no way to attach
        // authentication, so the receiver rejects the export. Metrics are lost.
        reportOneCounter(baseConfig());

        assertThat(unauthorizedCount.get())
                .as("server must have rejected at least one unauthenticated export")
                .isGreaterThanOrEqualTo(1);
        assertThat(acceptedCount.get())
                .as("no export can succeed without the Authorization header")
                .isZero();
    }

    @Test
    void afterFixWithHeadersExportIsAcceptedWithExactHeaders() {
        MetricConfig config = baseConfig();
        // percent-encoded per OTEL_EXPORTER_OTLP_HEADERS semantics: %20 = space
        config.setProperty(
                "exporter.http-headers",
                "Authorization=Basic%20dXNlcjpwYXNz," + API_KEY_HEADER + "=" + EXPECTED_API_KEY);

        reportOneCounter(config);

        assertThat(acceptedCount.get())
                .as("authenticated export must reach the receiver")
                .isGreaterThanOrEqualTo(1);
        Map<String, List<String>> headers = acceptedRequestHeaders.peek();
        assertThat(headers).isNotNull();
        assertThat(headers.get("Authorization")).containsExactly(EXPECTED_AUTH);
        assertThat(headers.get(API_KEY_HEADER)).containsExactly(EXPECTED_API_KEY);
    }

    @Test
    void afterFixWrongCredentialsAreStillRejected() {
        // guards against a fake pass: the server must actually validate values,
        // not just the presence of the headers
        MetricConfig config = baseConfig();
        config.setProperty(
                "exporter.http-headers",
                "Authorization=Basic%20d3Jvbmc6d3Jvbmc=," + API_KEY_HEADER + "=wrong");

        reportOneCounter(config);

        assertThat(acceptedCount.get()).isZero();
        assertThat(unauthorizedCount.get()).isGreaterThanOrEqualTo(1);
    }

    static class EndToEndTestMetricGroup extends UnregisteredMetricsGroup
            implements LogicalScopeProvider {

        @Override
        public String getLogicalScope(CharacterFilter characterFilter) {
            return "e2e.scope";
        }

        @Override
        public String getLogicalScope(CharacterFilter characterFilter, char c) {
            return "e2e.scope";
        }

        @Override
        public MetricGroup getWrappedMetricGroup() {
            return this;
        }
    }
}
