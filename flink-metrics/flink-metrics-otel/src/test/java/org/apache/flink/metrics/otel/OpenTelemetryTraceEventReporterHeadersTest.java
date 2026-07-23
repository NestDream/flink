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

import org.apache.flink.events.Event;
import org.apache.flink.events.otel.OpenTelemetryEventReporter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.otel.OpenTelemetryTraceReporter;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the trace and event reporters attach configured {@code exporter.http-headers} to
 * their OTLP/HTTP export requests, mirroring the metrics-reporter coverage in {@link
 * OpenTelemetryMetricReporterAuthTest}. Guards the wiring in {@code OpenTelemetryTraceReporter} and
 * {@code OpenTelemetryEventReporter}: commenting out either tryConfigureHeaders call turns the
 * corresponding test red.
 */
class OpenTelemetryTraceEventReporterHeadersTest {

    private static final String EXPECTED_AUTH = "Bearer trace-event-secret";

    private HttpServer server;
    private final ConcurrentHashMap<String, AtomicInteger> authorizedByPath =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> unauthorizedByPath =
            new ConcurrentHashMap<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        for (String path : new String[] {"/v1/traces", "/v1/logs"}) {
            server.createContext(
                    path,
                    exchange -> {
                        String auth = exchange.getRequestHeaders().getFirst("Authorization");
                        if (EXPECTED_AUTH.equals(auth)) {
                            authorizedByPath
                                    .computeIfAbsent(path, k -> new AtomicInteger())
                                    .incrementAndGet();
                            exchange.getResponseHeaders()
                                    .add("Content-Type", "application/x-protobuf");
                            exchange.sendResponseHeaders(200, -1);
                        } else {
                            unauthorizedByPath
                                    .computeIfAbsent(path, k -> new AtomicInteger())
                                    .incrementAndGet();
                            byte[] body = "unauthorized".getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(401, body.length);
                            try (OutputStream os = exchange.getResponseBody()) {
                                os.write(body);
                            }
                        }
                        exchange.close();
                    });
        }
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private MetricConfig configFor(String signalPath) {
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.protocol", "http");
        config.setProperty(
                "exporter.endpoint",
                String.format("http://127.0.0.1:%d%s", server.getAddress().getPort(), signalPath));
        config.setProperty("exporter.timeout", "10s");
        config.setProperty("exporter.http-headers", "Authorization=Bearer%20trace-event-secret");
        return config;
    }

    @Test
    void traceReporterSendsConfiguredHeaders() {
        OpenTelemetryTraceReporter reporter = new OpenTelemetryTraceReporter();
        reporter.open(configFor("/v1/traces"));
        reporter.notifyOfAddedSpan(
                Span.builder(OpenTelemetryTraceEventReporterHeadersTest.class, "testSpan")
                        .setStartTsMillis(System.currentTimeMillis() - 10)
                        .setEndTsMillis(System.currentTimeMillis())
                        .build());
        reporter.close();

        assertThat(authorizedByPath.getOrDefault("/v1/traces", new AtomicInteger()).get())
                .as("trace export must carry the configured Authorization header")
                .isGreaterThanOrEqualTo(1);
        assertThat(unauthorizedByPath.getOrDefault("/v1/traces", new AtomicInteger()).get())
                .isZero();
    }

    @Test
    void eventReporterSendsConfiguredHeaders() {
        OpenTelemetryEventReporter reporter = new OpenTelemetryEventReporter();
        reporter.open(configFor("/v1/logs"));
        reporter.notifyOfAddedEvent(
                Event.builder(OpenTelemetryTraceEventReporterHeadersTest.class, "testEvent")
                        .setObservedTsMillis(System.currentTimeMillis())
                        .setSeverity("INFO")
                        .setBody("headers e2e")
                        .build());
        reporter.close();

        assertThat(authorizedByPath.getOrDefault("/v1/logs", new AtomicInteger()).get())
                .as("event export must carry the configured Authorization header")
                .isGreaterThanOrEqualTo(1);
        assertThat(unauthorizedByPath.getOrDefault("/v1/logs", new AtomicInteger()).get()).isZero();
    }
}
