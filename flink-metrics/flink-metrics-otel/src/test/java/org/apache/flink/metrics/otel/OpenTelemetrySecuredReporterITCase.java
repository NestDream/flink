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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.events.Event;
import org.apache.flink.events.otel.OpenTelemetryEventReporter;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.otel.OpenTelemetryTraceReporter;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end ITCase against a real OpenTelemetry Collector (contrib) that requires bearer-token
 * authentication over TLS on both gRPC (:4317) and HTTP (:4318), and an mTLS client certificate on
 * a third receiver (:4319).
 *
 * <p>Covers the full FLINK-38309 discussion scope: custom headers (auth), TLS trust of a private
 * CA, and mutual TLS. Includes negative cases proving the collector genuinely enforces
 * authentication (no token = rejected) so a passing positive case cannot be a mirage.
 */
@ExtendWith(TestLoggerExtension.class)
public class OpenTelemetrySecuredReporterITCase {

    private static final String TOKEN_HEADER = "Authorization=Bearer%20itcase-secret-token";
    private static final Path SECURED_CONFIG =
            Paths.get("src/test/resources/otel-config-secured.yaml");

    @TempDir static Path outputDir;
    @TempDir static Path tlsDir;

    private static TestTlsMaterial tls;
    private static GenericContainer<?> collector;

    @BeforeAll
    static void startSecuredCollector() throws Exception {
        File logFile = new File(outputDir.toFile(), "logs.json");
        assertThat(logFile.createNewFile()).isTrue();

        // The server certificate must cover whatever host testcontainers exposes:
        // "localhost" locally, a Docker bridge IP (e.g. 172.17.0.1) on containerized CI.
        // We resolve it before the container starts, using the same testcontainers host.
        String host =
                new GenericContainer<>(
                                DockerImageName.parse(
                                        "otel/opentelemetry-collector-contrib:0.111.0"))
                        .getHost();
        String sans = "dns:localhost,dns:host.testcontainers.internal,ip:127.0.0.1";
        if (!"localhost".equals(host)) {
            sans += host.matches("\\d+\\.\\d+\\.\\d+\\.\\d+") ? ",ip:" + host : ",dns:" + host;
        }
        tls = TestTlsMaterial.generate(tlsDir, sans);

        collector =
                new GenericContainer<>(
                                DockerImageName.parse(
                                        "otel/opentelemetry-collector-contrib:0.111.0"))
                        .withExposedPorts(4317, 4318, 4319, 4320)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(SECURED_CONFIG.toString()),
                                "/otel-config.yaml")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(tls.serverPem.toString(), 0644),
                                "/tls/server.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(tls.serverKeyPem.toString(), 0644),
                                "/tls/server-key.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(tls.caPem.toString(), 0644),
                                "/tls/ca.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(logFile.getAbsolutePath(), 0777),
                                "/data/logs.json")
                        .withCommand("--config", "/otel-config.yaml")
                        .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));
        collector.start();
    }

    @AfterAll
    static void stopCollector() {
        if (collector != null) {
            collector.stop();
        }
    }

    private static String endpoint(int port) {
        return String.format("https://%s:%d", collector.getHost(), collector.getMappedPort(port));
    }

    private static MetricConfig securedConfig(int port, String protocol) {
        MetricConfig config = new MetricConfig();
        // the OTLP/HTTP exporter needs the full signal path; gRPC wants host:port only
        String url = "http".equals(protocol) ? endpoint(port) + "/v1/metrics" : endpoint(port);
        config.setProperty("exporter.endpoint", url);
        config.setProperty("exporter.protocol", protocol);
        config.setProperty("exporter.timeout", "10s");
        config.setProperty("exporter.ssl.trusted-certificates", tls.caPem.toString());
        return config;
    }

    /** Exports one counter and returns whether it landed in the collector's file output. */
    private boolean exportAndCheckDelivery(MetricConfig config, String counterName)
            throws Exception {
        OpenTelemetryMetricReporter reporter = new OpenTelemetryMetricReporter();
        reporter.open(config);
        SimpleCounter counter = new SimpleCounter();
        counter.inc(7);
        reporter.notifyOfAddedMetric(counter, counterName, new SecuredTestMetricGroup());
        reporter.report();
        reporter.close();
        return waitForCounterInCollectorOutput(counterName);
    }

    private boolean waitForCounterInCollectorOutput(String counterName) throws Exception {
        return waitForNameInCollectorOutput(counterName, "resourceMetrics");
    }

    private boolean waitForNameInCollectorOutput(String name, String signalKey) throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        ObjectMapper mapper = new ObjectMapper();
        while (deadline.hasTimeLeft()) {
            List<String> lines =
                    collector.copyFileFromContainer(
                            "/data/logs.json",
                            is -> {
                                try (BufferedReader r =
                                        new BufferedReader(
                                                new InputStreamReader(
                                                        is, StandardCharsets.UTF_8))) {
                                    return r.lines().collect(Collectors.toList());
                                }
                            });
            for (String line : new ArrayList<>(lines)) {
                if (line.contains(name)) {
                    // sanity: must be valid otlp-file json of the expected signal
                    JsonNode node = mapper.readTree(line);
                    if (node.has(signalKey)) {
                        return true;
                    }
                }
            }
            Thread.sleep(500);
        }
        return false;
    }

    @Test
    void grpcWithTokenOverTlsIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4317, "grpc");
        config.setProperty("exporter.http-headers", TOKEN_HEADER);
        assertThat(exportAndCheckDelivery(config, "grpcAuthOk"))
                .as("gRPC export with bearer token over TLS must reach the collector")
                .isTrue();
    }

    @Test
    void grpcWithoutTokenIsRejected() throws Exception {
        MetricConfig config = securedConfig(4317, "grpc");
        assertThat(exportAndCheckDelivery(config, "grpcNoAuth"))
                .as("collector must reject gRPC export without the bearer token")
                .isFalse();
    }

    @Test
    void httpWithTokenOverTlsIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4318, "http");
        config.setProperty("exporter.http-headers", TOKEN_HEADER);
        assertThat(exportAndCheckDelivery(config, "httpAuthOk"))
                .as("HTTP export with bearer token over TLS must reach the collector")
                .isTrue();
    }

    @Test
    void httpWithWrongTokenIsRejected() throws Exception {
        MetricConfig config = securedConfig(4318, "http");
        config.setProperty("exporter.http-headers", "Authorization=Bearer%20wrong-token");
        assertThat(exportAndCheckDelivery(config, "httpWrongAuth"))
                .as("collector must reject HTTP export with a wrong bearer token")
                .isFalse();
    }

    @Test
    void grpcMutualTlsWithClientCertIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4319, "grpc");
        config.setProperty("exporter.ssl.client-certificate", tls.clientPem.toString());
        config.setProperty("exporter.ssl.client-key", tls.clientKeyPem.toString());
        assertThat(exportAndCheckDelivery(config, "grpcMtlsOk"))
                .as("gRPC export with mTLS client certificate must reach the collector")
                .isTrue();
    }

    @Test
    void httpMutualTlsWithClientCertIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4320, "http");
        config.setProperty("exporter.ssl.client-certificate", tls.clientPem.toString());
        config.setProperty("exporter.ssl.client-key", tls.clientKeyPem.toString());
        assertThat(exportAndCheckDelivery(config, "httpMtlsOk"))
                .as("HTTP export with mTLS client certificate must reach the collector")
                .isTrue();
    }

    @Test
    void grpcMutualTlsWithoutClientCertIsRejected() throws Exception {
        MetricConfig config = securedConfig(4319, "grpc");
        assertThat(exportAndCheckDelivery(config, "grpcMtlsMissingCert"))
                .as("collector must reject the mTLS port without a client certificate")
                .isFalse();
    }

    private boolean exportSpanAndCheckDelivery(MetricConfig config, String spanName)
            throws Exception {
        OpenTelemetryTraceReporter reporter = new OpenTelemetryTraceReporter();
        reporter.open(config);
        reporter.notifyOfAddedSpan(
                Span.builder(OpenTelemetrySecuredReporterITCase.class, spanName)
                        .setStartTsMillis(System.currentTimeMillis() - 10)
                        .setEndTsMillis(System.currentTimeMillis())
                        .build());
        reporter.close();
        return waitForNameInCollectorOutput(spanName, "resourceSpans");
    }

    private boolean exportEventAndCheckDelivery(MetricConfig config, String eventName)
            throws Exception {
        OpenTelemetryEventReporter reporter = new OpenTelemetryEventReporter();
        reporter.open(config);
        reporter.notifyOfAddedEvent(
                Event.builder(OpenTelemetrySecuredReporterITCase.class, eventName)
                        .setObservedTsMillis(System.currentTimeMillis())
                        .setSeverity("INFO")
                        .setBody("secured e2e")
                        .build());
        reporter.close();
        return waitForNameInCollectorOutput(eventName, "resourceLogs");
    }

    private static String signalPath(String protocol, String signal) {
        return "http".equals(protocol) ? "/v1/" + signal : "";
    }

    @Test
    void traceReporterGrpcWithTokenOverTlsIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4317, "grpc");
        config.setProperty("exporter.http-headers", TOKEN_HEADER);
        assertThat(exportSpanAndCheckDelivery(config, "traceGrpcAuthOk"))
                .as("trace gRPC export with bearer token over TLS must reach the collector")
                .isTrue();
    }

    @Test
    void traceReporterHttpWithTokenOverTlsIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4318, "http");
        config.setProperty("exporter.endpoint", endpoint(4318) + signalPath("http", "traces"));
        config.setProperty("exporter.http-headers", TOKEN_HEADER);
        assertThat(exportSpanAndCheckDelivery(config, "traceHttpAuthOk"))
                .as("trace HTTP export with bearer token over TLS must reach the collector")
                .isTrue();
    }

    @Test
    void eventReporterGrpcWithTokenOverTlsIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4317, "grpc");
        config.setProperty("exporter.http-headers", TOKEN_HEADER);
        assertThat(exportEventAndCheckDelivery(config, "eventGrpcAuthOk"))
                .as("event gRPC export with bearer token over TLS must reach the collector")
                .isTrue();
    }

    @Test
    void eventReporterHttpWithTokenOverTlsIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4318, "http");
        config.setProperty("exporter.endpoint", endpoint(4318) + signalPath("http", "logs"));
        config.setProperty("exporter.http-headers", TOKEN_HEADER);
        assertThat(exportEventAndCheckDelivery(config, "eventHttpAuthOk"))
                .as("event HTTP export with bearer token over TLS must reach the collector")
                .isTrue();
    }

    @Test
    void traceReporterGrpcMutualTlsWithClientCertIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4319, "grpc");
        config.setProperty("exporter.ssl.client-certificate", tls.clientPem.toString());
        config.setProperty("exporter.ssl.client-key", tls.clientKeyPem.toString());
        assertThat(exportSpanAndCheckDelivery(config, "traceGrpcMtlsOk"))
                .as("trace gRPC export with mTLS client certificate must reach the collector")
                .isTrue();
    }

    @Test
    void eventReporterHttpMutualTlsWithClientCertIsDelivered() throws Exception {
        MetricConfig config = securedConfig(4320, "http");
        config.setProperty("exporter.endpoint", endpoint(4320) + signalPath("http", "logs"));
        config.setProperty("exporter.ssl.client-certificate", tls.clientPem.toString());
        config.setProperty("exporter.ssl.client-key", tls.clientKeyPem.toString());
        assertThat(exportEventAndCheckDelivery(config, "eventHttpMtlsOk"))
                .as("event HTTP export with mTLS client certificate must reach the collector")
                .isTrue();
    }

    @Test
    void tlsConfigWithUnreadablePemFailsFastAtOpen() {
        MetricConfig config = securedConfig(4317, "grpc");
        config.setProperty("exporter.ssl.trusted-certificates", "/nonexistent/ca.pem");
        OpenTelemetryMetricReporter reporter = new OpenTelemetryMetricReporter();
        assertThatThrownBy(() -> reporter.open(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot read PEM file");
    }

    @Test
    void clientCertWithoutKeyFailsFastAtOpen() {
        MetricConfig config = securedConfig(4319, "grpc");
        config.setProperty("exporter.ssl.client-certificate", tls.clientPem.toString());
        OpenTelemetryMetricReporter reporter = new OpenTelemetryMetricReporter();
        assertThatThrownBy(() -> reporter.open(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be set together");
    }

    static class SecuredTestMetricGroup extends UnregisteredMetricsGroup
            implements LogicalScopeProvider {

        @Override
        public String getLogicalScope(CharacterFilter characterFilter) {
            return "secured.scope";
        }

        @Override
        public String getLogicalScope(CharacterFilter characterFilter, char c) {
            return "secured.scope";
        }

        @Override
        public MetricGroup getWrappedMetricGroup() {
            return this;
        }
    }
}
