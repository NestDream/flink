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

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.metrics.MetricConfig;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OpenTelemetryReporterOptions#tryConfigureHeaders}. */
class OpenTelemetryReporterOptionsHeadersTest {

    private static Map<String, String> collect(MetricConfig config) {
        Map<String, String> out = new LinkedHashMap<>();
        OpenTelemetryReporterOptions.tryConfigureHeaders(config, out::put);
        return out;
    }

    @Test
    void testNoHeadersConfiguredAddsNothing() {
        assertThat(collect(new MetricConfig())).isEmpty();
    }

    @Test
    void testSingleHeader() {
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "X-Api-Key=abc123");
        assertThat(collect(config)).containsExactly(Map.entry("X-Api-Key", "abc123"));
    }

    @Test
    void testMultipleHeadersPreserveOrder() {
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "DD-API-KEY=k1,X-Custom=v2");
        assertThat(collect(config))
                .containsExactly(Map.entry("DD-API-KEY", "k1"), Map.entry("X-Custom", "v2"));
    }

    @Test
    void testPercentEncodedValueIsDecoded() {
        // OTEL_EXPORTER_OTLP_HEADERS semantics: values are percent-encoded,
        // so "Basic dXNlcjpwYXNz" is written as Basic%20dXNlcjpwYXNz
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "Authorization=Basic%20dXNlcjpwYXNz");
        assertThat(collect(config))
                .containsExactly(Map.entry("Authorization", "Basic dXNlcjpwYXNz"));
    }

    @Test
    void testWhitespaceAroundEntriesIsTolerated() {
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", " A=1 , B=2 ");
        assertThat(collect(config)).containsExactly(Map.entry("A", "1"), Map.entry("B", "2"));
    }

    @Test
    void testEntryWithoutEqualsSignFailsFast() {
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "secret-blob-without-equals");
        assertThatThrownBy(() -> collect(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exporter.http-headers")
                .hasMessageContaining("#1")
                // entries may hold credentials: the raw content must never be echoed
                // into the exception message (it would end up in ERROR logs)
                .satisfies(
                        t ->
                                assertThat(t.getMessage())
                                        .doesNotContain("secret-blob-without-equals"));
    }

    @Test
    void testEmptyHeaderNameFailsFast() {
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "=value");
        assertThatThrownBy(() -> collect(config)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNonAsciiDecodedValueFailsFastInsteadOfSilentReporterDeath() {
        // If this reached the OTel okhttp sender it would throw at export time and
        // Flink would silently drop the reporter; we fail fast at open() instead.
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "X-Bad=%E2%98%83");
        assertThatThrownBy(() -> collect(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("printable ASCII");
    }

    @Test
    void testEmptyValueEntryIsDroppedMatchingOtelReferenceImplementation() {
        // matches opentelemetry-java's DefaultConfigProperties.getMap, which silently
        // drops entries with empty values instead of sending an empty header
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "X-Empty=,X-Kept=v");
        assertThat(collect(config)).containsExactly(Map.entry("X-Kept", "v"));
    }

    @Test
    void testPlusSignDecodesToSpaceMatchingOtelReferenceImplementation() {
        // URLDecoder treats '+' as a space (x-www-form-urlencoded semantics). The
        // opentelemetry-java reference implementation of OTEL_EXPORTER_OTLP_HEADERS
        // (OtlpConfigUtil.configureOtlpHeaders) has the same behavior, so we pin it
        // here deliberately: a literal '+' must be written as %2B.
        MetricConfig config = new MetricConfig();
        config.setProperty("exporter.http-headers", "X-Plus=a+b,X-Encoded-Plus=a%2Bb");
        assertThat(collect(config))
                .containsExactly(Map.entry("X-Plus", "a b"), Map.entry("X-Encoded-Plus", "a+b"));
    }

    @Test
    void testConfigKeyIsRedactedByGlobalConfigurationSensitiveKeys() {
        // The key name deliberately contains "http-headers", which is a substring in
        // GlobalConfiguration.SENSITIVE_KEYS: full-config logging redacts the value.
        String fullKey =
                "metrics.reporter.otel." + OpenTelemetryReporterOptions.EXPORTER_HTTP_HEADERS.key();
        assertThat(GlobalConfiguration.isSensitive(fullKey, java.util.Collections.emptyList()))
                .isTrue();
        assertThat(
                        GlobalConfiguration.isSensitive(
                                OpenTelemetryReporterOptions.EXPORTER_HTTP_HEADERS.key(),
                                java.util.Collections.emptyList()))
                .isTrue();
    }
}
