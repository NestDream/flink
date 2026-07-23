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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.util.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Config options for subclasses of {@link OpenTelemetryReporterBase}. */
@PublicEvolving
@Documentation.SuffixOption(ConfigConstants.METRICS_REPORTER_PREFIX + "OpenTelemetry")
public final class OpenTelemetryReporterOptions {

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryReporterOptions.class);

    public enum Protocol {
        gRPC,
        HTTP
    }

    public static final String COMPRESSION_NONE = "none";
    public static final String COMPRESSION_GZIP = "gzip";

    private OpenTelemetryReporterOptions() {}

    public static final ConfigOption<Protocol> EXPORTER_PROTOCOL =
            ConfigOptions.key("exporter.protocol")
                    .enumType(Protocol.class)
                    .defaultValue(Protocol.gRPC)
                    .withDescription(
                            Description.builder()
                                    .text("Protocol for the OpenTelemetry Reporters.")
                                    .build());

    public static final ConfigOption<String> EXPORTER_ENDPOINT =
            ConfigOptions.key("exporter.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Endpoint for the OpenTelemetry Reporters.")
                                    .build());

    public static final ConfigOption<String> EXPORTER_TIMEOUT =
            ConfigOptions.key("exporter.timeout")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Timeout for OpenTelemetry Reporters, as Duration string. Example: 10s for 10 seconds")
                                    .build());

    public static final ConfigOption<String> EXPORTER_COMPRESSION =
            ConfigOptions.key("exporter.compression")
                    .stringType()
                    .defaultValue(COMPRESSION_NONE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            String.format(
                                                    "Compression method for OTel Reporter only '%s' or '%s'. Default is '%s'.",
                                                    COMPRESSION_GZIP,
                                                    COMPRESSION_NONE,
                                                    COMPRESSION_NONE))
                                    .build());

    public static final ConfigOption<String> SERVICE_NAME =
            ConfigOptions.key("service.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("service.name passed to OpenTelemetry Reporters.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<String> SERVICE_VERSION =
            ConfigOptions.key("service.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("service.version passed to OpenTelemetry Reporters.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch.size")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Number of metrics per export batch. "
                                                    + "Values <= 0 disable batching and all metrics are exported in a single request.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<Long> EXPORT_COMPLETION_TIMEOUT_MILLIS =
            ConfigOptions.key("export-completion-timeout-millis")
                    .longType()
                    .defaultValue(300_000L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Timeout in milliseconds for waiting on async export completion.")
                                    .build());

    /** Prefix key used to identify attribute value length limit configuration entries. */
    public static final String ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY =
            "transform.attribute-value-length-limits.";

    /**
     * Config option for attribute value length limits. Only used for documentation purposes — the
     * actual option is prefix-based and parsed by {@link MetricAttributeTransformer}.
     *
     * <p>For example, to limit the {@code task_name} attribute to 60 characters set {@code
     * metrics.reporter.otel.transform.attribute-value-length-limits.task_name: 60} in the Flink
     * configuration. The special key {@code *} defines a global limit for all attributes not
     * explicitly listed. {@code 0} drops an attribute; negative values disable the limit for that
     * attribute (useful to override a global cap).
     */
    @PublicEvolving
    public static final ConfigOption<Integer> ATTRIBUTE_VALUE_LENGTH_LIMITS =
            ConfigOptions.key(ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + "<attribute-name>")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Limits of the exported attribute values length. Only applies to the metric "
                                                    + "reporter; ignored by the trace and event reporters. "
                                                    + "Configuration is prefix based, "
                                                    + "for example to limit `task_name` attribute set "
                                                    + "`metrics.reporter.otel.transform.attribute-value-length-limits.task_name: 60` "
                                                    + "in the config for OTel reporter. "
                                                    + "A special key '*' can be used to define a global limit for all attributes not "
                                                    + "explicitly listed. "
                                                    + "For example `metrics.reporter.otel.transform.attribute-value-length-limits.*: 1024` "
                                                    + "will limit all attributes to attributeValue.substring(0, 1024). "
                                                    + "Global limit defaults to Integer.MAX_VALUE if not set. Individual attribute "
                                                    + "limits always override the global limit and are verified by exact match on the "
                                                    + "attribute name. "
                                                    + "0 can be used to drop an attribute. Negative values are interpreted as no limit "
                                                    + "for the attribute (can be used for global limit overrides).")
                                    .build());

    /** Config key for the collision tracker's maximum size. */
    public static final String COLLISION_TRACKING_MAX_SLOTS_KEY =
            "transform.collision-tracking-max-slots";

    @PublicEvolving
    public static final ConfigOption<Integer> COLLISION_TRACKING_MAX_SLOTS =
            ConfigOptions.key(COLLISION_TRACKING_MAX_SLOTS_KEY)
                    .intType()
                    .defaultValue(50_000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Maximum number of distinct (metric name, transformed attributes) "
                                                    + "slots the truncation-collision tracker retains. Only applies to "
                                                    + "the metric reporter; ignored by the trace and event reporters. "
                                                    + "When the cap is "
                                                    + "reached the least-recently-touched slot is evicted (LRU); a "
                                                    + "previously warned slot that later gets evicted may fire its warning "
                                                    + "again on re-entry. "
                                                    + "Only consulted when attribute-value length limits are configured — "
                                                    + "if no truncation is happening, there is nothing to track and this "
                                                    + "option has no effect. "
                                                    + "Set to 0 to disable collision tracking entirely. Malformed or "
                                                    + "negative values fall back to the default with a warning in the logs.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<String> EXPORTER_HTTP_HEADERS =
            ConfigOptions.key("exporter.http-headers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Additional headers to attach to each OTLP export request, "
                                                    + "as a comma-separated list of key=value pairs following the "
                                                    + "W3C Baggage format used by OTEL_EXPORTER_OTLP_HEADERS. "
                                                    + "Values must be percent-encoded (e.g. a space is %20, "
                                                    + "a literal '+' is %2B). Entries with an empty value are "
                                                    + "ignored; for duplicate header names the last entry wins. "
                                                    + "Example: Authorization=Basic%20dXNlcjpwYXNz,X-Custom=value. "
                                                    + "Applies to both HTTP and gRPC exporters.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<String> EXPORTER_SSL_TRUSTED_CERTIFICATES =
            ConfigOptions.key("exporter.ssl.trusted-certificates")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Path to a PEM file with the certificates to trust for "
                                                    + "the OTLP endpoint (e.g. a private CA). When unset, "
                                                    + "the JVM default trust store is used. Applies to both "
                                                    + "HTTP and gRPC exporters.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<String> EXPORTER_SSL_CLIENT_CERTIFICATE =
            ConfigOptions.key("exporter.ssl.client-certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Path to a PEM file with the client certificate for mutual "
                                                    + "TLS towards the OTLP endpoint. Must be set together "
                                                    + "with exporter.ssl.client-key.")
                                    .build());

    @PublicEvolving
    public static final ConfigOption<String> EXPORTER_SSL_CLIENT_KEY =
            ConfigOptions.key("exporter.ssl.client-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Path to a PEM file (PKCS#8) with the client private key for "
                                                    + "mutual TLS towards the OTLP endpoint. Must be set "
                                                    + "together with exporter.ssl.client-certificate.")
                                    .build());

    /**
     * Parses the {@link #EXPORTER_HTTP_HEADERS} value and feeds each header to the given consumer.
     *
     * <p>The format follows the OTLP exporter specification for {@code OTEL_EXPORTER_OTLP_HEADERS}:
     * a comma-separated list of {@code key=value} pairs, with values percent-encoded (W3C Baggage).
     * Invalid input fails fast with a clear message rather than silently disabling the reporter
     * later.
     */
    @Internal
    public static void tryConfigureHeaders(
            MetricConfig metricConfig, BiConsumer<String, String> addHeader) {
        final String headersConfKey = EXPORTER_HTTP_HEADERS.key();
        if (!metricConfig.containsKey(headersConfKey)) {
            return;
        }
        final String raw = metricConfig.getProperty(headersConfKey).trim();
        if (raw.isEmpty()) {
            return;
        }
        final String endpoint = metricConfig.getProperty(EXPORTER_ENDPOINT.key(), "");
        if (endpoint.toLowerCase().startsWith("http://")) {
            LOG.warn(
                    "{} is configured together with a plaintext http:// endpoint; "
                            + "header values (e.g. credentials) will be sent unencrypted. "
                            + "Use an https:// endpoint to protect them in transit.",
                    headersConfKey);
        }
        for (Map.Entry<String, String> header : parseHeaders(raw).entrySet()) {
            addHeader.accept(header.getKey(), header.getValue());
        }
    }

    /** Parses a W3C Baggage style header list ({@code k1=v1,k2=v2}, values percent-encoded). */
    @Internal
    static Map<String, String> parseHeaders(String raw) {
        final Map<String, String> headers = new LinkedHashMap<>();
        final String[] pairs = raw.split(",");
        for (int i = 0; i < pairs.length; i++) {
            final String trimmed = pairs[i].trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            // error messages identify entries by position, never by content: raw
            // entries may contain credentials and exception messages end up in logs
            final int eq = trimmed.indexOf('=');
            checkArgument(
                    eq > 0,
                    "Invalid header entry #%s in %s: expected key=value pairs separated by commas.",
                    i + 1,
                    EXPORTER_HTTP_HEADERS.key());
            final String key = trimmed.substring(0, eq).trim();
            final String encodedValue = trimmed.substring(eq + 1).trim();
            checkArgument(
                    key.chars().allMatch(c -> c > 0x20 && c < 0x7f && c != ','),
                    "Invalid header name in entry #%s in %s: only printable ASCII without commas is allowed.",
                    i + 1,
                    EXPORTER_HTTP_HEADERS.key());
            final String value;
            try {
                value = URLDecoder.decode(encodedValue, StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid percent-encoding in value for header '%s' in %s.",
                                key, EXPORTER_HTTP_HEADERS.key()));
            }
            checkArgument(
                    value.chars().allMatch(c -> (c >= 0x20 && c < 0x7f) || c == '\t'),
                    "Invalid value for header '%s' in %s: decoded value must be printable ASCII.",
                    key,
                    EXPORTER_HTTP_HEADERS.key());
            if (value.isEmpty()) {
                // matches the opentelemetry-java reference implementation of
                // OTEL_EXPORTER_OTLP_HEADERS, which silently drops entries with empty values
                continue;
            }
            headers.put(key, value);
        }
        return headers;
    }

    /**
     * Configures TLS trust and mutual-TLS client credentials on an OTLP exporter builder.
     *
     * <p>PEM files are read eagerly so that a bad path or unreadable file fails fast at reporter
     * {@code open()} instead of on the first export.
     */
    @Internal
    public static void tryConfigureTls(
            MetricConfig metricConfig,
            Consumer<byte[]> setTrustedCertificates,
            BiConsumer<byte[], byte[]> setClientTls) {
        final String trustedKey = EXPORTER_SSL_TRUSTED_CERTIFICATES.key();
        if (metricConfig.containsKey(trustedKey)) {
            setTrustedCertificates.accept(
                    readPemFile(trustedKey, metricConfig.getProperty(trustedKey)));
        }
        final String certKey = EXPORTER_SSL_CLIENT_CERTIFICATE.key();
        final String keyKey = EXPORTER_SSL_CLIENT_KEY.key();
        final boolean hasCert = metricConfig.containsKey(certKey);
        final boolean hasKey = metricConfig.containsKey(keyKey);
        checkArgument(
                hasCert == hasKey,
                "%s and %s must be set together for mutual TLS.",
                certKey,
                keyKey);
        if (hasCert) {
            final byte[] cert = readPemFile(certKey, metricConfig.getProperty(certKey));
            final byte[] key = readPemFile(keyKey, metricConfig.getProperty(keyKey));
            setClientTls.accept(key, cert);
        }
    }

    private static byte[] readPemFile(String optionKey, String path) {
        try {
            return Files.readAllBytes(Paths.get(path.trim()));
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException(
                    String.format("Cannot read PEM file '%s' configured via %s.", path, optionKey),
                    e);
        }
    }

    @Internal
    public static void tryConfigureTimeout(MetricConfig metricConfig, Consumer<Duration> builder) {
        final String timeoutConfKey = EXPORTER_TIMEOUT.key();
        if (metricConfig.containsKey(timeoutConfKey)) {
            builder.accept(TimeUtils.parseDuration(metricConfig.getProperty(timeoutConfKey)));
        }
    }

    @Internal
    public static void tryConfigureEndpoint(MetricConfig metricConfig, Consumer<String> builder) {
        final String endpointConfKey = EXPORTER_ENDPOINT.key();
        checkArgument(
                metricConfig.containsKey(endpointConfKey), "Must set " + EXPORTER_ENDPOINT.key());
        builder.accept(metricConfig.getProperty(endpointConfKey));
    }

    @Internal
    public static void tryConfigureCompression(
            MetricConfig metricConfig, Consumer<String> builder) {
        final String compressionConfKey = EXPORTER_COMPRESSION.key();
        if (metricConfig.containsKey(compressionConfKey)) {
            String compression = metricConfig.getProperty(compressionConfKey);
            checkArgument(
                    COMPRESSION_NONE.equals(compression) || COMPRESSION_GZIP.equals(compression),
                    "Unsupported compression method: '%s'. Supported values are '%s' and '%s'.",
                    compression,
                    COMPRESSION_NONE,
                    COMPRESSION_GZIP);
            builder.accept(compression);
        }
    }
}
