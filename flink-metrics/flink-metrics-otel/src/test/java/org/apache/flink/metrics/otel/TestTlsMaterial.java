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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;

/**
 * Generates a throwaway self-signed CA plus a server and a client certificate for the secured OTLP
 * ITCase, entirely at test runtime via the JDK's {@code keytool}. Nothing is committed to the
 * repository.
 *
 * <p>The server certificate's subject alternative names are supplied by the caller so the actual
 * container host (which is {@code localhost} locally but a Docker bridge IP such as {@code
 * 172.17.0.1} on containerized CI) is always covered. The files are written as PEM, which is what
 * the OpenTelemetry collector and the reporter both consume.
 */
final class TestTlsMaterial {

    final Path caPem;
    final Path serverPem;
    final Path serverKeyPem;
    final Path clientPem;
    final Path clientKeyPem;

    private TestTlsMaterial(Path caPem, Path s, Path sk, Path c, Path ck) {
        this.caPem = caPem;
        this.serverPem = s;
        this.serverKeyPem = sk;
        this.clientPem = c;
        this.clientKeyPem = ck;
    }

    /**
     * Generates the CA, server and client material into {@code dir}, with {@code sans} on the
     * server cert.
     */
    static TestTlsMaterial generate(Path dir, String sans) throws Exception {
        Path caKs = dir.resolve("ca.p12");
        Path serverKs = dir.resolve("server.p12");
        Path clientKs = dir.resolve("client.p12");
        String pass = "testpass";

        // self-signed CA
        keytool(
                "-genkeypair",
                "-alias",
                "ca",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-dname",
                "CN=flink-otel-test-ca",
                "-validity",
                "3650",
                "-ext",
                "bc:c",
                "-keystore",
                caKs.toString(),
                "-storepass",
                pass,
                "-storetype",
                "PKCS12");

        Path serverKeyPem = issue(dir, "server", "CN=localhost", sans, caKs, pass);
        Path clientKeyPem = issue(dir, "client", "CN=flink-otel-test-client", null, caKs, pass);

        Path caPem = dir.resolve("ca.pem");
        writeCertPem(caKs, "ca", pass, caPem);

        return new TestTlsMaterial(
                caPem,
                dir.resolve("server.pem"),
                serverKeyPem,
                dir.resolve("client.pem"),
                clientKeyPem);
    }

    /**
     * Generates a keypair, signs it with the CA, and writes the leaf cert (+chain) and key as PEM.
     */
    private static Path issue(
            Path dir, String alias, String dname, String sans, Path caKs, String pass)
            throws Exception {
        Path ks = dir.resolve(alias + ".p12");
        java.util.List<String> gen =
                new java.util.ArrayList<>(
                        java.util.Arrays.asList(
                                "-genkeypair",
                                "-alias",
                                alias,
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-dname",
                                dname,
                                "-validity",
                                "3650",
                                "-keystore",
                                ks.toString(),
                                "-storepass",
                                pass,
                                "-storetype",
                                "PKCS12"));
        if (sans != null) {
            gen.add("-ext");
            gen.add("san=" + sans);
        }
        keytool(gen.toArray(new String[0]));

        // CSR -> signed by CA -> import chain back
        Path csr = dir.resolve(alias + ".csr");
        keytool(
                "-certreq",
                "-alias",
                alias,
                "-keystore",
                ks.toString(),
                "-storepass",
                pass,
                "-file",
                csr.toString());
        Path signed = dir.resolve(alias + "-signed.pem");
        java.util.List<String> sign =
                new java.util.ArrayList<>(
                        java.util.Arrays.asList(
                                "-gencert",
                                "-alias",
                                "ca",
                                "-keystore",
                                caKs.toString(),
                                "-storepass",
                                pass,
                                "-infile",
                                csr.toString(),
                                "-outfile",
                                signed.toString(),
                                "-validity",
                                "3650"));
        if (sans != null) {
            sign.add("-ext");
            sign.add("san=" + sans);
        }
        keytool(sign.toArray(new String[0]));
        keytool(
                "-importcert",
                "-alias",
                "ca",
                "-keystore",
                ks.toString(),
                "-storepass",
                pass,
                "-file",
                caPemPathFor(caKs, pass, dir),
                "-noprompt");
        keytool(
                "-importcert",
                "-alias",
                alias,
                "-keystore",
                ks.toString(),
                "-storepass",
                pass,
                "-file",
                signed.toString(),
                "-noprompt");

        writeCertPem(ks, alias, pass, dir.resolve(alias + ".pem"));
        Path keyPem = dir.resolve(alias + "-key.pem");
        writeKeyPem(ks, alias, pass, keyPem);
        return keyPem;
    }

    private static String caPemPathFor(Path caKs, String pass, Path dir) throws Exception {
        Path caPem = dir.resolve("ca.pem");
        if (!Files.exists(caPem)) {
            writeCertPem(caKs, "ca", pass, caPem);
        }
        return caPem.toString();
    }

    private static void writeCertPem(Path ks, String alias, String pass, Path out)
            throws Exception {
        KeyStore keyStore = load(ks, pass);
        StringBuilder sb = new StringBuilder();
        Certificate[] chain = keyStore.getCertificateChain(alias);
        Certificate[] certs =
                chain != null ? chain : new Certificate[] {keyStore.getCertificate(alias)};
        for (Certificate cert : certs) {
            sb.append(pem("CERTIFICATE", cert.getEncoded()));
        }
        Files.write(out, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void writeKeyPem(Path ks, String alias, String pass, Path out) throws Exception {
        KeyStore keyStore = load(ks, pass);
        PrivateKey key = (PrivateKey) keyStore.getKey(alias, pass.toCharArray());
        Files.write(out, pem("PRIVATE KEY", key.getEncoded()).getBytes(StandardCharsets.UTF_8));
    }

    private static KeyStore load(Path ks, String pass) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(ks)) {
            keyStore.load(in, pass.toCharArray());
        }
        return keyStore;
    }

    private static String pem(String type, byte[] der) {
        String b64 =
                Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8))
                        .encodeToString(der);
        return "-----BEGIN " + type + "-----\n" + b64 + "\n-----END " + type + "-----\n";
    }

    private static void keytool(String... args) throws Exception {
        java.util.List<String> cmd = new java.util.ArrayList<>();
        String javaHome = System.getProperty("java.home");
        cmd.add(javaHome + "/bin/keytool");
        cmd.addAll(java.util.Arrays.asList(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = p.getInputStream()) {
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
        }
        int code = p.waitFor();
        if (code != 0) {
            throw new IllegalStateException(
                    "keytool failed (" + code + "): " + out.toString(StandardCharsets.UTF_8));
        }
    }
}
