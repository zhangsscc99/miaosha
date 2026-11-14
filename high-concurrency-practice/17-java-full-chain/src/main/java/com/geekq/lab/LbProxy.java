package com.geekq.lab;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 极简轮询负载均衡器：用 3 个 JVM 进程模拟 K8s 里的 3 个 Pod + Service/Ingress。
 * 只做实验用，不具备生产网关的 TLS、健康检查、重试等能力。
 */
public class LbProxy {

    public static void main(String[] args) throws Exception {
        String backendsArg = arg(args, "--backends", "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083");
        int port = Integer.parseInt(arg(args, "--port", "18090"));
        List<String> backends = new ArrayList<>(List.of(backendsArg.split(",")));
        AtomicInteger rr = new AtomicInteger();
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 4096);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", exchange -> {
            String backend = backends.get(Math.floorMod(rr.getAndIncrement(), backends.size()));
            URI in = exchange.getRequestURI();
            String target = backend + in.getPath() + (in.getRawQuery() == null ? "" : "?" + in.getRawQuery());
            try {
                HttpResponse<byte[]> resp = client.send(
                        HttpRequest.newBuilder(URI.create(target)).GET().timeout(Duration.ofSeconds(5)).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                byte[] body = resp.body();
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(resp.statusCode(), body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } catch (Exception e) {
                byte[] body = "{\"code\":502}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(502, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        System.out.printf("[lb] port=%d backends=%s pid=%d%n", port, backends, ProcessHandle.current().pid());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(0)));
    }

    private static String arg(String[] args, String name, String def) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(name) && i + 1 < args.length) {
                return args[i + 1];
            }
            if (args[i].startsWith(name + "=")) {
                return args[i].substring(name.length() + 1);
            }
        }
        return def;
    }
}
