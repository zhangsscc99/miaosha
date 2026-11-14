package com.geekq.lab.edge;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 秒杀商品详情页的服务端，两种形态：
 *
 *   /dyn?id=N   完整详情页（约 8KB HTML）：一次 Redis GET 拿商品快照，然后在进程内拼模板。
 *               这就是「没有静态化」时每个请求都要付的 CPU。
 *   /api?id=N   2KB JSON：静态页面 + AJAX 拿库存/倒计时的那一半。
 *   /stats      本进程的处理计数，用来算 nginx 到底把多少请求挡在了上游之外。
 *
 * 页面大小刻意做成和真实移动端商详页一个量级（8KB 左右），这样后面的带宽账才有意义。
 */
public final class EdgeServer {

    static int PORT = 18090;
    static int REDIS_PORT = 6379;
    static int GOODS = 1000;
    /** 注入的渲染耗时（毫秒）。用来模拟真实商详页在回源时占住一个请求的时间。 */
    static long DELAY_MS = 0;

    static final AtomicLong PAGE_REQ = new AtomicLong();
    static final AtomicLong API_REQ = new AtomicLong();
    static final AtomicLong REDIS_CMDS = new AtomicLong();
    static final AtomicLong ERRORS = new AtomicLong();

    static JedisPool pool;
    static String pagePrefix;
    static String pageSuffix;
    static String specRows;

    public static void main(String[] args) throws Exception {
        PORT = Integer.parseInt(arg(args, "--port", "18090"));
        REDIS_PORT = Integer.parseInt(arg(args, "--redisPort", "6379"));
        GOODS = Integer.parseInt(arg(args, "--goods", "1000"));
        DELAY_MS = Long.parseLong(arg(args, "--delayMs", "0"));

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(64);
        cfg.setMaxIdle(64);
        pool = new JedisPool(cfg, "127.0.0.1", REDIS_PORT, 2000);

        pagePrefix = buildPrefix();
        pageSuffix = buildSuffix();
        StringBuilder rows = new StringBuilder();
        for (int i = 1; i <= 40; i++) {
            rows.append("<tr><td class=\"k\">参数 ").append(i).append("</td><td class=\"v\">")
                    .append("规格值-").append(i).append(" / 批次 ").append(2026 + (i % 2))
                    .append(" / 产地 杭州</td></tr>");
        }
        specRows = rows.toString();

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 4096);
        ExecutorService poolExec = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(poolExec);

        server.createContext("/dyn", ex -> handle(ex, true));
        server.createContext("/api", ex -> handle(ex, false));
        server.createContext("/health", ex -> {
            byte[] b = "ok".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        });
        server.createContext("/delay", ex -> {
            String q = ex.getRequestURI().getRawQuery();
            DELAY_MS = q == null ? 0 : Long.parseLong(q.replace("ms=", ""));
            byte[] b = ("delay=" + DELAY_MS).getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        });
        server.createContext("/stats", ex -> {
            String json = "{\"page\":" + PAGE_REQ.get() + ",\"api\":" + API_REQ.get()
                    + ",\"redis\":" + REDIS_CMDS.get() + ",\"errors\":" + ERRORS.get() + "}";
            byte[] b = json.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        });

        server.start();
        System.out.println("edge server on :" + PORT + " goods=" + GOODS
                + " pageBytes=" + renderPage(1, "{}").getBytes(StandardCharsets.UTF_8).length);
    }

    private static void handle(HttpExchange ex, boolean page) throws IOException {
        long t0 = System.nanoTime();
        String path = ex.getRequestURI().getPath();
        if (!path.equals("/dyn") && !path.equals("/api")) {
            ex.sendResponseHeaders(404, -1);
            ex.close();
            return;
        }
        int id = parseId(ex.getRequestURI().getRawQuery());
        if (page && DELAY_MS > 0) {
            try {
                Thread.sleep(DELAY_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        String itemJson = null;
        try (Jedis j = pool.getResource()) {
            itemJson = j.get("item:" + id);
            REDIS_CMDS.incrementAndGet();
        } catch (Exception e) {
            ERRORS.incrementAndGet();
        }
        if (itemJson == null) {
            byte[] b = "not found".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(404, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
            return;
        }

        byte[] body;
        String ctype;
        if (page) {
            body = renderPage(id, itemJson).getBytes(StandardCharsets.UTF_8);
            ctype = "text/html; charset=utf-8";
            PAGE_REQ.incrementAndGet();
        } else {
            body = renderApi(id, itemJson).getBytes(StandardCharsets.UTF_8);
            ctype = "application/json; charset=utf-8";
            API_REQ.incrementAndGet();
        }
        ex.getResponseHeaders().set("Content-Type", ctype);
        ex.getResponseHeaders().set("X-Resp-Us", Long.toString((System.nanoTime() - t0) / 1000));
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private static String renderPage(int id, String itemJson) {
        String[] parts = splitItem(itemJson);
        String title = parts[0];
        String price = parts[1];
        String stock = parts[2];
        int remaining = Integer.parseInt(stock);
        StringBuilder sb = new StringBuilder(9000);
        sb.append(pagePrefix);
        sb.append("<h1 id=\"title\">").append(title).append("</h1>");
        sb.append("<div class=\"price\"><span class=\"now\">￥").append(price).append("</span>")
                .append("<span class=\"was\">￥").append(origPrice(price)).append("</span>")
                .append("<span class=\"tip\">限时秒杀</span></div>");
        sb.append("<div class=\"stock\">剩余 <b id=\"stock\">").append(remaining)
                .append("</b> 件 · 已抢 ").append(88000 - remaining).append(" 件</div>");
        sb.append("<div class=\"countdown\">距结束 <b id=\"cd\">00:59:59</b></div>");
        sb.append("<button id=\"buy\" onclick=\"buy()\">立即抢购</button>");
        sb.append("<div class=\"spec\"><table><tbody>").append(specRows).append("</tbody></table></div>");
        sb.append("<div class=\"reviews\">");
        for (int i = 1; i <= 10; i++) {
            sb.append("<div class=\"r\"><span class=\"u\">用户").append(1000 + id * 7 + i)
                    .append("</span><p>质量不错，物流很快，第 ").append(i)
                    .append(" 次回购了，价格比平时便宜不少。</p></div>");
        }
        sb.append("</div>");
        sb.append(pageSuffix);
        return sb.toString();
    }

    private static String renderApi(int id, String itemJson) {
        String[] parts = splitItem(itemJson);
        StringBuilder sb = new StringBuilder(2300);
        sb.append("{\"id\":").append(id)
                .append(",\"title\":\"").append(parts[0].replace("\"", "\\\"")).append("\"")
                .append(",\"price\":").append(parts[1])
                .append(",\"stock\":").append(parts[2])
                .append(",\"serverTime\":").append(System.currentTimeMillis())
                .append(",\"endTime\":").append(System.currentTimeMillis() + 3_600_000L)
                .append(",\"soldOut\":").append(Integer.parseInt(parts[2]) <= 0)
                .append(",\"tags\":[\"限时秒杀\",\"包邮\",\"7天无理由\",\"正品保障\",\"极速退款\"]")
                .append(",\"coupons\":[{\"id\":1,\"amount\":20,\"threshold\":199},")
                .append("{\"id\":2,\"amount\":50,\"threshold\":499}],")
                .append("\"skus\":[");
        for (int i = 1; i <= 12; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append("{\"skuId\":").append(id * 100 + i)
                    .append(",\"name\":\"颜色-").append(i).append(" / 尺码 M\"")
                    .append(",\"price\":").append(parts[1])
                    .append(",\"stock\":").append(Math.max(0, Integer.parseInt(parts[2]) - i * 3)).append('}');
        }
        sb.append("],\"services\":[\"48小时发货\",\"运费险\",\"上门取件\",\"价保30天\"],");
        sb.append("\"desc\":\"");
        for (int i = 0; i < 12; i++) {
            sb.append("第").append(i + 1).append("段商品描述：面料透气，做工扎实，适合日常通勤和运动场景。");
        }
        sb.append("\"}");
        return sb.toString();
    }

    private static String[] splitItem(String json) {
        // 商品快照格式固定：{"title":"...","price":"...","stock":N}
        String title = between(json, "\"title\":\"", "\"");
        String price = between(json, "\"price\":\"", "\"");
        String stock = between(json, "\"stock\":", "}");
        return new String[]{title, price, stock};
    }

    private static String between(String s, String a, String b) {
        int i = s.indexOf(a);
        if (i < 0) {
            return "0";
        }
        int j = s.indexOf(b, i + a.length());
        if (j < 0) {
            return "0";
        }
        return s.substring(i + a.length(), j);
    }

    private static String origPrice(String price) {
        try {
            return String.format("%.2f", Double.parseDouble(price) * 2.4);
        } catch (Exception e) {
            return price;
        }
    }

    private static String buildPrefix() {
        return """
                <!DOCTYPE html>
                <html lang="zh-CN"><head><meta charset="utf-8">
                <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
                <title>限时秒杀 - 商品详情</title>
                <style>
                *{margin:0;padding:0;box-sizing:border-box}
                body{font-family:-apple-system,"PingFang SC",sans-serif;background:#f5f5f5;color:#222;font-size:14px}
                header{height:44px;line-height:44px;text-align:center;background:#e4393c;color:#fff;font-size:16px}
                .gallery{height:180px;background:linear-gradient(135deg,#ffe6e6,#ffd0d0);display:flex;align-items:center;justify-content:center;color:#c33}
                .box{background:#fff;margin:8px;border-radius:8px;padding:12px}
                h1{font-size:17px;line-height:1.45;font-weight:600}
                .price{margin-top:8px}
                .now{color:#e4393c;font-size:26px;font-weight:700}
                .was{color:#999;text-decoration:line-through;margin-left:8px;font-size:12px}
                .tip{background:#e4393c;color:#fff;font-size:11px;padding:2px 6px;border-radius:3px;margin-left:8px}
                .stock{margin-top:8px;color:#666}
                .stock b{color:#e4393c;font-size:18px}
                .countdown{margin-top:6px;color:#e4393c;font-size:13px}
                #buy{width:100%;height:46px;margin-top:12px;border:0;border-radius:23px;background:linear-gradient(90deg,#ff6a00,#e4393c);color:#fff;font-size:17px;font-weight:600}
                .spec table{width:100%;border-collapse:collapse}
                .spec td{border-bottom:1px solid #f0f0f0;padding:8px 4px;font-size:12px}
                .spec .k{color:#999;width:96px}
                .spec .v{color:#333}
                .reviews .r{border-bottom:1px solid #f5f5f5;padding:8px 0}
                .reviews .u{color:#2b6cb0;font-size:12px}
                .reviews p{color:#555;font-size:12px;line-height:1.6;margin-top:4px}
                footer{text-align:center;color:#aaa;font-size:11px;padding:16px 0 28px}
                </style></head><body>
                <header>限时秒杀</header>
                <div class="gallery"><img src="https://img.example.com/p/placeholder.jpg" alt="商品图" width="140" height="140"></div>
                <div class="box">
                """;
    }

    private static String buildSuffix() {
        return """
                </div>
                <footer>秒杀商品由平台自营发货 · 价格以结算页为准</footer>
                <script>
                function buy(){document.getElementById('buy').disabled=true;document.getElementById('buy').innerText='排队中...';}
                var left=3600;setInterval(function(){left=left>0?left-1:0;var h=Math.floor(left/3600),m=Math.floor(left%3600/60),s=left%60;document.getElementById('cd').innerText=(h<10?'0':'')+h+':'+(m<10?'0':'')+m+':'+(s<10?'0':'')+s;},1000);
                </script>
                </body></html>
                """;
    }

    private static int parseId(String query) {
        if (query == null) {
            return 1;
        }
        for (String kv : query.split("&")) {
            if (kv.startsWith("id=")) {
                try {
                    return Integer.parseInt(kv.substring(3));
                } catch (NumberFormatException e) {
                    return 1;
                }
            }
        }
        return 1;
    }

    static String arg(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return def;
    }
}
