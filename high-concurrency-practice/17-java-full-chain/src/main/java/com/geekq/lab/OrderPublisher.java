package com.geekq.lab;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 订单异步管道：
 *   请求线程 --(有界队列，满了就拒绝)--> 单线程 publisher --(publisher confirm)--> RabbitMQ
 *   RabbitMQ --(prefetch 2000)--> 批量消费者 --(每 200 条或 15ms 一次)--> MySQL
 *
 * 有界队列是"削峰"的闸门：它把突发流量变成稳定的 DB 写入速率，代价是请求延迟变长（排队），
 * 所以入口必须限流并给用户返回"排队中"。
 */
public class OrderPublisher implements AutoCloseable {

    private static final int PUBLISH_BATCH = 200;
    private static final int DB_BATCH = 200;

    private final Db db;
    private final String queue;
    private final Connection connection;
    private final Channel pubChannel;
    private final Channel consumeChannel;

    private final BlockingQueue<OrderRow> outbound;
    private final BlockingQueue<Delivery> inbound;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread publishThread;
    private final Thread flushThread;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong confirmed = new AtomicLong();
    private final AtomicLong persisted = new AtomicLong();
    private final AtomicLong publishFailed = new AtomicLong();
    private final AtomicLong persistFailed = new AtomicLong();

    private record Pending(long tag, OrderRow row) {
    }

    public OrderPublisher(Db db) throws Exception {
        this.db = db;
        this.queue = System.getProperty("lab.mq.queue", "hc_order_queue");
        this.outbound = new ArrayBlockingQueue<>(Integer.getInteger("lab.mq.outbound", 200_000));
        this.inbound = new ArrayBlockingQueue<>(Integer.getInteger("lab.mq.inbound", 100_000));

        ConnectionFactory f = new ConnectionFactory();
        f.setHost(System.getProperty("lab.mq.host", "127.0.0.1"));
        f.setPort(Integer.getInteger("lab.mq.port", 5672));
        f.setUsername(System.getProperty("lab.mq.user", "guest"));
        f.setPassword(System.getProperty("lab.mq.password", "guest"));
        f.setAutomaticRecoveryEnabled(true);
        f.setRequestedHeartbeat(30);
        this.connection = f.newConnection("hc-lab");

        this.pubChannel = connection.createChannel();
        this.consumeChannel = connection.createChannel();
        pubChannel.queueDeclare(queue, true, false, false, null);
        // 每次启动清掉上一轮残留，保证实验数据干净
        pubChannel.queuePurge(queue);
        pubChannel.confirmSelect();
        consumeChannel.queueDeclare(queue, true, false, false, null);
        consumeChannel.basicQos(2000);
        consumeChannel.basicConsume(queue, false, (tag, delivery) -> {
            try {
                inbound.put(delivery); // 满了就阻塞消费线程，形成端到端背压
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, tag -> System.out.println("[mq] consume cancelled: " + tag));

        this.publishThread = new Thread(this::publishLoop, "mq-publisher");
        this.flushThread = new Thread(this::flushLoop, "mq-batch-writer");
        publishThread.setDaemon(true);
        flushThread.setDaemon(true);
        publishThread.start();
        flushThread.start();
        System.out.println("[mq] pipeline started, queue=" + queue);
    }

    /** 请求线程调用：只入内存队列，不等待 broker 确认。 */
    public boolean enqueue(OrderRow row) {
        return outbound.offer(row);
    }

    private void publishLoop() {
        List<OrderRow> buf = new ArrayList<>(PUBLISH_BATCH);
        while (running.get() || !outbound.isEmpty()) {
            try {
                OrderRow first = outbound.poll(20, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                buf.add(first);
                outbound.drainTo(buf, PUBLISH_BATCH - 1);
                publishBatch(buf);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                publishFailed.addAndGet(buf.size());
                System.out.println("[mq] publish failed: " + e);
            } finally {
                buf.clear();
            }
        }
    }

    private void publishBatch(List<OrderRow> buf) throws Exception {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .deliveryMode(2) // persistent
                .contentType("text/plain")
                .build();
        for (OrderRow row : buf) {
            pubChannel.basicPublish("", queue, props, encode(row));
        }
        pubChannel.waitForConfirmsOrDie(5000);
        published.addAndGet(buf.size());
        confirmed.addAndGet(buf.size());
    }

    private void flushLoop() {
        List<Delivery> batch = new ArrayList<>(DB_BATCH);
        while (running.get() || !inbound.isEmpty()) {
            try {
                Delivery first = inbound.poll(20, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                inbound.drainTo(batch, DB_BATCH - 1);
                flushBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                persistFailed.addAndGet(batch.size());
                System.out.println("[mq] persist failed: " + e);
                try {
                    long maxTag = batch.get(batch.size() - 1).getEnvelope().getDeliveryTag();
                    consumeChannel.basicNack(maxTag, true, true);
                } catch (Exception ignore) {
                    // 通道断开时由自动恢复机制接管
                }
            } finally {
                batch.clear();
            }
        }
    }

    private void flushBatch(List<Delivery> batch) throws Exception {
        List<OrderRow> rows = new ArrayList<>(batch.size());
        for (Delivery d : batch) {
            rows.add(decode(d));
        }
        db.insertOrders(rows);
        long maxTag = batch.get(batch.size() - 1).getEnvelope().getDeliveryTag();
        consumeChannel.basicAck(maxTag, true);
        persisted.addAndGet(rows.size());
    }

    private static byte[] encode(OrderRow r) {
        return (r.orderNo() + "|" + r.userId() + "|" + r.goodsId()).getBytes(StandardCharsets.UTF_8);
    }

    private static OrderRow decode(Delivery d) {
        String s = new String(d.getBody(), StandardCharsets.UTF_8);
        String[] p = s.split("\\|");
        return new OrderRow(p[0], Long.parseLong(p[1]), Long.parseLong(p[2]));
    }

    public long pending() {
        return outbound.size() + inbound.size();
    }

    public String statsJson() {
        return "{\"outbound\":" + outbound.size()
                + ",\"inbound\":" + inbound.size()
                + ",\"published\":" + published.get()
                + ",\"confirmed\":" + confirmed.get()
                + ",\"persisted\":" + persisted.get()
                + ",\"publishFailed\":" + publishFailed.get()
                + ",\"persistFailed\":" + persistFailed.get() + "}";
    }

    @Override
    public void close() {
        running.set(false);
        try {
            publishThread.join(5000);
            flushThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            if (pubChannel.isOpen()) {
                pubChannel.close();
            }
            if (consumeChannel.isOpen()) {
                consumeChannel.close();
            }
            connection.close();
        } catch (Exception ignore) {
            // shutting down
        }
    }
}
