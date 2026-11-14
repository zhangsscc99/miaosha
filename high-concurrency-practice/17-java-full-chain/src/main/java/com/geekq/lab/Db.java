package com.geekq.lab;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * 实验用 DAO：真实的 MySQL 9.6 + HikariCP。
 * v0 走 "UPDATE ... WHERE stock>0" + 同步 INSERT；v1 只做同步 INSERT；v2/v3 由批量消费者落库。
 */
public class Db implements AutoCloseable {

    private final HikariDataSource ds;

    public Db() {
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(System.getProperty("lab.mysql.url",
                "jdbc:mysql://127.0.0.1:3306/miaosha_lab?useSSL=false&serverTimezone=UTC"
                        + "&rewriteBatchedStatements=true&characterEncoding=utf8&allowPublicKeyRetrieval=true"));
        c.setUsername(System.getProperty("lab.mysql.user", "root"));
        c.setPassword(System.getProperty("lab.mysql.password", "root"));
        int pool = Integer.getInteger("lab.db.pool", 32);
        c.setMaximumPoolSize(pool);
        c.setMinimumIdle(pool);
        c.setConnectionTimeout(3000);
        c.setPoolName("lab-pool");
        ds = new HikariDataSource(c);
    }

    /** v0：DB 行锁串行扣减 + 同步写订单，一个事务。 */
    public int deductAndCreateOrder(long goodsId, long userId, String orderNo) throws SQLException {
        try (Connection cn = ds.getConnection()) {
            cn.setAutoCommit(false);
            try {
                int updated;
                try (PreparedStatement ps = cn.prepareStatement(
                        "UPDATE hc_stock SET stock = stock - 1 WHERE goods_id = ? AND stock > 0")) {
                    ps.setLong(1, goodsId);
                    updated = ps.executeUpdate();
                }
                if (updated == 0) {
                    cn.rollback();
                    return 0;
                }
                try (PreparedStatement ps = cn.prepareStatement(
                        "INSERT INTO hc_order(order_no, user_id, goods_id, status, create_time) VALUES(?,?,?,0,NOW(3))")) {
                    ps.setString(1, orderNo);
                    ps.setLong(2, userId);
                    ps.setLong(3, goodsId);
                    ps.executeUpdate();
                }
                cn.commit();
                return 1;
            } catch (SQLException e) {
                cn.rollback();
                throw e;
            }
        }
    }

    /** v1：Redis 扣减成功后同步写一条订单。 */
    public void insertOrder(long goodsId, long userId, String orderNo) throws SQLException {
        try (Connection cn = ds.getConnection();
             PreparedStatement ps = cn.prepareStatement(
                     "INSERT INTO hc_order(order_no, user_id, goods_id, status, create_time) VALUES(?,?,?,0,NOW(3))")) {
            ps.setString(1, orderNo);
            ps.setLong(2, userId);
            ps.setLong(3, goodsId);
            ps.executeUpdate();
        }
    }

    /** v2/v3：MQ 消费者按批落库，一条 SQL 插入多行。 */
    public int insertOrders(List<OrderRow> rows) throws SQLException {
        if (rows.isEmpty()) {
            return 0;
        }
        try (Connection cn = ds.getConnection()) {
            cn.setAutoCommit(false);
            try (PreparedStatement ps = cn.prepareStatement(
                    "INSERT INTO hc_order(order_no, user_id, goods_id, status, create_time) VALUES(?,?,?,0,?)")) {
                Timestamp now = new Timestamp(System.currentTimeMillis());
                for (OrderRow r : rows) {
                    ps.setString(1, r.orderNo());
                    ps.setLong(2, r.userId());
                    ps.setLong(3, r.goodsId());
                    ps.setTimestamp(4, now);
                    ps.addBatch();
                }
                int[] res = ps.executeBatch();
                cn.commit();
                return res.length;
            } catch (SQLException e) {
                cn.rollback();
                throw e;
            }
        }
    }

    public void reset(long goodsId, int stock) throws SQLException {
        try (Connection cn = ds.getConnection()) {
            cn.setAutoCommit(false);
            try (PreparedStatement del = cn.prepareStatement("DELETE FROM hc_order WHERE goods_id = ?")) {
                del.setLong(1, goodsId);
                del.executeUpdate();
            }
            try (PreparedStatement up = cn.prepareStatement(
                    "INSERT INTO hc_stock(goods_id, stock) VALUES(?,?) ON DUPLICATE KEY UPDATE stock = VALUES(stock)")) {
                up.setLong(1, goodsId);
                up.setInt(2, stock);
                up.executeUpdate();
            }
            cn.commit();
        }
    }

    public int stock(long goodsId) throws SQLException {
        try (Connection cn = ds.getConnection();
             PreparedStatement ps = cn.prepareStatement("SELECT stock FROM hc_stock WHERE goods_id = ?")) {
            ps.setLong(1, goodsId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : -1;
            }
        }
    }

    /** 对账：活动结束后把 Redis / 本地缓存的真实余量回写到 DB，供财务与库存系统核对。 */
    public void updateStock(long goodsId, int stock) throws SQLException {
        try (Connection cn = ds.getConnection();
             PreparedStatement ps = cn.prepareStatement(
                     "INSERT INTO hc_stock(goods_id, stock) VALUES(?,?) ON DUPLICATE KEY UPDATE stock = VALUES(stock)")) {
            ps.setLong(1, goodsId);
            ps.setInt(2, Math.max(0, stock));
            ps.executeUpdate();
        }
    }

    public long orderCount(long goodsId) throws SQLException {
        try (Connection cn = ds.getConnection();
             PreparedStatement ps = cn.prepareStatement("SELECT COUNT(*) FROM hc_order WHERE goods_id = ?")) {
            ps.setLong(1, goodsId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public void ping() throws SQLException {
        try (Connection cn = ds.getConnection()) {
            cn.isValid(2);
        }
    }

    @Override
    public void close() {
        ds.close();
    }
}
