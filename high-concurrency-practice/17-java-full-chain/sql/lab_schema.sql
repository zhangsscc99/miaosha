-- 高并发实验库（与业务库 miaosha 隔离，避免污染演示数据）
CREATE DATABASE IF NOT EXISTS miaosha_lab DEFAULT CHARACTER SET utf8mb4;
USE miaosha_lab;

DROP TABLE IF EXISTS hc_order;
DROP TABLE IF EXISTS hc_stock;

CREATE TABLE hc_stock (
    goods_id BIGINT PRIMARY KEY,
    stock    INT NOT NULL
) ENGINE = InnoDB;

CREATE TABLE hc_order (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_no    VARCHAR(40) NOT NULL,
    user_id     BIGINT      NOT NULL,
    goods_id    BIGINT      NOT NULL,
    status      TINYINT     NOT NULL DEFAULT 0,
    create_time DATETIME(3) NOT NULL,
    UNIQUE KEY uk_order_no (order_no),
    KEY idx_goods (goods_id)
) ENGINE = InnoDB;

INSERT INTO hc_stock (goods_id, stock) VALUES (1, 0);
