ALTER TABLE TBC20721.nft_utxo_set
ADD INDEX idx_collection_id_index (collection_id, collection_index);

-- 交易主表
CREATE TABLE transactions (
    Fid BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    tx_hash VARCHAR(64) NOT NULL COMMENT '交易哈希值，唯一标识一笔交易',
    fee DECIMAL(16, 8) NOT NULL COMMENT '交易手续费',
    time_stamp BIGINT COMMENT '交易时间戳，单位秒',
    transaction_utc_time VARCHAR(30) COMMENT '格式化的UTC时间，如：2023-01-01 12:00:00',
    tx_type VARCHAR(10) NOT NULL COMMENT '交易类型，如：P2PKH、TBC20、TBC721、P2MS',
    block_height BIGINT COMMENT '区块高度',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    PRIMARY KEY (Fid),
    UNIQUE KEY idx_tx_hash (tx_hash),
    INDEX idx_time_stamp (time_stamp),
    INDEX idx_tx_type (tx_type),
    INDEX idx_block_height (block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='交易基本信息表';

-- 地址交易关系表
CREATE TABLE address_transactions (
    Fid BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    address VARCHAR(64) NOT NULL COMMENT '比特币地址',
    tx_hash VARCHAR(64) NOT NULL COMMENT '关联的交易哈希',
    is_sender BOOLEAN NOT NULL COMMENT '是否为发送方，1表示是，0表示否',
    is_recipient BOOLEAN NOT NULL COMMENT '是否为接收方，1表示是，0表示否',
    balance_change DECIMAL(16, 8) COMMENT '该地址在此交易中的余额变化',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    PRIMARY KEY (Fid),
    UNIQUE KEY idx_address_tx (address, tx_hash),
    INDEX idx_address (address),
    INDEX idx_tx_hash (tx_hash),
    CONSTRAINT fk_addr_tx_hash FOREIGN KEY (tx_hash) REFERENCES transactions(tx_hash) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='地址与交易关系表';

-- 参与方地址表
CREATE TABLE transaction_participants (
    Fid BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    tx_hash VARCHAR(64) NOT NULL COMMENT '关联的交易哈希',
    address VARCHAR(64) NOT NULL COMMENT '参与方地址',
    role ENUM('sender', 'recipient') NOT NULL COMMENT '参与角色：sender发送方，recipient接收方',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    PRIMARY KEY (Fid),
    INDEX idx_tx_hash (tx_hash),
    INDEX idx_address_role (address, role),
    CONSTRAINT fk_part_tx_hash FOREIGN KEY (tx_hash) REFERENCES transactions(tx_hash) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='交易参与方信息表';