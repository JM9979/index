-- Set foreign key checks
SET FOREIGN_KEY_CHECKS = 0;

-- NFT Collection Table
CREATE TABLE IF NOT EXISTS `nft_collections` (
  `collection_id` char(64) NOT NULL COMMENT '集合ID，使用交易ID作为唯一标识',
  `collection_name` varchar(64) DEFAULT NULL COMMENT '集合名称',
  `collection_creator_address` varchar(64) DEFAULT NULL COMMENT '创建者地址',
  `collection_creator_script_hash` char(64) DEFAULT NULL COMMENT '创建者脚本哈希',
  `collection_symbol` varchar(64) DEFAULT NULL COMMENT '集合符号',
  `collection_attributes` text COMMENT '集合属性，JSON格式',
  `collection_description` text COMMENT '集合描述',
  `collection_supply` int DEFAULT NULL COMMENT '集合供应量，表示该集合最多可以包含多少NFT',
  `collection_create_timestamp` int DEFAULT NULL COMMENT '创建时间戳',
  `collection_icon` mediumtext COMMENT '集合图标URL或图像数据',
  PRIMARY KEY (`collection_id`),
  KEY `idx_collection_creator` (`collection_creator_script_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='存储NFT集合相关信息';

-- NFT UTXO Set Table
CREATE TABLE IF NOT EXISTS `nft_utxo_set` (
  `nft_contract_id` char(64) NOT NULL COMMENT 'NFT合约ID，使用交易ID作为唯一标识',
  `collection_id` char(64) DEFAULT NULL COMMENT '所属集合ID',
  `collection_index` int DEFAULT NULL COMMENT '在集合中的索引位置',
  `collection_name` varchar(64) DEFAULT NULL COMMENT '所属集合名称',
  `nft_utxo_id` char(64) DEFAULT NULL COMMENT 'NFT当前所在的UTXO交易ID',
  `nft_code_balance` bigint unsigned DEFAULT NULL COMMENT 'NFT代码余额（以聪为单位）',
  `nft_p2pkh_balance` bigint unsigned DEFAULT NULL COMMENT 'NFT P2PKH余额（以聪为单位）',
  `nft_name` varchar(64) DEFAULT NULL COMMENT 'NFT名称',
  `nft_symbol` varchar(64) DEFAULT NULL COMMENT 'NFT符号',
  `nft_attributes` text COMMENT 'NFT属性，JSON格式',
  `nft_description` text COMMENT 'NFT描述',
  `nft_transfer_time_count` int DEFAULT NULL COMMENT 'NFT转账次数',
  `nft_holder_address` varchar(64) DEFAULT NULL COMMENT 'NFT持有者地址',
  `nft_holder_script_hash` char(64) DEFAULT NULL COMMENT 'NFT持有者脚本哈希',
  `nft_create_timestamp` int DEFAULT NULL COMMENT '创建时间戳',
  `nft_last_transfer_timestamp` int DEFAULT NULL COMMENT '最后转账时间戳',
  `nft_icon` mediumtext COMMENT 'NFT图像URL或图像数据',
  PRIMARY KEY (`nft_contract_id`),
  UNIQUE KEY `nft_utxo_id` (`nft_utxo_id`),
  KEY `idx_utxo_id` (`nft_utxo_id`),
  KEY `idx_nft_holder` (`nft_holder_script_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='存储NFT代币UTXO集相关信息';

-- Token Table
CREATE TABLE IF NOT EXISTS `ft_tokens` (
  `ft_contract_id` char(64) NOT NULL COMMENT '可替代代币合约ID，使用交易ID作为唯一标识',
  `ft_code_script` text COMMENT '代币代码脚本',
  `ft_tape_script` text COMMENT '代币tape脚本',
  `ft_supply` bigint unsigned DEFAULT NULL COMMENT '代币总供应量',
  `ft_decimal` tinyint unsigned DEFAULT NULL COMMENT '小数位数',
  `ft_name` varchar(64) DEFAULT NULL COMMENT '代币名称',
  `ft_symbol` varchar(64) DEFAULT NULL COMMENT '代币符号',
  `ft_description` text COMMENT '代币描述',
  `ft_origin_utxo` char(72) DEFAULT NULL COMMENT '原始UTXO，用于追踪代币来源',
  `ft_creator_combine_script` char(42) DEFAULT NULL COMMENT '创建者组合脚本',
  `ft_holders_count` int DEFAULT NULL COMMENT '持有者数量',
  `ft_icon_url` varchar(255) DEFAULT NULL COMMENT '代币图标URL',
  `ft_create_timestamp` int DEFAULT NULL COMMENT '创建时间戳',
  `ft_token_price` decimal(27,18) DEFAULT NULL COMMENT '代币价格',
  PRIMARY KEY (`ft_contract_id`),
  UNIQUE KEY `ft_origin_utxo` (`ft_origin_utxo`),
  KEY `idx_name` (`ft_name`(20))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='存储可替代代币相关信息';

-- Token TXO Set Table
CREATE TABLE IF NOT EXISTS `ft_txo_set` (
  `utxo_txid` char(64) NOT NULL COMMENT 'UTXO交易ID',
  `utxo_vout` int NOT NULL COMMENT 'UTXO输出索引',
  `ft_holder_combine_script` char(42) DEFAULT NULL COMMENT '持有者组合脚本',
  `ft_contract_id` char(64) DEFAULT NULL COMMENT '可替代代币合约ID',
  `utxo_balance` bigint unsigned DEFAULT NULL COMMENT 'UTXO余额（以聪为单位）',
  `ft_balance` bigint unsigned DEFAULT NULL COMMENT '代币余额',
  `if_spend` tinyint(1) DEFAULT NULL COMMENT '是否已花费，1表示已花费，0表示未花费',
  PRIMARY KEY (`utxo_txid`,`utxo_vout`),
  KEY `idx_script_hash_contract_id` (`ft_holder_combine_script`,`ft_contract_id`),
  KEY `idx_if_spend` (`if_spend`),
  KEY `fk_utxo_set_contract` (`ft_contract_id`),
  CONSTRAINT `fk_utxo_set_contract` FOREIGN KEY (`ft_contract_id`) REFERENCES `ft_tokens` (`ft_contract_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='存储可替代代币UTXO集相关信息';

-- Token Balance Table
CREATE TABLE IF NOT EXISTS `ft_balance` (
  `ft_holder_combine_script` char(42) NOT NULL COMMENT '持有者组合脚本',
  `ft_contract_id` char(64) NOT NULL COMMENT '可替代代币合约ID',
  `ft_balance` bigint unsigned DEFAULT NULL COMMENT '代币余额',
  PRIMARY KEY (`ft_holder_combine_script`,`ft_contract_id`),
  KEY `fk_balance_contract` (`ft_contract_id`),
  CONSTRAINT `fk_balance_contract` FOREIGN KEY (`ft_contract_id`) REFERENCES `ft_tokens` (`ft_contract_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='存储可替代代币持有者余额信息';

-- Restore foreign key checks
SET FOREIGN_KEY_CHECKS = 1; 