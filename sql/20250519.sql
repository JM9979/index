/*
 Navicat Premium Dump SQL

 Source Server         : wslmysql
 Source Server Type    : MySQL
 Source Server Version : 80042 (8.0.42-0ubuntu0.20.04.1)
 Source Host           : 172.18.183.54:3306
 Source Schema         : TBC20721

 Target Server Type    : MySQL
 Target Server Version : 80042 (8.0.42-0ubuntu0.20.04.1)
 File Encoding         : 65001

 Date: 19/05/2025 22:27:16
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for address_transactions
-- ----------------------------
DROP TABLE IF EXISTS `address_transactions`;
CREATE TABLE `address_transactions`  (
  `Fid` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `address` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '比特币地址',
  `tx_hash` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '关联的交易哈希',
  `is_sender` tinyint(1) NOT NULL COMMENT '是否为发送方，1表示是，0表示否',
  `is_recipient` tinyint(1) NOT NULL COMMENT '是否为接收方，1表示是，0表示否',
  `balance_change` decimal(16, 8) NULL DEFAULT NULL COMMENT '该地址在此交易中的余额变化',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  PRIMARY KEY (`Fid`) USING BTREE,
  UNIQUE INDEX `idx_address_tx`(`address` ASC, `tx_hash` ASC) USING BTREE,
  INDEX `idx_address`(`address` ASC) USING BTREE,
  INDEX `idx_tx_hash`(`tx_hash` ASC) USING BTREE,
  CONSTRAINT `fk_addr_tx_hash` FOREIGN KEY (`tx_hash`) REFERENCES `transactions` (`tx_hash`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '地址与交易关系表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of address_transactions
-- ----------------------------

-- ----------------------------
-- Table structure for ft_balance
-- ----------------------------
DROP TABLE IF EXISTS `ft_balance`;
CREATE TABLE `ft_balance`  (
  `ft_holder_combine_script` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '持有者组合脚本',
  `ft_contract_id` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '可替代代币合约ID',
  `ft_balance` bigint UNSIGNED NULL DEFAULT NULL COMMENT '代币余额',
  PRIMARY KEY (`ft_holder_combine_script`, `ft_contract_id`) USING BTREE,
  INDEX `fk_balance_contract`(`ft_contract_id` ASC) USING BTREE,
  CONSTRAINT `fk_balance_contract` FOREIGN KEY (`ft_contract_id`) REFERENCES `ft_tokens` (`ft_contract_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '存储可替代代币持有者余额信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of ft_balance
-- ----------------------------

-- ----------------------------
-- Table structure for ft_tokens
-- ----------------------------
DROP TABLE IF EXISTS `ft_tokens`;
CREATE TABLE `ft_tokens`  (
  `ft_contract_id` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '可替代代币合约ID，使用交易ID作为唯一标识',
  `ft_code_script` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '代币代码脚本',
  `ft_tape_script` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '代币tape脚本',
  `ft_supply` bigint UNSIGNED NULL DEFAULT NULL COMMENT '代币总供应量',
  `ft_decimal` tinyint UNSIGNED NULL DEFAULT NULL COMMENT '小数位数',
  `ft_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '代币名称',
  `ft_symbol` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '代币符号',
  `ft_description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '代币描述',
  `ft_origin_utxo` char(72) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '原始UTXO，用于追踪代币来源',
  `ft_creator_combine_script` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '创建者组合脚本',
  `ft_holders_count` int NULL DEFAULT NULL COMMENT '持有者数量',
  `ft_icon_url` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '代币图标URL',
  `ft_create_timestamp` int NULL DEFAULT NULL COMMENT '创建时间戳',
  `ft_token_price` decimal(27, 18) NULL DEFAULT NULL COMMENT '代币价格',
  PRIMARY KEY (`ft_contract_id`) USING BTREE,
  UNIQUE INDEX `ft_origin_utxo`(`ft_origin_utxo` ASC) USING BTREE,
  INDEX `idx_name`(`ft_name`(20) ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '存储可替代代币相关信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of ft_tokens
-- ----------------------------

-- ----------------------------
-- Table structure for ft_txo_set
-- ----------------------------
DROP TABLE IF EXISTS `ft_txo_set`;
CREATE TABLE `ft_txo_set`  (
  `utxo_txid` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'UTXO交易ID',
  `utxo_vout` int NOT NULL COMMENT 'UTXO输出索引',
  `ft_holder_combine_script` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '持有者组合脚本',
  `ft_contract_id` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '可替代代币合约ID',
  `utxo_balance` bigint UNSIGNED NULL DEFAULT NULL COMMENT 'UTXO余额（以聪为单位）',
  `ft_balance` bigint UNSIGNED NULL DEFAULT NULL COMMENT '代币余额',
  `if_spend` tinyint(1) NULL DEFAULT NULL COMMENT '是否已花费，1表示已花费，0表示未花费',
  PRIMARY KEY (`utxo_txid`, `utxo_vout`) USING BTREE,
  INDEX `idx_script_hash_contract_id`(`ft_holder_combine_script` ASC, `ft_contract_id` ASC) USING BTREE,
  INDEX `idx_if_spend`(`if_spend` ASC) USING BTREE,
  INDEX `fk_utxo_set_contract`(`ft_contract_id` ASC) USING BTREE,
  CONSTRAINT `fk_utxo_set_contract` FOREIGN KEY (`ft_contract_id`) REFERENCES `ft_tokens` (`ft_contract_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '存储可替代代币UTXO集相关信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of ft_txo_set
-- ----------------------------

-- ----------------------------
-- Table structure for nft_collections
-- ----------------------------
DROP TABLE IF EXISTS `nft_collections`;
CREATE TABLE `nft_collections`  (
  `collection_id` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '集合ID，使用交易ID作为唯一标识',
  `collection_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '集合名称',
  `collection_creator_address` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '创建者地址',
  `collection_creator_script_hash` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '创建者脚本哈希',
  `collection_symbol` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '集合符号',
  `collection_attributes` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '集合属性，JSON格式',
  `collection_description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '集合描述',
  `collection_supply` int NULL DEFAULT NULL COMMENT '集合供应量，表示该集合最多可以包含多少NFT',
  `collection_create_timestamp` int NULL DEFAULT NULL COMMENT '创建时间戳',
  `collection_icon` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT '集合图标URL或图像数据',
  PRIMARY KEY (`collection_id`) USING BTREE,
  INDEX `idx_collection_creator`(`collection_creator_script_hash` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '存储NFT集合相关信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of nft_collections
-- ----------------------------

-- ----------------------------
-- Table structure for nft_utxo_set
-- ----------------------------
DROP TABLE IF EXISTS `nft_utxo_set`;
CREATE TABLE `nft_utxo_set`  (
  `nft_contract_id` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'NFT合约ID，使用交易ID作为唯一标识',
  `collection_id` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '所属集合ID',
  `collection_index` int NULL DEFAULT NULL COMMENT '在集合中的索引位置',
  `collection_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '所属集合名称',
  `nft_utxo_id` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'NFT当前所在的UTXO交易ID',
  `nft_code_balance` bigint UNSIGNED NULL DEFAULT NULL COMMENT 'NFT代码余额（以聪为单位）',
  `nft_p2pkh_balance` bigint UNSIGNED NULL DEFAULT NULL COMMENT 'NFT P2PKH余额（以聪为单位）',
  `nft_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'NFT名称',
  `nft_symbol` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'NFT符号',
  `nft_attributes` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT 'NFT属性，JSON格式',
  `nft_description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT 'NFT描述',
  `nft_transfer_time_count` int NULL DEFAULT NULL COMMENT 'NFT转账次数',
  `nft_holder_address` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'NFT持有者地址',
  `nft_holder_script_hash` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'NFT持有者脚本哈希',
  `nft_create_timestamp` int NULL DEFAULT NULL COMMENT '创建时间戳',
  `nft_last_transfer_timestamp` int NULL DEFAULT NULL COMMENT '最后转账时间戳',
  `nft_icon` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL COMMENT 'NFT图像URL或图像数据',
  PRIMARY KEY (`nft_contract_id`) USING BTREE,
  UNIQUE INDEX `nft_utxo_id`(`nft_utxo_id` ASC) USING BTREE,
  INDEX `idx_utxo_id`(`nft_utxo_id` ASC) USING BTREE,
  INDEX `idx_nft_holder`(`nft_holder_script_hash` ASC) USING BTREE,
  INDEX `idx_collection_id_index`(`collection_id` ASC, `collection_index` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '存储NFT代币UTXO集相关信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of nft_utxo_set
-- ----------------------------

-- ----------------------------
-- Table structure for t_index_build_status
-- ----------------------------
DROP TABLE IF EXISTS `t_index_build_status`;
CREATE TABLE `t_index_build_status`  (
  `id` int NULL DEFAULT NULL,
  `name` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `value` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL,
  UNIQUE INDEX `name`(`name` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '索引构建状态' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of t_index_build_status
-- ----------------------------
INSERT INTO `t_index_build_status` VALUES (1, 'index_height', '0');
INSERT INTO `t_index_build_status` VALUES (3, 'last_mempool', '[]');
INSERT INTO `t_index_build_status` VALUES (2, 'mempool', '[]');

-- ----------------------------
-- Table structure for transaction_participants
-- ----------------------------
DROP TABLE IF EXISTS `transaction_participants`;
CREATE TABLE `transaction_participants`  (
  `Fid` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `tx_hash` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '关联的交易哈希',
  `address` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '参与方地址',
  `role` enum('sender','recipient') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '参与角色：sender发送方，recipient接收方',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  PRIMARY KEY (`Fid`) USING BTREE,
  INDEX `idx_tx_hash`(`tx_hash` ASC) USING BTREE,
  INDEX `idx_address_role`(`address` ASC, `role` ASC) USING BTREE,
  CONSTRAINT `fk_part_tx_hash` FOREIGN KEY (`tx_hash`) REFERENCES `transactions` (`tx_hash`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '交易参与方信息表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of transaction_participants
-- ----------------------------

-- ----------------------------
-- Table structure for transactions
-- ----------------------------
DROP TABLE IF EXISTS `transactions`;
CREATE TABLE `transactions`  (
  `Fid` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `tx_hash` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '交易哈希值，唯一标识一笔交易',
  `fee` decimal(16, 8) NOT NULL COMMENT '交易手续费',
  `time_stamp` bigint NULL DEFAULT NULL COMMENT '交易时间戳，单位秒',
  `transaction_utc_time` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '格式化的UTC时间，如：2023-01-01 12:00:00',
  `tx_type` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '交易类型，如：P2PKH、TBC20、TBC721、P2MS',
  `block_height` int NULL DEFAULT NULL COMMENT '交易所在区块高度',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  PRIMARY KEY (`Fid`) USING BTREE,
  UNIQUE INDEX `idx_tx_hash`(`tx_hash` ASC) USING BTREE,
  INDEX `idx_time_stamp`(`time_stamp` ASC) USING BTREE,
  INDEX `idx_tx_type`(`tx_type` ASC) USING BTREE,
  INDEX `idx_block_height`(`block_height` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '交易基本信息表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of transactions
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
