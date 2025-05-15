-- Set foreign key checks
SET FOREIGN_KEY_CHECKS = 0;

-- NFT Collection Table
CREATE TABLE IF NOT EXISTS `nft_collections` (
  `collection_id` char(64) NOT NULL,
  `collection_name` varchar(64) DEFAULT NULL,
  `collection_creator_address` varchar(64) DEFAULT NULL,
  `collection_creator_script_hash` char(64) DEFAULT NULL,
  `collection_symbol` varchar(64) DEFAULT NULL,
  `collection_attributes` text,
  `collection_description` text,
  `collection_supply` int DEFAULT NULL,
  `collection_create_timestamp` int DEFAULT NULL,
  `collection_icon` mediumtext,
  PRIMARY KEY (`collection_id`),
  KEY `idx_collection_creator` (`collection_creator_script_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- NFT UTXO Set Table
CREATE TABLE IF NOT EXISTS `nft_utxo_set` (
  `nft_contract_id` char(64) NOT NULL,
  `collection_id` char(64) DEFAULT NULL,
  `collection_index` int DEFAULT NULL,
  `collection_name` varchar(64) DEFAULT NULL,
  `nft_utxo_id` char(64) DEFAULT NULL,
  `nft_code_balance` bigint unsigned DEFAULT NULL,
  `nft_p2pkh_balance` bigint unsigned DEFAULT NULL,
  `nft_name` varchar(64) DEFAULT NULL,
  `nft_symbol` varchar(64) DEFAULT NULL,
  `nft_attributes` text,
  `nft_description` text,
  `nft_transfer_time_count` int DEFAULT NULL,
  `nft_holder_address` varchar(64) DEFAULT NULL,
  `nft_holder_script_hash` char(64) DEFAULT NULL,
  `nft_create_timestamp` int DEFAULT NULL,
  `nft_last_transfer_timestamp` int DEFAULT NULL,
  `nft_icon` mediumtext,
  PRIMARY KEY (`nft_contract_id`),
  UNIQUE KEY `nft_utxo_id` (`nft_utxo_id`),
  KEY `idx_utxo_id` (`nft_utxo_id`),
  KEY `idx_nft_holder` (`nft_holder_script_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Token Table
CREATE TABLE IF NOT EXISTS `ft_tokens` (
  `ft_contract_id` char(64) NOT NULL,
  `ft_code_script` text,
  `ft_tape_script` text,
  `ft_supply` bigint unsigned DEFAULT NULL,
  `ft_decimal` tinyint unsigned DEFAULT NULL,
  `ft_name` varchar(64) DEFAULT NULL,
  `ft_symbol` varchar(64) DEFAULT NULL,
  `ft_description` text,
  `ft_origin_utxo` char(72) DEFAULT NULL,
  `ft_creator_combine_script` char(42) DEFAULT NULL,
  `ft_holders_count` int DEFAULT NULL,
  `ft_icon_url` varchar(255) DEFAULT NULL,
  `ft_create_timestamp` int DEFAULT NULL,
  `ft_token_price` decimal(27,18) DEFAULT NULL,
  PRIMARY KEY (`ft_contract_id`),
  UNIQUE KEY `ft_origin_utxo` (`ft_origin_utxo`),
  KEY `idx_name` (`ft_name`(20))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Token TXO Set Table
CREATE TABLE IF NOT EXISTS `ft_txo_set` (
  `utxo_txid` char(64) NOT NULL,
  `utxo_vout` int NOT NULL,
  `ft_holder_combine_script` char(42) DEFAULT NULL,
  `ft_contract_id` char(64) DEFAULT NULL,
  `utxo_balance` bigint unsigned DEFAULT NULL,
  `ft_balance` bigint unsigned DEFAULT NULL,
  `if_spend` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`utxo_txid`,`utxo_vout`),
  KEY `idx_script_hash_contract_id` (`ft_holder_combine_script`,`ft_contract_id`),
  KEY `idx_if_spend` (`if_spend`),
  KEY `fk_utxo_set_contract` (`ft_contract_id`),
  CONSTRAINT `fk_utxo_set_contract` FOREIGN KEY (`ft_contract_id`) REFERENCES `ft_tokens` (`ft_contract_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Token Balance Table
CREATE TABLE IF NOT EXISTS `ft_balance` (
  `ft_holder_combine_script` char(42) NOT NULL,
  `ft_contract_id` char(64) NOT NULL,
  `ft_balance` bigint unsigned DEFAULT NULL,
  PRIMARY KEY (`ft_holder_combine_script`,`ft_contract_id`),
  KEY `fk_balance_contract` (`ft_contract_id`),
  CONSTRAINT `fk_balance_contract` FOREIGN KEY (`ft_contract_id`) REFERENCES `ft_tokens` (`ft_contract_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Restore foreign key checks
SET FOREIGN_KEY_CHECKS = 1; 