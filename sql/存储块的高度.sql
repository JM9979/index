ALTER TABLE nft_collections ADD COLUMN block_height UNSIGNED INT;
ALTER TABLE nft_utxo_set ADD COLUMN block_height UNSIGNED INT;
ALTER TABLE ft_tokens ADD COLUMN block_height UNSIGNED INT;
ALTER TABLE ft_txo_set ADD COLUMN block_height UNSIGNED INT;