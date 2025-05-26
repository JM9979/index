"""
字段长度限制处理工具
"""

class FieldTruncate:
    @staticmethod
    def truncate_fields(data: dict) -> dict:
        """
        根据数据库字段长度限制截断字段值
        
        Args:
            data: 包含字段值的字典
            
        Returns:
            处理后的字典
        """
        field_limits = {
            # ft_balance
            'ft_holder_combine_script': 42,
            'ft_contract_id': 64,
            
            # ft_tokens
            'ft_name': 64,
            'ft_symbol': 64,
            'ft_origin_utxo': 72,
            'ft_creator_combine_script': 42,
            'ft_icon_url': 255,
            
            # ft_txo_set
            'utxo_txid': 64,
            'ft_holder_combine_script': 42,
            'ft_contract_id': 64,
            
            # nft_collections
            'collection_id': 64,
            'collection_name': 64,
            'collection_creator_address': 64,
            'collection_creator_script_hash': 64,
            'collection_symbol': 64,
            
            # nft_utxo_set
            'nft_contract_id': 64,
            'collection_id': 64,
            'collection_name': 64,
            'nft_utxo_id': 64,
            'nft_name': 64,
            'nft_symbol': 64,
            'nft_holder_address': 64,
            'nft_holder_script_hash': 64,
        }
        
        result = {}
        for key, value in data.items():
            if key in field_limits and isinstance(value, str):
                max_length = field_limits[key]
                result[key] = value[:max_length]
            else:
                result[key] = value
                
        return result 