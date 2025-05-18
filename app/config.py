"""
Configuration for the API.
Simple configuration for the build_index_v2.py
"""

class Config:
    # TBC RPC配置
    TBC_RPC_URL = "http://localhost:8332"
    TBC_RPC_AUTH = ("tbcuser", "randompasswd")  # 替换为实际的用户名和密码

    # MySQL数据库配置
    MYSQL_HOST = "localhost"
    MYSQL_PORT = 3306
    MYSQL_USER = "root"
    MYSQL_PASS = "123456"  # 替换为实际的密码
    MYSQL_DB = "TBC20721"
    
    # AWS S3配置
    AWS_ACCESS_KEY_ID = "AKIAXRQA4LD32NZVWDMU"  # 替换为实际的访问密钥ID
    AWS_SECRET_ACCESS_KEY = "ifTHARBxnWDv/QoD3y769OgEh1sby9Wgb4Je7hjL"  # 替换为实际的秘密访问密钥
    AWS_REGION_NAME = "ap-southeast-1"
    S3_BUCKET_NAME = "tbc-node-nft-icon"

# 导出配置实例
config = Config() 