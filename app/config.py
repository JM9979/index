"""
Configuration for the API.
Simple configuration for the build_index_v2.py
"""

class Config:
    # TBC RPC配置
    TBC_RPC_URL = "http://localhost:8332"
    TBC_RPC_AUTH = ("user", "password")  # 替换为实际的用户名和密码

    # MySQL数据库配置
    MYSQL_HOST = "localhost"
    MYSQL_PORT = 3306
    MYSQL_USER = "root"
    MYSQL_PASS = "password"  # 替换为实际的密码
    MYSQL_DB = "TBC20721"

# 导出配置实例
config = Config() 