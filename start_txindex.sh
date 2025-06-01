#!/bin/bash

# 单独启动 transactions_index.py

echo "启动 txindex 服务..."

# 方案1: 使用 PM2
echo "使用 PM2 启动..."
pm2 start ecosystem.config.js --only txindex

echo "服务状态："
pm2 list

echo ""
echo "查看日志: pm2 logs txindex"
echo "停止服务: pm2 stop txindex"
echo "重启服务: pm2 restart txindex"
