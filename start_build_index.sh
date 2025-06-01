#!/bin/bash

# 单独启动 build_index_v2.py

echo "启动 build_index_v2 服务..."

# 方案1: 使用 PM2
echo "使用 PM2 启动..."
pm2 start ecosystem.config.js --only apiindex

echo "服务状态："
pm2 list

echo ""
echo "查看日志: pm2 logs apiindex"
echo "停止服务: pm2 stop apiindex"
echo "重启服务: pm2 restart apiindex"
