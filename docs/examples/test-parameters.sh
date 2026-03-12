#!/bin/bash

# 参数传递端到端测试脚本
# 测试新的参数传递功能

set -e

JAR_FILE="flink-etl-client/target/flink-etl-client-1.0.0-SNAPSHOT.jar"
CONFIG_FILE="docs/examples/mysql-to-console.json"

echo "===== 测试 1: 使用 --file 参数 ====="
java -jar $JAR_FILE --file $CONFIG_FILE

echo ""
echo "===== 测试 2: 使用 --config 参数 ====="
JSON_STRING=$(cat $CONFIG_FILE | tr '\n' ' ')
java -jar $JAR_FILE --config "$JSON_STRING"

echo ""
echo "===== 测试 3: 文件不存在错误 ====="
java -jar $JAR_FILE --file /non/existent/file.json || echo "预期错误"

echo ""
echo "===== 测试 4: JSON 解析错误 ====="
java -jar $JAR_FILE --config "{ invalid json }" || echo "预期错误"

echo ""
echo "===== 测试 5: 向后兼容性测试（旧参数格式）====="
java -jar $JAR_FILE $CONFIG_FILE

echo ""
echo "===== 所有测试完成 ====="