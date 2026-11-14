#!/usr/bin/env bash
# 构建实验室代码（独立 Maven 模块，不影响仓库原有 Spring Boot 工程）
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/Cellar/openjdk/27.0.1.0.3/libexec/openjdk.jdk/Contents/Home}"
MVN="${MVN:-mvn}"

$MVN -q -B compile
$MVN -q -B dependency:build-classpath -Dmdep.outputFile=target/cp.txt
echo "[build] classpath -> $(pwd)/target/cp.txt"
