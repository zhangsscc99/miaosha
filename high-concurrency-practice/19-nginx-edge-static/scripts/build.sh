#!/usr/bin/env bash
# 编译。依赖（Jedis）已在本机 ~/.m2，-o 离线。
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -z "${JAVA_HOME:-}" ]]; then
  for c in /opt/homebrew/opt/openjdk /opt/homebrew/opt/openjdk@21 /Library/Java/JavaVirtualMachines/*/Contents/Home; do
    if [[ -x "$c/bin/java" ]]; then JAVA_HOME="$c"; break; fi
  done
fi
if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "JAVA_HOME not found; install a JDK 21+" >&2
  exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"

mvn -o -q -DskipTests compile
if [[ ! -f target/cp.txt ]]; then
  mvn -o -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt
fi
echo "built with $(java -version 2>&1 | head -1)"
