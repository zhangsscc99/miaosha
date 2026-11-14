#!/usr/bin/env bash
# 编译。依赖都在本机 ~/.m2 里（和 17 章同一套 Jedis/MySQL/Hikari），-o 离线。
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

if [[ -f target/cp.txt ]]; then
  :
else
  mvn -o -q -DskipTests compile
  mvn -o -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt
fi
mvn -o -q -DskipTests compile
echo "built with $(java -version 2>&1 | head -1)"
