#!/usr/bin/env bash
# Pin the build to Java 17. Flink 1.20 does not run on this machine's default JDK.
# Usage:  source scripts/env.sh
JAVA_17_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

if [ ! -d "$JAVA_17_HOME" ]; then
  echo "Java 17 not found at $JAVA_17_HOME" >&2
  echo "Install it with: brew install openjdk@17" >&2
  # Works whether the script is sourced (return) or executed (exit). ShellCheck
  # reads the exit as unreachable because it cannot know which it is.
  # shellcheck disable=SC2317
  return 1 2>/dev/null || exit 1
fi

export JAVA_HOME="$JAVA_17_HOME"
export PATH="$JAVA_HOME/bin:$PATH"
echo "JAVA_HOME=$JAVA_HOME"
java -version
