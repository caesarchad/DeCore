#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

if [[ ! -d target/perf-libs ]]; then
  if [[ $(uname) != Linux ]]; then
    echo Performance libraries are only available for Linux
    exit 1
  fi

  if [[ $(uname -m) != x86_64 ]]; then
    echo Performance libraries are only available for x86_64 architecture
    exit 1
  fi

  mkdir -p target/perf-libs
  (
    set -x
    cd target/perf-libs
    curl https://morgan-perf.s3.amazonaws.com/v0.12.1/x86_64-unknown-linux-gnu/morgan-perf.tgz | tar zxvf -
  )
fi

cat > target/perf-libs/env.sh <<'EOF'
SOLANA_PERF_LIBS="$(cd $(dirname "${BASH_SOURCE[0]}"); pwd)"

echo "morgan-perf-libs version: $(cat $SOLANA_PERF_LIBS/morgan-perf-HEAD.txt)"

if [[ -r "$SOLANA_PERF_LIBS"/morgan-perf-CUDA_HOME.txt ]]; then
  CUDA_HOME=$(cat "$SOLANA_PERF_LIBS"/morgan-perf-CUDA_HOME.txt)
else
  CUDA_HOME=/usr/local/cuda
fi

echo CUDA_HOME="$CUDA_HOME"
export CUDA_HOME="$CUDA_HOME"

echo LD_LIBRARY_PATH="$SOLANA_PERF_LIBS:$CUDA_HOME/lib64:$LD_LIBRARY_PATH"
export LD_LIBRARY_PATH="$SOLANA_PERF_LIBS:$CUDA_HOME/lib64:$LD_LIBRARY_PATH"

echo PATH="$SOLANA_PERF_LIBS:$CUDA_HOME/bin:$PATH"
export PATH="$SOLANA_PERF_LIBS:$CUDA_HOME/bin:$PATH"

if [[ -r "$CUDA_HOME"/version.txt && -r $SOLANA_PERF_LIBS/cuda-version.txt ]]; then
  if ! diff "$CUDA_HOME"/version.txt "$SOLANA_PERF_LIBS"/cuda-version.txt > /dev/null; then
      echo ==============================================
      echo "Warning: possible CUDA version mismatch with $CUDA_HOME"
      echo
      echo "Expected version: $(cat "$SOLANA_PERF_LIBS"/cuda-version.txt)"
      echo "Detected version: $(cat "$CUDA_HOME"/version.txt)"
      echo ==============================================
  fi
else
  echo ==============================================
  echo Warning: unable to validate CUDA version
  echo ==============================================
fi
EOF

echo
echo "genesis $PWD/target/perf-libs/env.sh to setup environment"
exit 0
