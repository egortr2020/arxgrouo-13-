#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f docker-compose.benchmark.yml"
WORKERS_LIST=(1 2 4)
RESULTS=()

for N in "${WORKERS_LIST[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Workers: $N"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    BUILD_FLAG=""
    if [[ $N -eq ${WORKERS_LIST[0]} ]]; then
        BUILD_FLAG="--build"
    fi

    OUTPUT=$(
        $COMPOSE up $BUILD_FLAG \
            --scale consumer="$N" \
            --abort-on-container-exit \
            --exit-code-from benchmark \
            2>&1
    )

    echo "$OUTPUT" | grep -E "(benchmark|worker|BENCHMARK|Total|Throughput|Avg|tasks)" || true
    echo ""

    TIME_LINE=$(echo "$OUTPUT" | grep "Total time" | tail -1)
    TPUT_LINE=$(echo "$OUTPUT" | grep "Throughput" | tail -1)

    ELAPSED=$(echo "$TIME_LINE" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    TPUT=$(echo "$TPUT_LINE"   | grep -oE '[0-9]+\.[0-9]+' | head -1)

    RESULTS+=("$N|${ELAPSED:-?}|${TPUT:-?}")

    $COMPOSE down -v --remove-orphans > /dev/null 2>&1 || true
    sleep 3
done

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║               SCALING SUMMARY                   ║"
echo "╠══════════╦══════════════╦══════════════╦═════════╣"
echo "║  Workers ║  Total time  ║  Throughput  ║ Speedup ║"
echo "╠══════════╬══════════════╬══════════════╬═════════╣"

BASE_TIME=""
for ROW in "${RESULTS[@]}"; do
    IFS='|' read -r W T P <<< "$ROW"
    if [[ -z "$BASE_TIME" ]]; then
        BASE_TIME="$T"
        SPEEDUP="1.00x"
    else
        if [[ "$T" != "?" && "$BASE_TIME" != "?" ]]; then
            SPEEDUP=$(awk "BEGIN { printf \"%.2fx\", $BASE_TIME / $T }")
        else
            SPEEDUP="?"
        fi
    fi
    printf "║  %-8s║  %-12s║  %-12s║ %-8s║\n" "$W" "${T}s" "${P} t/s" "$SPEEDUP"
done

echo "╚══════════╩══════════════╩══════════════╩═════════╝"
