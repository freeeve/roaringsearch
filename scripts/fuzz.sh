#!/bin/bash
# Run all fuzz tests for a specified duration each
# Usage: ./scripts/fuzz.sh [duration]
# Example: ./scripts/fuzz.sh 1h    (default)
#          ./scripts/fuzz.sh 10m
#          ./scripts/fuzz.sh 30s

DURATION="${1:-1h}"

FUZZ_TESTS=(
    "FuzzIndexReadFrom"
    "FuzzBitmapFilterRead"
    "FuzzSortColumnRead"
    "FuzzNormalize"
    "FuzzSearch"
    "FuzzAddAndSearch"
)

echo "Running ${#FUZZ_TESTS[@]} fuzz tests for $DURATION each"
echo "================================================"

for test in "${FUZZ_TESTS[@]}"; do
    echo ""
    echo ">>> Running $test for $DURATION"
    go test -fuzz="$test" -fuzztime="$DURATION"
    if [ $? -ne 0 ]; then
        echo "!!! $test failed"
        exit 1
    fi
    echo "<<< $test completed"
done

echo ""
echo "================================================"
echo "All fuzz tests passed"
