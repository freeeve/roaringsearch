#!/bin/bash
# Run all fuzz tests for a specified duration each
# Usage: ./scripts/fuzz.sh [duration] [workers]
# Example: ./scripts/fuzz.sh 1h      (default: 1h, 4 workers)
#          ./scripts/fuzz.sh 10m 8   (10 min, 8 workers)
#          ./scripts/fuzz.sh 30s     (30 sec, 4 workers)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

DURATION="${1:-1h}"
WORKERS="${2:-4}"
SEPARATOR="================================================"

copy_corpus_to_testdata() {
    echo ""
    echo "$SEPARATOR"
    echo "Copying fuzz corpus to testdata/"
    echo "$SEPARATOR"

    local gocache_fuzz
    gocache_fuzz="$(go env GOCACHE)/fuzz/github.com/freeeve/roaringsearch"
    local testdata_fuzz="$PROJECT_ROOT/testdata/fuzz"

    if [[ -d "$gocache_fuzz" ]]; then
        mkdir -p "$testdata_fuzz"
        local before_count
        before_count=$(find "$testdata_fuzz" -type f 2>/dev/null | wc -l | tr -d ' ')
        cp -r "$gocache_fuzz"/* "$testdata_fuzz"/ 2>/dev/null || true
        local after_count
        after_count=$(find "$testdata_fuzz" -type f | wc -l | tr -d ' ')
        local new_count=$((after_count - before_count))
        echo "Copied corpus from Go cache to testdata/fuzz/"
        echo "New files added: $new_count (total: $after_count)"
    else
        echo "No fuzz cache found at $gocache_fuzz"
    fi
    return 0
}

cleanup() {
    echo ""
    echo "Fuzz testing interrupted"
    copy_corpus_to_testdata
    exit 0
}
trap cleanup SIGINT SIGTERM

FUZZ_TESTS=(
    "FuzzIndexReadFrom"
    "FuzzBitmapFilterRead"
    "FuzzSortColumnRead"
    "FuzzNormalize"
    "FuzzSearch"
    "FuzzAddAndSearch"
)

echo "Running ${#FUZZ_TESTS[@]} fuzz tests for $DURATION each with $WORKERS workers"
echo "$SEPARATOR"

for test in "${FUZZ_TESTS[@]}"; do
    echo ""
    echo ">>> Running $test for $DURATION"
    go test -v -fuzz="$test" -fuzztime="$DURATION" -parallel="$WORKERS"
    status=$?
    if [[ $status -ne 0 ]]; then
        echo "!!! $test failed"
        exit 1
    fi
    echo "<<< $test completed"
done

echo ""
echo "$SEPARATOR"
echo "All fuzz tests passed"

copy_corpus_to_testdata
