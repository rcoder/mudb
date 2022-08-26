pre-bench:
    rm -rf .bench
    mkdir .bench

bench: pre-bench
    cargo bench

profile: pre-bench
    CARGO_PROFILE_BENCH_DEBUG=true sudo cargo flamegraph --bench readwrite
    sudo rm .bench/*

test:
    cargo test
