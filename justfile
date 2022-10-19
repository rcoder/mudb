pre-bench:
    rm -rf .bench
    mkdir .bench

bench: pre-bench
    cargo bench

profile: pre-bench
    sudo cargo flamegraph --bench readwrite -- --bench
    sudo rm .bench/*

test:
    cargo test
