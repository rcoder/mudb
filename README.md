# MuDB

> It's "mew-DB" if you're feeling fancy, or "mud-B" if you found the all-too likely bugs.

This crate provides a small library + command-line tool to manage "databases" made up of newline-delimited JSON and with a simple query and single or bulk-modification interface.

As databases storage engines formats go, ND-JSON is terrible on just about every possible criteria except two:

1. You can parse it with _anything_. Throw together a bit of JS or Python talking JSON-over-HTTP  or JSON-over-stdio and go to town! Even bash + jq would do just fine; toss in curl and you have a complete client for the database "backend".
2. It's (arguably) "human-readable"; more importantly, it's _Git-processable_. With a bit of care, your dataset can be continually committed to a shared Git repository, giving you instant backup, replication, and snapshot/rollback capabilities.

It's also an exercise in building a non-trivial "backend service" that can run natively in a WASI-supporting WASM runtime. With access to a durable filesystem, S3 bucket, or any other standard web storage API you can persist your database. You can use git, rsync, or some other more exotic file sync method to keep an arbitrary set of databases backed-up and (read-)replicated.

```
nix develop
just test
just bench
```

## TODO

- [ ] memory storage
- [ ] cli tool
- [ ] network server reference impl
- [ ] wasi target + deno embedding
- [ ] option of cbor/bincode/etc. on-disk format
- [ ] encryption
