for hosting **100,000+ actors per node** (with modest per-actor mailboxes -
  `send` waits, `try_send` fails fast; a hot actor can never grow unbounded.
SAFETY comments and documented invariants (module docs). Everything else -
scheduler, mailboxes, supervision, RPC, zero-copy serialization; is 100% safe.
instruction gate (`benches/iai_hot_path.rs`; needs valgrind) and the