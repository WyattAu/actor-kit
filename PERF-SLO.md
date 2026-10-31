# Performance SLOs: actor-kit
  (4 actors × 12 500 messages, cap-64 mailboxes, 10 consecutive rounds -
  500k cumulative messages; 10/10 green).
  `scheduler_tell`; full registry→mailbox→injector dispatch; 4 165
  `mailbox_config.capacity`; with the 10 000-message default this is
  mailboxes; at the 10 000-message default (~640 KB/actor) it would imply
forever (default capacity 10 000; the "~10k cumulative" threshold).
  was *write-only*; `stats()` always derived `total_messages_processed`
machine otherwise loaded; treat deltas, not absolutes):