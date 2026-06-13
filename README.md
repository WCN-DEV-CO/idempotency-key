# idempotency-key

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen.svg)](#)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](#)

Make any operation **safe to retry**. Tag a logical request with an idempotency
key — the side effect runs **at most once**, and every retry replays the stored
result. Sync + async. Zero dependencies. Pure standard library.

The pattern every payment API (Stripe, etc.) uses, as a tiny reusable helper.

## Install

```bash
pip install idempotency-key
```

## Quick start

```python
from idempotency_key import IdempotencyManager

mgr = IdempotencyManager()

def charge_card():
    # ... real side effect, e.g. hit a payment API ...
    return {"charged": True, "id": "ch_123"}

# First call runs it; retries replay the same result, never double-charging.
result = mgr.run("order-42-charge", charge_card)
result = mgr.run("order-42-charge", charge_card)  # replayed, not re-run
```

### As a decorator

```python
from idempotency_key import idempotent

@idempotent()
def send_email(user_id, subject):
    # runs once per unique (user_id, subject)
    return deliver(user_id, subject)
```

### Conflict detection

If the same key is reused with a **different** request fingerprint, you get a
`ConflictError` — catching the classic "client retried with changed data" bug.

### TTL + pluggable backends

```python
from idempotency_key import IdempotencyManager, InMemoryStore

mgr = IdempotencyManager(InMemoryStore(ttl_seconds=3600))
```

Implement the 3-method `IdempotencyStore` protocol to back it with Redis, a DB,
or anything else.

## Features

- ✅ At-most-once execution, result replay on retry
- ✅ Sync **and** async (`run_async`)
- ✅ Thread-safe (20 concurrent callers → one execution)
- ✅ Request-fingerprint conflict detection
- ✅ Optional TTL eviction
- ✅ Pluggable store backend
- ✅ **Zero dependencies**, standard library only

## License

MIT © WCN Development Co
