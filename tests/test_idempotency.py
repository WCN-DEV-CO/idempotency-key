import asyncio, threading, time
import pytest
from idempotency_key import (IdempotencyManager, InMemoryStore, idempotent,
                             make_key, ConflictError)

def test_runs_once():
    calls=[]
    mgr=IdempotencyManager()
    def op(): calls.append(1); return "done"
    assert mgr.run("k1", op)=="done"
    assert mgr.run("k1", op)=="done"
    assert mgr.run("k1", op)=="done"
    assert len(calls)==1

def test_different_keys_run_separately():
    calls=[]
    mgr=IdempotencyManager()
    mgr.run("a", lambda: calls.append("a"))
    mgr.run("b", lambda: calls.append("b"))
    assert len(calls)==2

def test_result_replayed_exactly():
    mgr=IdempotencyManager()
    r1=mgr.run("k", lambda: {"id":42})
    r2=mgr.run("k", lambda: {"id":99})
    assert r1==r2=={"id":42}

def test_conflict_on_different_fingerprint():
    mgr=IdempotencyManager(strict_fingerprint=True)
    mgr.run("pay", lambda: "ok", fingerprint="fpA")
    with pytest.raises(ConflictError):
        mgr.run("pay", lambda: "ok", fingerprint="fpB")

def test_make_key_deterministic():
    assert make_key("user",1,"charge")==make_key("user",1,"charge")
    assert make_key("user",1)!=make_key("user",2)

def test_decorator_idempotent():
    calls=[]
    mgr=IdempotencyManager()
    @idempotent(mgr)
    def charge(uid, amount): calls.append((uid,amount)); return amount*2
    assert charge(1,50)==100
    assert charge(1,50)==100
    assert len(calls)==1
    assert charge(2,50)==100
    assert len(calls)==2

def test_ttl_eviction():
    store=InMemoryStore(ttl_seconds=0.05)
    mgr=IdempotencyManager(store)
    calls=[]
    mgr.run("k", lambda: calls.append(1))
    time.sleep(0.08)
    mgr.run("k", lambda: calls.append(1))
    assert len(calls)==2

def test_thread_safety_single_execution():
    calls=[]
    mgr=IdempotencyManager()
    def op(): time.sleep(0.005); calls.append(1); return "x"
    threads=[threading.Thread(target=lambda: mgr.run("shared", op)) for _ in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(calls)==1

def test_async_runs_once():
    async def main():
        mgr=IdempotencyManager()
        calls=[]
        async def op(): calls.append(1); return "async-done"
        r1=await mgr.run_async("ak", op)
        r2=await mgr.run_async("ak", op)
        assert r1==r2=="async-done"
        assert len(calls)==1
    asyncio.run(main())
