#!/usr/bin/env python3
"""Second benchmark suite: repeated runs with mean/sd, WAL cost, kill -9 crash recovery, real CPU jobs."""
import json, subprocess, time, os, signal, statistics as st, sys, urllib.request, tempfile
ADDR="127.0.0.1:8097"; URL="http://"+ADDR
def start(*flags):
    p=subprocess.Popen(["bin/orchd","-addr",ADDR,*flags],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    for _ in range(100):
        try: urllib.request.urlopen(URL+"/healthz",timeout=0.2); return p
        except Exception: time.sleep(0.05)
    raise SystemExit("server did not start")
def stop(p): p.kill(); p.wait()
def load(label,**kw):
    a=["bin/loadgen","-url",URL,"-label",label]
    for k,v in kw.items(): a+=["-"+k.replace("_","-"),str(v)]
    return json.loads(subprocess.check_output(a))
def rep(reps,flags,label,**kw):
    rs=[]
    for i in range(reps):
        p=start(*flags)
        try: rs.append(load(label,**kw))
        finally: stop(p)
    return rs
def ms(vals): return f"{st.mean(vals):.2f} +/- {st.pstdev(vals):.2f}"
out=[]
def say(s): print(s); out.append(s)
N=3
say(f"# Repeated runs, N={N} each, mean +/- population sd")
r=rep(N,["-workers","16"],"base16",n=2000,work_ms=20)
say(f"16 workers, 2000x20ms: throughput jobs/s {ms([x['throughput_jobs_s'] for x in r])}")
r=rep(N,["-workers","16","-retries=false"],"noretry",n=2000,work_ms=20,fail=0.2)
say(f"20% fail, retries OFF: completion % {ms([x['completion_pct'] for x in r])}; total s {ms([x['total_s'] for x in r])}")
r=rep(N,["-workers","16","-max-attempts","5"],"retry",n=2000,work_ms=20,fail=0.2)
say(f"20% fail, retries ON : completion % {ms([x['completion_pct'] for x in r])}; total s {ms([x['total_s'] for x in r])}")
r=rep(N,["-workers","16","-healthcheck=false"],"nohc",n=1000,work_ms=20,hang=0.01,timeout="15s")
say(f"1% hang, health check OFF: completion % {ms([x['completion_pct'] for x in r])}")
r=rep(N,["-workers","16"],"hc",n=1000,work_ms=20,hang=0.01,timeout="15s")
say(f"1% hang, health check ON : completion % {ms([x['completion_pct'] for x in r])}; total s {ms([x['total_s'] for x in r])}")

say("# Write-ahead log cost (16 workers, 2000 jobs x 20ms, 32 submit clients)")
d=tempfile.mkdtemp()
for name,flags in [("no WAL",[]),("WAL, no fsync",["-wal",d+"/a.wal","-wal-sync=false"]),("WAL, fsync per submit",["-wal",d+"/b.wal"])]:
    rs=[]
    for i in range(3):
        for f in os.listdir(d): os.remove(d+"/"+f)
        p=start("-workers","16",*flags)
        try: rs.append(load(name,n=2000,work_ms=20))
        finally: stop(p)
    say(f"{name}: submit phase s {ms([x['submit_s'] for x in rs])}; total s {ms([x['total_s'] for x in rs])}")

say("# kill -9 crash recovery (WAL with fsync): submit 3000 jobs x 30ms, kill -9 at ~25% done, restart, drain")
for f in os.listdir(d): os.remove(d+"/"+f)
w=d+"/crash.wal"; p=start("-workers","16","-wal",w)
sub=subprocess.Popen(["bin/loadgen","-url",URL,"-n","3000","-c","32","-work-ms","30","-timeout","1s"],stdout=subprocess.DEVNULL)
sub.wait()
while True:
    s=json.load(urllib.request.urlopen(URL+"/stats"))
    if s["done"]>=700: break
    time.sleep(0.05)
done_before=s["done"]; stop(p)
p=start("-workers","16","-wal",w)
t0=time.time()
while True:
    s=json.load(urllib.request.urlopen(URL+"/stats"))
    if s["queued"]==0 and s["done"]+s["dead"]>=1 and time.time()-t0>0.5:
        # wait until no running jobs: done+dead stable
        time.sleep(0.5); s2=json.load(urllib.request.urlopen(URL+"/stats"))
        if s2["done"]==s["done"] and s2["queued"]==0: break
    if time.time()-t0>60: break
    time.sleep(0.1)
stop(p)
subs=set(); fin=set(); runs=0
for line in open(w):
    try: r=json.loads(line)
    except Exception: continue
    if r["op"]=="sub": subs.add(r["job"]["id"])
    elif r["op"] in("done","dead"): fin.add(r["id"]); 
say(f"jobs acknowledged: {len(subs)}; done at kill time: {done_before}; after restart, ids with a done/dead record: {len(fin & subs)}; lost: {len(subs-fin)}")

say("# Real CPU jobs (chained SHA-256), 200 jobs x 500,000 rounds, no sleep; machine has %d cores"%os.cpu_count())
for wk in (1,2,4,16):
    rs=[]
    for i in range(2):
        p=start("-workers",str(wk))
        try:
            a=["bin/loadgen","-url",URL,"-n","400","-c","16","-work-ms","0","-timeout","120s","-label","cpu"]
            # loadgen has no cpu flag; post jobs directly
            t0=time.time()
            import concurrent.futures as cf
            def post(_):
                req=urllib.request.Request(URL+"/jobs",data=json.dumps({"cpu_iters":500000}).encode(),headers={"Content-Type":"application/json"})
                urllib.request.urlopen(req).read()
            with cf.ThreadPoolExecutor(16) as ex: list(ex.map(post,range(200)))
            while json.load(urllib.request.urlopen(URL+"/stats"))["done"]<200: time.sleep(0.05)
            rs.append(200/(time.time()-t0))
        finally: stop(p)
    say(f"workers={wk}: jobs/s {ms(rs)}")
open("results2.txt","w").write("\n".join(out)+"\n")
