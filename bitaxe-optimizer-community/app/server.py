import asyncio,json,os,sqlite3,time
from pathlib import Path
from fastapi import FastAPI,HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel,Field
import httpx

DATA=Path(os.getenv("DATA_DIR","/data")); DATA.mkdir(parents=True,exist_ok=True)
DB=DATA/"optimizer.db"; app=FastAPI(title="Bitaxe Optimizer"); miners={}; tasks={}
DEFAULT={"asic_temp_target":60,"fan_target":40,"min_frequency":400,"max_frequency":650,
"frequency_step":6.25,"min_voltage":850,"max_voltage":1100,"voltage_step":25,
"max_asic_temp":75,"max_vr_temp":80,"max_hw_error":1,"sample_seconds":20,
"deadband_temp":0.5,"deadband_fan":2}

class MinerIn(BaseModel): name:str; host:str
class Settings(BaseModel):
 asic_temp_target:float=Field(60,ge=30,le=85); fan_target:float=Field(40,ge=0,le=100)
 min_frequency:float=Field(300,ge=1); max_frequency:float=Field(700,le=2000)
 frequency_step:float=Field(6.25,gt=0); min_voltage:float=Field(800,ge=1)
 max_voltage:float=Field(1200,le=2000); voltage_step:float=Field(25,gt=0)
 max_asic_temp:float=Field(75,ge=30,le=100); max_vr_temp:float=Field(80,ge=30,le=120)
 max_hw_error:float=Field(1,ge=0,le=100); sample_seconds:int=Field(20,ge=5,le=300)
 deadband_temp:float=Field(.5,ge=0); deadband_fan:float=Field(2,ge=0)

def con(): c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; return c
def init():
 c=con(); c.execute("CREATE TABLE IF NOT EXISTS miners(id INTEGER PRIMARY KEY,name TEXT,host TEXT,settings TEXT)")
 c.execute("CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY,miner_id INTEGER,ts REAL,payload TEXT)")
 for r in c.execute("SELECT * FROM miners"):
  miners[r["id"]]={"id":r["id"],"name":r["name"],"host":r["host"],"settings":json.loads(r["settings"]),"online":False,"telemetry":{},"reason":"Idle"}
 c.commit(); c.close()
async def getj(host,path):
 base=host.rstrip("/"); base=base if base.startswith("http") else "http://"+base
 async with httpx.AsyncClient(timeout=5) as x:
  r=await x.get(base+path); r.raise_for_status(); return r.json()
async def patch(host,data):
 base=host.rstrip("/"); base=base if base.startswith("http") else "http://"+base
 async with httpx.AsyncClient(timeout=5) as x:
  r=await x.patch(base+"/api/system",json=data); r.raise_for_status(); return r.json() if r.content else {}
def n(d,*ks):
 for k in ks:
  if isinstance(d.get(k),(int,float)): return float(d[k])
def norm(info,asic):
 d={}; d.update(info if isinstance(info,dict) else {}); d.update(asic if isinstance(asic,dict) else {})
 p=n(d,"power","powerWatts"); h=n(d,"hashRate","hashrate","hash_rate")
 return {"temp":n(d,"temp","temperature","asicTemp","asic_temp"),"fan":n(d,"fanPercent","fan_percent","fan"),
 "hashrate":h,"power":p,"vr_temp":n(d,"vrTemp","vr_temp","vrTemperature"),
 "hw_error":n(d,"hwErrors","hw_errors","errorRate","error_rate"),
 "frequency":n(d,"frequency","freq"),"voltage":n(d,"coreVoltage","voltage","core_voltage"),
 "jth":p/h if p and h and h>0 else None}
def score(t,s):
 if t.get("temp") is None or t.get("fan") is None:return 1e9
 return .5*abs(t["temp"]-s["asic_temp_target"])/max(1,s["asic_temp_target"]*.1)+.5*abs(t["fan"]-s["fan_target"])/20
def safe(t,s):
 return not ((t.get("temp") is not None and t["temp"]>=s["max_asic_temp"]) or
 (t.get("vr_temp") is not None and t["vr_temp"]>=s["max_vr_temp"]) or
 (t.get("hw_error") is not None and t["hw_error"]>s["max_hw_error"]))
async def poll(mid):
 while mid in miners:
  m=miners[mid]
  try:
   t=norm(await getj(m["host"],"/api/system/info"),await getj(m["host"],"/api/system/asic"))
   m["telemetry"]=t;m["online"]=True
   c=con();c.execute("INSERT INTO samples(miner_id,ts,payload) VALUES(?,?,?)",(mid,time.time(),json.dumps(t)));c.commit();c.close()
  except Exception:m["online"]=False;m["telemetry"]={}
  await asyncio.sleep(5)
async def optimize(mid):
 m=miners[mid]
 while mid in miners:
  s=m["settings"];t=m["telemetry"]
  if not m["online"] or not t: m["reason"]="Waiting for telemetry";await asyncio.sleep(5);continue
  f,v=t.get("frequency"),t.get("voltage")
  if not safe(t,s):
   m["reason"]="Safety limit exceeded — reducing operating point"
   try: await patch(m["host"],{"frequency":max(s["min_frequency"],(f or s["min_frequency"])-s["frequency_step"]),"coreVoltage":max(s["min_voltage"],(v or s["min_voltage"])-s["voltage_step"])})
   except Exception: pass
   await asyncio.sleep(s["sample_seconds"]);continue
  if f is None or v is None: m["reason"]="Reading current frequency/voltage";await asyncio.sleep(5);continue
  base=score(t,s); basej=t.get("jth") or 1e9; best=None
  for reason,cf,cv in [("frequency ↓",max(s["min_frequency"],f-s["frequency_step"]),v),("frequency ↑",min(s["max_frequency"],f+s["frequency_step"]),v),("voltage ↓",f,max(s["min_voltage"],v-s["voltage_step"])),("voltage ↑",f,min(s["max_voltage"],v+s["voltage_step"]))]:
   if cf==f and cv==v: continue
   try: await patch(m["host"],{"frequency":cf,"coreVoltage":cv})
   except Exception: continue
   m["reason"]="Testing "+reason;await asyncio.sleep(s["sample_seconds"]);tt=m["telemetry"]
   if not safe(tt,s):continue
   sc=score(tt,s);jj=tt.get("jth") or 1e9
   if sc<base-.03 or (sc<=base+.08 and jj<basej*.995): best=(reason,cf,cv,sc)
  if best:
   r,bf,bv,_=best;await patch(m["host"],{"frequency":bf,"coreVoltage":bv});m["reason"]="Exploring efficiency — targets are within range" if _<=base+.08 else "Adjusting "+r+" to improve target balance"
  else:
   try: await patch(m["host"],{"frequency":f,"coreVoltage":v})
   except Exception:pass
   m["reason"]="Holding — no better nearby operating point found"
  await asyncio.sleep(s["sample_seconds"])
@app.on_event("startup")
async def start():
 init()
 for mid in list(miners):asyncio.create_task(poll(mid))
@app.get("/")
async def index():return FileResponse(Path(__file__).parent/"static/index.html")
@app.get("/api/miners")
async def ls():return list(miners.values())
@app.post("/api/miners")
async def add(x:MinerIn):
 c=con();q=c.execute("INSERT INTO miners(name,host,settings) VALUES(?,?,?)",(x.name,x.host,json.dumps(DEFAULT)));mid=q.lastrowid;c.commit();c.close()
 miners[mid]={"id":mid,"name":x.name,"host":x.host,"settings":dict(DEFAULT),"online":False,"telemetry":{},"reason":"Starting telemetry"};asyncio.create_task(poll(mid));return miners[mid]
@app.delete("/api/miners/{mid}")
async def rem(mid:int):
 if mid not in miners:raise HTTPException(404)
 if mid in tasks:tasks[mid].cancel()
 c=con();c.execute("DELETE FROM samples WHERE miner_id=?",(mid,));c.execute("DELETE FROM miners WHERE id=?",(mid,));c.commit();c.close();miners.pop(mid);return {"ok":True}
@app.get("/api/miners/{mid}/settings")
async def gs(mid:int):return miners[mid]["settings"]
@app.put("/api/miners/{mid}/settings")
async def ss(mid:int,x:Settings):
 miners[mid]["settings"]=x.model_dump();c=con();c.execute("UPDATE miners SET settings=? WHERE id=?",(json.dumps(miners[mid]["settings"]),mid));c.commit();c.close();return miners[mid]["settings"]
@app.post("/api/miners/{mid}/optimize")
async def go(mid:int):
 if mid not in miners:raise HTTPException(404)
 if mid not in tasks or tasks[mid].done():tasks[mid]=asyncio.create_task(optimize(mid))
 return {"running":True}
@app.post("/api/miners/{mid}/stop")
async def stop(mid:int):
 t=tasks.pop(mid,None)
 if t:t.cancel()
 miners[mid]["reason"]="Optimizer paused";return {"running":False}
@app.get("/api/miners/{mid}/samples")
async def samples(mid:int):
 c=con();rows=c.execute("SELECT ts,payload FROM samples WHERE miner_id=? ORDER BY ts DESC LIMIT 300",(mid,)).fetchall();c.close()
 return [{"ts":r["ts"],**json.loads(r["payload"])} for r in reversed(rows)]
