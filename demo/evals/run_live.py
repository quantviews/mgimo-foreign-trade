"""Explicit live evaluation; not part of offline CI. Uses an existing user's export access.
Run inside demo container: PYTHONPATH=/app python run_live.py --email EMAIL --cases FILE --output DIR
Review every answer against its rubric; numeric validity alone is not semantic success.
"""
import argparse,asyncio,json,time
from contextvars import ContextVar
from pathlib import Path
from app import deepseek,db
from app.exports import export_context

async def main(args):
 cases=json.loads(Path(args.cases).read_text(encoding='utf-8-sig'))
 out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
 pool=await db.init_pool();uid=await pool.fetchval('SELECT id FROM users WHERE email=$1',args.email)
 if uid is None:raise ValueError('Existing user required')
 trace=ContextVar('evaluation_trace')
 original=deepseek.execute_tool; original_verify=deepseek.verify_answer
 async def tool(c,n,a):
  raw=await original(c,n,a)
  trace.get()['calls'].append({'name':n,'args':a})
  return raw
 def verify(draft,facts):
  result=original_verify(draft,facts)
  trace.get()['verifications'].append({'valid':result['valid'],'errors':result['errors']})
  return result
 deepseek.execute_tool=tool;deepseek.verify_answer=verify
 sem=asyncio.Semaphore(2)
 async def one(case):
  async with sem:
   history=[];results=[]
   for q in [case['question']]+([case['followup']] if case.get('followup') else []):
    report={'question':q,'calls':[],'verifications':[]};token=trace.set(report);started=time.monotonic()
    try:
     history.append({'role':'user','content':q})
     events=[e async for e in deepseek.run_agent(history,user_id=uid)]
     report['events']=events
     answer='\n'.join(e['text'] for e in events if e['type']=='answer')
     history.append({'role':'assistant','content':answer})
    except Exception as exc:report['exception']=type(exc).__name__
    finally:trace.reset(token)
    report['seconds']=round(time.monotonic()-started,1);results.append(report)
   Path(out/(case['id']+'.json')).write_text(json.dumps({'case':case,'turns':results},ensure_ascii=False,indent=2),encoding='utf-8')
   print(case['id'],[[(e['type']) for e in r.get('events',[])] for r in results],flush=True)
 try:await asyncio.gather(*(one(c) for c in cases))
 finally:await db.close_pool()

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--email',required=True);p.add_argument('--cases',required=True);p.add_argument('--output',required=True)
 asyncio.run(main(p.parse_args()))
