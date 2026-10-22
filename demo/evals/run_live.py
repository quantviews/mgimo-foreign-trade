"""Explicit live evaluation; not part of offline CI. Uses an existing user's export access.
Run inside demo container: PYTHONPATH=/app python run_live.py --email EMAIL --cases FILE --output DIR
Review every answer against its rubric; numeric validity alone is not semantic success.
"""
import argparse,asyncio,json,time
import httpx
from contextvars import ContextVar
from pathlib import Path
from app import deepseek,db
from app import analytical_writer
from app.exports import export_context

async def main(args):
 if args.model:deepseek.settings.deepseek_model=args.model
 if args.thinking:deepseek.settings.deepseek_thinking=args.thinking
 cases=json.loads(Path(args.cases).read_text(encoding='utf-8-sig'))
 out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
 pool=await db.init_pool();uid=await pool.fetchval('SELECT id FROM users WHERE email=$1',args.email)
 if uid is None:raise ValueError('Existing user required')
 trace=ContextVar('evaluation_trace')
 original_send=httpx.AsyncClient.send
 async def send(client,request,*a,**kw):
  started=time.monotonic()
  response=await original_send(client,request,*a,**kw)
  if request.url.path.endswith('/chat/completions'):
   record={'status':response.status_code,'seconds':round(time.monotonic()-started,2)}
   try:
    obj=response.json();record.update(model=obj.get('model'),usage=obj.get('usage'),
      finish_reason=(obj.get('choices') or [{}])[0].get('finish_reason'))
   except (ValueError,IndexError):pass
   current=trace.get(None)
   if current is not None:current.setdefault('model_requests',[]).append(record)
  return response
 httpx.AsyncClient.send=send
 original=deepseek.execute_tool; original_verify=deepseek.verify_answer
 original_editor=analytical_writer.json_completion
 async def editor(c,s,p):
  result=await original_editor(c,s,p)
  trace.get().setdefault('editorial',[]).append(
   {'stage':'audit','result':result} if s==analytical_writer.AUDIT else
   {'stage':'write','numeric_gate':analytical_writer.validate_blocks(result.get('blocks'),p['facts'],p.get('question','')),
    'blocks':result.get('blocks')})
  return result
 analytical_writer.json_completion=editor
 async def tool(c,n,a):
  raw=await original(c,n,a)
  trace.get()['calls'].append({'name':n,'args':a})
  return raw
 def verify(draft,facts):
  result=original_verify(draft,facts)
  trace.get()['verifications'].append({'valid':result['valid'],'errors':result['errors']})
  return result
 deepseek.execute_tool=tool;deepseek.verify_answer=verify
 sem=asyncio.Semaphore(args.concurrency)
 async def one(case):
  async with sem:
   history=[];results=[]
   for q in [case['question']]+([case['followup']] if case.get('followup') else []):
    report={'question':q,'requested_model':deepseek.settings.deepseek_model,'thinking':deepseek.settings.deepseek_thinking,'calls':[],'verifications':[]};token=trace.set(report);started=time.monotonic()
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
 p.add_argument('--model');p.add_argument('--concurrency',type=int,default=2)
 p.add_argument('--thinking',choices=['enabled','disabled'])
 asyncio.run(main(p.parse_args()))
