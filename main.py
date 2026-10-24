"""STM PRO BOT - compact single-file application.
Signal-only bot; it never places trades. Uses real provider data only in pyquotex mode.
"""
import asyncio, json, logging, os, sqlite3, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes
from strategy import evaluate, validate_candles

load_dotenv()
logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO').upper(),format='%(asctime)s %(levelname)s %(name)s: %(message)s')
log=logging.getLogger('stmpro')
CATEGORIES=['FOREX','CURRENCY','CRYPTO','COMMODITIES']
STRATEGIES=['AI','STM','RANDOM','BLACK4X','OPX4O']

class Settings:
    def __init__(self):
        self.token=os.getenv('TELEGRAM_BOT_TOKEN','').strip()
        self.email=os.getenv('QUOTEX_EMAIL','').strip(); self.password=os.getenv('QUOTEX_PASSWORD','').strip()
        self.mode=os.getenv('PROVIDER_MODE','pyquotex').strip().lower()
        self.admin_ids={int(x.strip()) for x in os.getenv('ADMIN_IDS','').split(',') if x.strip().isdigit()}
        self.channel=os.getenv('SIGNAL_CHANNEL_ID',os.getenv('CHANNEL_ID','')).strip()
        self.membership_channel=os.getenv('CHANNEL_ID','').strip()
        self.channel_username=os.getenv('CHANNEL_USERNAME','').strip(); self.channel_url=os.getenv('CHANNEL_URL','https://t.me/').strip()
        self.admin_username=os.getenv('ADMIN_USERNAME','admin').strip().lstrip('@')
        self.feedback=os.getenv('FEEDBACK_CONTACT','not configured').strip()
        self.referral=os.getenv('QUOTEX_REFERRAL_URL','').strip()
        self.payout=float(os.getenv('PAYOUT_THRESHOLD','80')); self.tz=os.getenv('TIMEZONE','Indian/Maldives')
        self.interval=max(1.0,float(os.getenv('SCAN_INTERVAL_SECONDS','5'))); self.concurrency=max(1,int(os.getenv('MAX_CONCURRENCY','4')))
        self.confidence=max(0,min(100,int(os.getenv('MIN_CONFIDENCE','55'))))
        self.enabled={x.strip().upper() for x in os.getenv('ENABLED_STRATEGIES','AI,STM,BLACK4X,OPX4O').split(',') if x.strip()}
        if not self.token: raise ValueError('TELEGRAM_BOT_TOKEN is missing in .env')
        if self.mode not in {'pyquotex','mock'}: raise ValueError('PROVIDER_MODE must be pyquotex or mock')
        if not 0<=self.payout<=100: raise ValueError('PAYOUT_THRESHOLD must be between 0 and 100')
        ZoneInfo(self.tz)
        if self.mode=='pyquotex' and (not self.email or not self.password): raise ValueError('QUOTEX_EMAIL and QUOTEX_PASSWORD are required for pyquotex mode')

class Store:
    """SQLite is embedded here so no separate database.py/module is required."""
    def __init__(self,path='stm_pro.sqlite3'):
        self.path=path
        with self.db() as c:
            c.executescript('''CREATE TABLE IF NOT EXISTS users(uid INTEGER PRIMARY KEY, verified INTEGER DEFAULT 0, categories TEXT DEFAULT '[]', pairs TEXT DEFAULT '[]', strategies TEXT DEFAULT '[]');
            CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, pair TEXT, category TEXT, strategy TEXT, direction TEXT, signal_at TEXT, entry_at TEXT, expiry_at TEXT, payout REAL, confidence INTEGER, reason TEXT, reference_price REAL, status TEXT DEFAULT 'PENDING', open_price REAL, close_price REAL, mtg_open REAL, mtg_close REAL, result TEXT, dedupe TEXT UNIQUE, channel_posted INTEGER DEFAULT 0, result_posted INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY,v TEXT);''')
    def db(self):
        c=sqlite3.connect(self.path,timeout=20); c.row_factory=sqlite3.Row; c.execute('PRAGMA journal_mode=WAL'); return c
    def user(self,uid):
        with self.db() as c:r=c.execute('SELECT * FROM users WHERE uid=?',(uid,)).fetchone()
        if not r:return {'uid':uid,'verified':0,'categories':[],'pairs':[],'strategies':[]}
        d=dict(r)
        for k in ('categories','pairs','strategies'):d[k]=json.loads(d[k] or '[]')
        return d
    def save_user(self,uid,**kw):
        with self.db() as c:
            c.execute('INSERT OR IGNORE INTO users(uid) VALUES(?)',(uid,))
            for k,v in kw.items():
                if k=='verified':c.execute('UPDATE users SET verified=? WHERE uid=?',(int(bool(v)),uid))
                elif k in {'categories','pairs','strategies'}:c.execute(f'UPDATE users SET {k}=? WHERE uid=?',(json.dumps(v),uid))
    def setting(self,k,default=None):
        with self.db() as c:r=c.execute('SELECT v FROM settings WHERE k=?',(k,)).fetchone()
        return json.loads(r['v']) if r else default
    def set_setting(self,k,v):
        with self.db() as c:c.execute('INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',(k,json.dumps(v)))
    def add_signal(self,s):
        with self.db() as c:
            cur=c.execute('''INSERT OR IGNORE INTO signals(pair,category,strategy,direction,signal_at,entry_at,expiry_at,payout,confidence,reason,reference_price,dedupe) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',tuple(s[k] for k in ('pair','category','strategy','direction','signal_at','entry_at','expiry_at','payout','confidence','reason','reference_price','dedupe')))
            if not cur.rowcount:return None
            return cur.lastrowid
    def open_signals(self):
        with self.db() as c:return [dict(r) for r in c.execute("SELECT * FROM signals WHERE result IS NULL ORDER BY id")]
    def resolve(self,sid,**kw):
        with self.db() as c:c.execute('UPDATE signals SET '+','.join(f'{k}=?' for k in kw)+' WHERE id=?',(*kw.values(),sid))
    def stats(self):
        with self.db() as c:r=c.execute("SELECT result,COUNT(*) n FROM signals WHERE result IS NOT NULL GROUP BY result").fetchall()
        d={x['result']:x['n'] for x in r}; w=d.get('WIN',0)+d.get('MTG WIN',0); l=d.get('LOSS',0); return {'wins':w,'losses':l,'accuracy':round(100*w/(w+l),2) if w+l else 0,'resolved':w+l}

class QuotexProvider:
    def __init__(self,cfg):self.cfg=cfg;self.client=None;self.lock=asyncio.Semaphore(cfg.concurrency)
    async def connect(self):
        if self.cfg.mode=='mock':log.warning('MOCK MODE: synthetic candles; do not treat results as live performance.');return
        try:
            from pyquotex.stable_api import Quotex
            self.client=Quotex(email=self.cfg.email,password=self.cfg.password,lang='en')
            result=await self.client.connect()
            if isinstance(result,tuple) and result and result[0] is False:raise RuntimeError('Quotex login returned failure')
        except Exception as e: self.client=None; raise RuntimeError(f'PyQuotex connection failed/incompatible: {type(e).__name__}: {e}') from e
    async def close(self):
        if self.client:
            try:await self.client.close()
            except Exception:log.exception('Error closing Quotex client')
    def _categorize(self,name):
        n=name.upper().replace('/','').replace(' ','')
        if 'OTC' in n:
            if any(x in n for x in ('BTC','ETH','LTC','XRP','DOGE','ADA','SOL','DOT','BCH','BNB','TRX','LINK','XLM','AVAX','ETC')):return 'CRYPTO'
            if any(x in n for x in ('GOLD','SILVER','XAU','XAG','UKBRENT','USCRUDE','BRENT','WTI')):return 'COMMODITIES'
            return 'CURRENCY'
        if any(x in n for x in ('BTC','ETH','LTC','XRP','DOGE','ADA','SOL','DOT','BCH','BNB','TRX','LINK','XLM','AVAX','ETC')):return 'CRYPTO'
        if any(x in n for x in ('GOLD','SILVER','XAU','XAG','UKBRENT','USCRUDE','BRENT','WTI','OIL')):return 'COMMODITIES'
        return 'FOREX'
    async def pairs(self):
        if self.cfg.mode=='mock':return [{'pair':p,'category':self._categorize(p)} for p in ['EURUSD','GBPUSD','USDJPY','AUDUSD','EURUSD-OTC','GBPUSD-OTC','USDIDR-OTC','BTCUSD','ETHUSD','Gold-OTC','UKBrent-OTC','Silver-OTC','USCrude-OTC']]
        if not self.client:raise RuntimeError('Quotex is not connected')
        candidates=[]
        for obj,name in [(self.client,'get_all_asset_name'),(self.client,'get_available_asset'),(self.client,'get_payment')]:
            fn=getattr(obj,name,None)
            if callable(fn):
                try:
                    val=fn(); val=await val if hasattr(val,'__await__') else val
                    if val:candidates.append(val)
                except Exception:log.debug('Asset discovery %s unavailable',name,exc_info=True)
        api=getattr(self.client,'api',None)
        for obj,name in [(api,'instruments'),(api,'assets')]:
            val=getattr(obj,name,None) if obj else None
            if val:candidates.append(val)
        found={}
        def consume(val):
            if isinstance(val,dict):
                for k,v in val.items():
                    if isinstance(v,dict):
                        name=v.get('name') or v.get('asset') or v.get('symbol') or (k if isinstance(k,str) else None)
                        if name:found[str(name)]=1
                    elif isinstance(k,str) and any(ch.isalpha() for ch in k):found[k]=1
                    elif isinstance(v,str) and any(ch.isalpha() for ch in v):found[v]=1
                    elif isinstance(v,(list,tuple,set)):consume(v)
            elif isinstance(val,(list,tuple,set)):
                for v in val:
                    if isinstance(v,str):found[v]=1
                    elif isinstance(v,dict):
                        name=v.get('name') or v.get('asset') or v.get('symbol')
                        if name:found[str(name)]=1
                    elif isinstance(v,(list,tuple,set)):consume(v)
        for val in candidates:consume(val)
        return [{'pair':p,'category':self._categorize(p)} for p in sorted(found) if p and len(p)<40]
    async def candles(self,pair,limit=150):
        if self.cfg.mode=='mock':
            import numpy as np
            now=int(time.time())//60*60; times=np.arange(now-(limit-1)*60,now+1,60); base=150 if 'JPY' in pair else 1.1
            close=base+np.cumsum(np.sin(np.arange(len(times))/4)*.0002+np.cos(np.arange(len(times))/9)*.0001); op=np.r_[close[0],close[:-1]]
            return pd.DataFrame({'time':pd.to_datetime(times,unit='s',utc=True),'open':op,'high':np.maximum(op,close)+.00015,'low':np.minimum(op,close)-.00015,'close':close,'volume':1})
        if not self.client:raise RuntimeError('Quotex is not connected')
        async with self.lock:
            raw=await self.client.get_candles(pair,int(time.time()),limit*60,60)
        if not raw:raise RuntimeError(f'No candles for {pair}')
        df=pd.DataFrame(raw)
        if not {'time','open','high','low','close'}.issubset(df.columns):raise RuntimeError(f'Unknown candle schema for {pair}: {list(df.columns)}')
        df['time']=pd.to_datetime(df.time,unit='s',utc=True,errors='coerce')
        for col in ('open','high','low','close'):df[col]=pd.to_numeric(df[col],errors='coerce')
        return df.dropna(subset=['time','open','high','low','close']).sort_values('time').drop_duplicates('time').tail(limit).reset_index(drop=True)
    async def payout(self,pair):
        if self.cfg.mode=='mock':return 85.0
        # Use only provider-reported payout metadata; no invented/default payout.
        for obj,name in [(self.client,'get_payment'),(self.client,'get_payout'),(getattr(self.client,'api',None),'get_payment')]:
            fn=getattr(obj,name,None) if obj else None
            if not callable(fn):continue
            try:
                val=fn();val=await val if hasattr(val,'__await__') else val
                if isinstance(val,dict):
                    for k,v in val.items():
                        if str(k).replace('_','').replace('-','').upper()==pair.replace('_','').replace('-','').upper() or (isinstance(v,dict) and str(v.get('name','')).upper()==pair.upper()):
                            x=v.get('payout',v.get('payment',v.get('percent'))) if isinstance(v,dict) else v
                            if isinstance(x,(int,float)) and 0<=x<=100:return float(x*100 if 0 < x <= 1 else x)
                if isinstance(val,(int,float)) and 0<=val<=100:return float(val*100 if 0 < val <= 1 else val)
            except Exception:log.debug('Payout endpoint %s unavailable',name,exc_info=True)
        return None

class Bot:
    def __init__(self,cfg,store,provider):self.cfg=cfg;self.db=store;self.provider=provider;self.running=False;self.task=None;self.scan_lock=asyncio.Lock();self.limit=None;self.sent_count=0
    def admin(self,uid):return uid in self.cfg.admin_ids
    def local(self,iso):return datetime.fromisoformat(iso).astimezone(ZoneInfo(self.cfg.tz)).strftime('%Y-%m-%d %H:%M:%S %Z')
    def signal_text(self,s):
        return (f'🤖 STM PRO BOT\n━━━━━━━━━━━━━━━━━━━━\n💱 Pair: {s["pair"]}\n📂 Category: {s["category"]}\n⏱️ Trade at: {self.local(s["entry_at"])}\n🔮 Direction: {s["direction"]}\n💰 Verified payout: {s["payout"]:.0f}%\n⏳ Expiry: 1 minute\n🎯 Confluence score: {s["confidence"]}%\n🧠 Strategy: {s["strategy"]}\n📝 Reason: {s["reason"]}\n⚠️ Signal only; risk remains and results are not guaranteed.')
    def result_text(self,s):
        st=self.db.stats();op=s.get('open_price');cp=s.get('close_price')
        return (f'📊 STM PRO BOT RESULT\n━━━━━━━━━━━━━━━━━━━━\n💱 Pair: {s["pair"]}\n⏱️ Entry: {self.local(s["entry_at"])}\n🔮 Direction: {s["direction"]}\n📈 Opening: {op if op is not None else "UNAVAILABLE"}\n📉 Closing: {cp if cp is not None else "UNAVAILABLE"}\n🏁 Result: {s.get("result","PENDING")}\n🔁 MTG open/close: {s.get("mtg_open")}/{s.get("mtg_close")}\n━━━━━━━━━━━━━━━━━━━━\n🏆 Wins: {st["wins"]} | ❌ Losses: {st["losses"]}\n📊 Verified accuracy: {st["accuracy"]}%\n📩 Feedback: {self.cfg.feedback}')
    async def publish(self,app,s):
        msg=self.signal_text(s)
        if self.cfg.channel:
            try:await app.bot.send_message(self.cfg.channel,msg)
            except Exception:log.exception('Could not post signal to channel')
        with self.db.db() as c: users=[dict(r) for r in c.execute('SELECT * FROM users WHERE verified=1')]
        for u in users:
            cats=json.loads(u['categories'] or '[]');pairs=json.loads(u['pairs'] or '[]');sts=json.loads(u['strategies'] or '[]')
            if cats and s['category'] not in cats:continue
            if pairs and s['pair'] not in pairs:continue
            if sts and s['strategy'] not in sts:continue
            try:await app.bot.send_message(int(u['uid']),msg)
            except Exception:log.debug('Signal DM unavailable for user %s',u['uid'])
    async def evaluate_pair(self,p,st,app):
        try:
            now=datetime.now(timezone.utc)
            # Only emit during the final 10 seconds before the next minute.
            if now.second<50:return
            df=await self.provider.candles(p['pair'],150)
            if df is None:return
            df=df.copy(); df['time']=pd.to_datetime(df.time,utc=True,errors='coerce')
            now=pd.Timestamp.now(tz='UTC'); df=df.loc[df.time+pd.Timedelta(seconds=60)<=now].reset_index(drop=True)
            ok,_=validate_candles(df,60)
            if not ok:return
            age=(now-(df.time.iloc[-1]+pd.Timedelta(seconds=60))).total_seconds()
            if age>90:return
            payout=await self.provider.payout(p['pair'])
            if payout is None or payout<self.cfg.payout:return
            cand=evaluate(st,df,{'allow_random':self.cfg.mode=='mock'})
            if not cand or cand.confidence<self.cfg.confidence:return
            now=datetime.now(timezone.utc)
            if now.second<50:return
            entry=(now+timedelta(minutes=1)).replace(second=0,microsecond=0); expiry=entry+timedelta(minutes=1)
            import hashlib
            key=hashlib.sha256(f'{p["pair"]}|{entry.isoformat()}|{cand.strategy}'.encode()).hexdigest()
            row={'pair':p['pair'],'category':p['category'],'strategy':cand.strategy,'direction':cand.direction,'signal_at':now.isoformat(),'entry_at':entry.isoformat(),'expiry_at':expiry.isoformat(),'payout':float(payout),'confidence':int(cand.confidence),'reason':cand.reason,'reference_price':float(df.close.iloc[-1]),'dedupe':key}
            sid=self.db.add_signal(row)
            if not sid:return
            row['id']=sid;await self.publish(app,row);self.sent_count+=1
            log.info('Signal published id=%s pair=%s strategy=%s',sid,row['pair'],row['strategy'])
        except Exception:log.exception('Scan candidate failed for %s/%s',p.get('pair'),st)
    async def scan(self,app):
        async with self.scan_lock:
            pairs=await self.provider.pairs()
            if not pairs:log.warning('Provider returned no available pairs; check pyquotex asset discovery/API version.');return
            tasks=[]
            for p in pairs:
                if p['category'] not in CATEGORIES:continue
                for st in self.cfg.enabled:
                    if st in STRATEGIES:tasks.append(self.evaluate_pair(p,st,app))
            await asyncio.gather(*tasks)
    async def resolve_results(self,app):
        for s in self.db.open_signals():
            try:
                entry=pd.Timestamp(s['entry_at']); expiry=pd.Timestamp(s['expiry_at']); now=pd.Timestamp.now(tz='UTC')
                if now < expiry:continue
                df=await self.provider.candles(s['pair'],180)
                if df is None or df.empty:continue
                df=df.copy();df['time']=pd.to_datetime(df.time,utc=True,errors='coerce')
                # exact candle-start matching avoids guessing which candle was traded
                first=df.loc[df.time==entry]
                if first.empty:continue
                a=first.iloc[0]; op=float(a.open);cp=float(a.close)
                if cp==op:result='TIE'
                else:result='WIN' if ((s['direction']=='CALL' and cp>op) or (s['direction']=='PUT' and cp<op)) else 'LOSS'
                updates={'open_price':op,'close_price':cp,'status':result,'result':result}
                if result=='LOSS':
                    mtg_time=expiry
                    if now < mtg_time+pd.Timedelta(seconds=60):
                        # Wait for the next complete candle; preserve initial LOSS as pending for MTG classification.
                        continue
                    second=df.loc[df.time==mtg_time]
                    if second.empty:continue
                    b=second.iloc[0]; mop=float(b.open);mcp=float(b.close)
                    mw='WIN' if ((s['direction']=='CALL' and mcp>mop) or (s['direction']=='PUT' and mcp<mop)) else 'LOSS'
                    if mcp==mop:mw='TIE'
                    result='MTG WIN' if mw=='WIN' else 'LOSS'
                    updates.update(mtg_open=mop,mtg_close=mcp,status=result,result=result)
                self.db.resolve(s['id'],**updates)
                with self.db.db() as c: saved=dict(c.execute('SELECT * FROM signals WHERE id=?',(s['id'],)).fetchone())
                msg=self.result_text(saved)
                if self.cfg.channel:
                    try:await app.bot.send_message(self.cfg.channel,msg)
                    except Exception:log.exception('Could not publish result to channel')
                with self.db.db() as c: users=[int(r['uid']) for r in c.execute('SELECT uid FROM users WHERE verified=1')]
                for uid in users:
                    try:await app.bot.send_message(uid,msg)
                    except Exception:pass
                log.info('Result resolved id=%s result=%s',s['id'],result)
            except Exception:log.exception('Could not resolve result id=%s',s.get('id'))
    async def loop(self,app):
        while self.running:
            try:await self.resolve_results(app)
            except Exception:log.exception('Result cycle failed')
            if self.running:
                try:await self.scan(app)
                except Exception:log.exception('Scan cycle failed')
            if self.limit and self.sent_count>=self.limit:
                self.limit=None;self.running=False;self.db.set_setting('running',False)
            await asyncio.sleep(self.cfg.interval)
    async def start(self,app,limit=None):
        self.limit=limit;self.sent_count=0;self.running=True;self.db.set_setting('running',True)
        if self.task and not self.task.done():return
        self.task=asyncio.create_task(self.loop(app),name='stmpro-scan-loop')
    async def stop(self,persist=True):
        self.running=False
        if persist:self.db.set_setting('running',False)
        if self.task and not self.task.done():self.task.cancel()
        self.task=None

# Telegram UI helpers

def categories_kb(selected):
    rows=[]
    for i in range(0,4,2):rows.append([InlineKeyboardButton(('✅ ' if c in selected else '')+c,callback_data='cat:'+c) for c in CATEGORIES[i:i+2]])
    rows.append([InlineKeyboardButton('SELECT ALL',callback_data='cat:ALL'),InlineKeyboardButton('Continue → Pairs',callback_data='pairs:0')])
    return InlineKeyboardMarkup(rows)
def pairs_kb(pairs,selected,page=0):
    chunk=pairs[page*8:(page+1)*8];rows=[[InlineKeyboardButton(('✅ ' if p['pair'] in selected else '')+p['pair'],callback_data='pair:'+p['pair'])] for p in chunk]
    nav=[]
    if page>0:nav.append(InlineKeyboardButton('◀ Prev',callback_data=f'pairs:{page-1}'))
    if (page+1)*8<len(pairs):nav.append(InlineKeyboardButton('Next ▶',callback_data=f'pairs:{page+1}'))
    if nav:rows.append(nav)
    rows.append([InlineKeyboardButton('SELECT ALL PAIRS',callback_data='pairs:ALL'),InlineKeyboardButton('Continue → Strategy',callback_data='strategies')])
    return InlineKeyboardMarkup(rows)
def strategies_kb(selected):
    rows=[[InlineKeyboardButton(('✅ ' if s in selected else '')+s,callback_data='strat:'+s)] for s in STRATEGIES]
    rows.append([InlineKeyboardButton('SELECT ALL',callback_data='strat:ALL'),InlineKeyboardButton('Save selection',callback_data='save')])
    return InlineKeyboardMarkup(rows)
def uto_kb():return InlineKeyboardMarkup([[InlineKeyboardButton(str(n),callback_data='uto:'+str(n)) for n in (10,20,25,30,40)]])

async def start_cmd(update,ctx):
    app=ctx.application; bot=app.bot_data['bot'];u=update.effective_user;bot.db.save_user(u.id)
    msg='🤖 STM PRO BOT\nSelect your market categories, pairs and strategies. Signals are informational only.\n'
    if bot.cfg.channel_username:msg+='Channel: '+bot.cfg.channel_username+'\n'
    if bot.cfg.referral:msg+='Referral: '+bot.cfg.referral+'\n'
    rows=[]
    if bot.cfg.channel_url:rows.append([InlineKeyboardButton('Join Channel',url=bot.cfg.channel_url)])
    rows.append([InlineKeyboardButton('Verify / Continue',callback_data='verify'),InlineKeyboardButton('Get Signal',callback_data='categories')])
    await update.effective_message.reply_text(msg,reply_markup=InlineKeyboardMarkup(rows))
async def callback(update,ctx):
    q=update.callback_query;await q.answer();bot=ctx.application.bot_data['bot'];uid=q.from_user.id;data=q.data or '';user=bot.db.user(uid)
    if data=='verify':
        # Verify channel membership only if channel configured; no fake verification.
        if bot.cfg.membership_channel:
            try:
                member=await ctx.bot.get_chat_member(bot.cfg.membership_channel,uid)
                if member.status not in ('member','administrator','creator'):await q.edit_message_text('Please join the configured channel, then retry /start.');return
            except Exception:await q.edit_message_text('Membership check failed. Ensure the bot is an admin in CHANNEL_ID.');return
        bot.db.save_user(uid,verified=True);await q.edit_message_text('Verified. Use /start to select markets and strategies.');return
    if data=='categories':
        if not user['verified']:await q.edit_message_text('First press Verify / Continue in /start.');return
        await q.edit_message_text('Choose market categories:',reply_markup=categories_kb(user['categories']));return
    if data.startswith('cat:'):
        v=data[4:];sel=user['categories']
        sel=CATEGORIES.copy() if v=='ALL' else (sel+[v] if v not in sel else [x for x in sel if x!=v])
        bot.db.save_user(uid,categories=sel);await q.edit_message_text('Choose market categories, then continue:',reply_markup=categories_kb(sel));return
    if data.startswith('pairs:') or data.startswith('pair:'):
        allpairs=await bot.provider.pairs()
        cats=user['categories']
        allpairs=[p for p in allpairs if not cats or p['category'] in cats]
        selected=user['pairs']
        page=0
        if data == 'pairs:ALL':
            selected=[p['pair'] for p in allpairs]
            bot.db.save_user(uid,pairs=selected)
        elif data.startswith('pair:'):
            val=data[5:]
            selected=selected+[val] if val not in selected else [x for x in selected if x!=val]
            bot.db.save_user(uid,pairs=selected)
        else:
            try: page=max(0,int(data.split(':',1)[1]))
            except ValueError: page=0
        await q.edit_message_text('Select pairs (8 per page):',reply_markup=pairs_kb(allpairs,selected,page));return
    if data=='strategies':await q.edit_message_text('Choose strategies:',reply_markup=strategies_kb(user['strategies']));return
    if data.startswith('strat:'):
        v=data[6:];sel=user['strategies'];sel=STRATEGIES.copy() if v=='ALL' else (sel+[v] if v not in sel else [x for x in sel if x!=v])
        bot.db.save_user(uid,strategies=sel);await q.edit_message_text('Choose strategies:',reply_markup=strategies_kb(sel));return
    if data=='save':await q.edit_message_text('Selection saved. The scanner will send matching eligible signals while running. Use /status.');return
    if data.startswith('uto:'):
        if not bot.admin(uid):await q.edit_message_text('Unauthorized.');return
        n=int(data.split(':')[1]);await bot.start(ctx.application,limit=n);await q.edit_message_text(f'Scanner started with a limit of {n} published signals. It may find fewer if filters reject candidates.');return
async def admin_guard(update,ctx):
    bot=ctx.application.bot_data['bot'];uid=update.effective_user.id if update.effective_user else 0
    if not bot.admin(uid):await update.effective_message.reply_text('Unauthorized.');return False
    return True
async def stmop(update,ctx):
    if not await admin_guard(update,ctx):return
    await ctx.application.bot_data['bot'].stop();await update.effective_message.reply_text('STMOP: new signal generation stopped. Pending results remain stored.')
async def stmon(update,ctx):
    if not await admin_guard(update,ctx):return
    await ctx.application.bot_data['bot'].start(ctx.application);await update.effective_message.reply_text('Signal scanner started.')
async def stmuto(update,ctx):
    if not await admin_guard(update,ctx):return
    await update.effective_message.reply_text('Choose the number of eligible signals:',reply_markup=uto_kb())
async def status_cmd(update,ctx):
    bot=ctx.application.bot_data['bot'];await update.effective_message.reply_text(f'Running: {bot.running}\nOpen/pending signals: {len(bot.db.open_signals())}\nStats: {bot.db.stats()}')
async def stats_cmd(update,ctx):
    await update.effective_message.reply_text(str(ctx.application.bot_data['bot'].db.stats()))
async def help_cmd(update,ctx):
    await update.effective_message.reply_text('/start /help /status /stats\nAdmin: /STMON /STMOP /STMUTO\nSignals only: this bot does not place trades.')
async def post_init(app):
    bot=app.bot_data['bot'];await bot.provider.connect();log.info('Provider connected mode=%s',bot.cfg.mode)
    if bot.cfg.mode=='mock':log.warning('Synthetic test mode is active.')
    # Resume automatically only if a prior running state was saved.
    if bot.db.setting('running',True):await bot.start(app)
async def post_shutdown(app):
    bot=app.bot_data.get('bot')
    if bot:
        # Keep the persisted running preference intact so an enabled bot resumes after restart.
        await bot.stop(persist=False);await bot.provider.close()
def main():
    cfg=Settings();store=Store();provider=QuotexProvider(cfg);bot=Bot(cfg,store,provider)
    app=ApplicationBuilder().token(cfg.token).post_init(post_init).post_shutdown(post_shutdown).build();app.bot_data['bot']=bot
    for cmd,fn in [('start',start_cmd),('help',help_cmd),('status',status_cmd),('stats',stats_cmd),('STMOP',stmop),('STMON',stmon),('STMUTO',stmuto)]:app.add_handler(CommandHandler(cmd,fn))
    app.add_handler(CallbackQueryHandler(callback))
    log.info('STM PRO BOT starting; mode=%s strategies=%s',cfg.mode,','.join(sorted(cfg.enabled)))
    # Python 3.14 no longer implicitly creates the main-thread event loop.
    # python-telegram-bot's run_polling() expects one to exist before it starts.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    app.run_polling(allowed_updates=['message','callback_query'])

if __name__=='__main__':
    main()
