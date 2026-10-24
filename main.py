"""STM PRO BOT — signal-only Telegram bot. It never places trades.
Uses provider-reported Quotex data in pyquotex mode; mock mode is for UI/testing only.
"""
import asyncio, hashlib, json, logging, os, sqlite3, time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pandas as pd
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes
from strategy import evaluate, validate_candles

load_dotenv()
logging.basicConfig(level=os.getenv('LOG_LEVEL','INFO').upper(), format='%(asctime)s %(levelname)s %(name)s: %(message)s')
log = logging.getLogger('stmpro')
CATEGORIES = ['FOREX', 'CURRENCY', 'CRYPTO', 'COMMODITIES']
STRATEGIES = ['AI', 'STM', 'RANDOM', 'BLACK4X', 'OPX4O']
BATCH_SIZES = (10, 20, 25, 30, 40)

class Settings:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN','').strip()
        self.email = os.getenv('QUOTEX_EMAIL','').strip()
        self.password = os.getenv('QUOTEX_PASSWORD','').strip()
        self.mode = os.getenv('PROVIDER_MODE','pyquotex').strip().lower()
        self.admin_ids = {int(x.strip()) for x in os.getenv('ADMIN_IDS','').split(',') if x.strip().isdigit()}
        self.channel = os.getenv('SIGNAL_CHANNEL_ID', os.getenv('CHANNEL_ID','')).strip()
        self.membership_channel = os.getenv('CHANNEL_ID','').strip()
        self.channel_username = os.getenv('CHANNEL_USERNAME','').strip()
        self.channel_url = os.getenv('CHANNEL_URL','').strip()
        self.admin_username = os.getenv('ADMIN_USERNAME','admin').strip().lstrip('@')
        self.feedback = os.getenv('FEEDBACK_CONTACT', self.admin_username).strip().lstrip('@')
        self.referral = os.getenv('QUOTEX_REFERRAL_URL','').strip()
        self.payout = float(os.getenv('PAYOUT_THRESHOLD','80'))
        self.tz = os.getenv('TIMEZONE','Indian/Maldives').strip()
        self.interval = max(1.0, float(os.getenv('SCAN_INTERVAL_SECONDS','1')))
        self.concurrency = max(1, int(os.getenv('MAX_CONCURRENCY','4')))
        self.confidence = max(0, min(100, int(os.getenv('MIN_CONFIDENCE','55'))))
        self.enabled = {x.strip().upper() for x in os.getenv('ENABLED_STRATEGIES','AI,STM,BLACK4X,OPX4O').split(',') if x.strip()}
        if not self.token: raise ValueError('TELEGRAM_BOT_TOKEN is missing in .env')
        if self.mode not in {'pyquotex','mock'}: raise ValueError('PROVIDER_MODE must be pyquotex or mock')
        if not 0 <= self.payout <= 100: raise ValueError('PAYOUT_THRESHOLD must be between 0 and 100')
        ZoneInfo(self.tz)
        if self.mode == 'pyquotex' and (not self.email or not self.password):
            raise ValueError('QUOTEX_EMAIL and QUOTEX_PASSWORD are required for pyquotex mode')

class Store:
    def __init__(self, path='stm_pro.sqlite3'):
        self.path = path
        with self.db() as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS users(uid INTEGER PRIMARY KEY, verified INTEGER DEFAULT 0, categories TEXT DEFAULT '[]', pairs TEXT DEFAULT '[]', strategies TEXT DEFAULT '[]');
            CREATE TABLE IF NOT EXISTS signals(id INTEGER PRIMARY KEY, pair TEXT, category TEXT, strategy TEXT, direction TEXT, signal_at TEXT, entry_at TEXT, expiry_at TEXT, payout REAL, confidence INTEGER, reason TEXT, reference_price REAL, status TEXT DEFAULT 'PENDING', open_price REAL, close_price REAL, mtg_open REAL, mtg_close REAL, result TEXT, dedupe TEXT UNIQUE, channel_posted INTEGER DEFAULT 0, result_posted INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY,v TEXT);
            ''')
    def db(self):
        c=sqlite3.connect(self.path, timeout=20); c.row_factory=sqlite3.Row; c.execute('PRAGMA journal_mode=WAL'); return c
    def user(self, uid):
        with self.db() as c: r=c.execute('SELECT * FROM users WHERE uid=?',(uid,)).fetchone()
        if not r: return {'uid':uid,'verified':0,'categories':[],'pairs':[],'strategies':[]}
        d=dict(r)
        for k in ('categories','pairs','strategies'): d[k]=json.loads(d[k] or '[]')
        return d
    def save_user(self, uid, **kw):
        with self.db() as c:
            c.execute('INSERT OR IGNORE INTO users(uid) VALUES(?)',(uid,))
            for k,v in kw.items():
                if k=='verified': c.execute('UPDATE users SET verified=? WHERE uid=?',(int(bool(v)),uid))
                elif k in {'categories','pairs','strategies'}: c.execute(f'UPDATE users SET {k}=? WHERE uid=?',(json.dumps(v),uid))
    def verified_users(self):
        with self.db() as c: return [dict(r) for r in c.execute('SELECT * FROM users WHERE verified=1')]
    def setting(self,k,default=None):
        with self.db() as c: r=c.execute('SELECT v FROM settings WHERE k=?',(k,)).fetchone()
        return json.loads(r['v']) if r else default
    def set_setting(self,k,v):
        with self.db() as c: c.execute('INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',(k,json.dumps(v)))
    def add_signal(self,s):
        with self.db() as c:
            cur=c.execute('''INSERT OR IGNORE INTO signals(pair,category,strategy,direction,signal_at,entry_at,expiry_at,payout,confidence,reason,reference_price,dedupe) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',tuple(s[k] for k in ('pair','category','strategy','direction','signal_at','entry_at','expiry_at','payout','confidence','reason','reference_price','dedupe')))
            return cur.lastrowid if cur.rowcount else None
    def open_signals(self):
        with self.db() as c: return [dict(r) for r in c.execute('SELECT * FROM signals WHERE result IS NULL ORDER BY id')]
    def resolve(self,sid,**kw):
        with self.db() as c: c.execute('UPDATE signals SET '+','.join(f'{k}=?' for k in kw)+' WHERE id=?',(*kw.values(),sid))
    def stats(self, since=None):
        query="SELECT result,COUNT(*) n FROM signals WHERE result IS NOT NULL"
        args=[]
        if since: query += ' AND entry_at>=?'; args.append(since)
        query += ' GROUP BY result'
        with self.db() as c: rows=c.execute(query,args).fetchall()
        d={r['result']:r['n'] for r in rows}; wins=d.get('WIN',0)+d.get('MTG WIN',0); losses=d.get('LOSS',0); total=wins+losses
        return {'wins':wins,'losses':losses,'accuracy':round(100*wins/total,2) if total else 0,'resolved':total,'mtg_wins':d.get('MTG WIN',0)}
    def signals_since(self, since):
        with self.db() as c: return [dict(r) for r in c.execute('SELECT * FROM signals WHERE entry_at>=? AND result IS NOT NULL ORDER BY entry_at',(since,))]

class QuotexProvider:
    def __init__(self,cfg): self.cfg=cfg; self.client=None; self.lock=asyncio.Semaphore(cfg.concurrency)
    async def connect(self):
        if self.cfg.mode=='mock': log.warning('MOCK MODE: synthetic candles and 85%% test payout; never use mock results as live performance.'); return
        try:
            from pyquotex.stable_api import Quotex
            self.client=Quotex(email=self.cfg.email,password=self.cfg.password,lang='en')
            result=await self.client.connect()
            if isinstance(result,tuple) and result and result[0] is False: raise RuntimeError('Quotex login returned failure')
        except Exception as e: self.client=None; raise RuntimeError(f'PyQuotex connection failed/incompatible: {type(e).__name__}: {e}') from e
    async def close(self):
        if self.client:
            try: await self.client.close()
            except Exception: log.exception('Error closing Quotex client')
    @staticmethod
    def categorize(name):
        n=name.upper().replace('/','').replace(' ','').replace('(','').replace(')','').replace('-','')
        crypto=('BTC','ETH','LTC','XRP','DOGE','ADA','SOL','DOT','BCH','BNB','TRX','LINK','XLM','AVAX','ETC')
        commodities=('GOLD','SILVER','XAU','XAG','UKBRENT','USCRUDE','BRENT','WTI','OIL')
        if any(x in n for x in crypto): return 'CRYPTO'
        if any(x in n for x in commodities): return 'COMMODITIES'
        if 'OTC' in n: return 'CURRENCY'
        return 'FOREX'
    async def pairs(self):
        if self.cfg.mode=='mock':
            names=['EURUSD','GBPUSD','USDJPY','AUDUSD','EURUSD-OTC','GBPUSD-OTC','USDIDR-OTC','BTCUSD','ETHUSD','Gold-OTC','UKBrent-OTC','Silver-OTC','USCrude-OTC']
            return [{'pair':p,'category':self.categorize(p)} for p in names]
        if not self.client: raise RuntimeError('Quotex is not connected')
        candidates=[]
        for obj,name in [(self.client,'get_all_asset_name'),(self.client,'get_available_asset'),(self.client,'get_payment')]:
            fn=getattr(obj,name,None)
            if callable(fn):
                try:
                    val=fn(); val=await val if hasattr(val,'__await__') else val
                    if val: candidates.append(val)
                except Exception: log.debug('Asset discovery %s unavailable',name,exc_info=True)
        api=getattr(self.client,'api',None)
        for obj,name in [(api,'instruments'),(api,'assets')]:
            val=getattr(obj,name,None) if obj else None
            if val: candidates.append(val)
        found={}
        def consume(val):
            if isinstance(val,dict):
                for k,v in val.items():
                    if isinstance(v,dict):
                        name=v.get('name') or v.get('asset') or v.get('symbol') or (k if isinstance(k,str) else None)
                        if name: found[str(name)]=1
                    elif isinstance(k,str) and any(ch.isalpha() for ch in k): found[k]=1
                    elif isinstance(v,str) and any(ch.isalpha() for ch in v): found[v]=1
                    elif isinstance(v,(list,tuple,set)): consume(v)
            elif isinstance(val,(list,tuple,set)):
                for v in val:
                    if isinstance(v,str): found[v]=1
                    elif isinstance(v,dict):
                        name=v.get('name') or v.get('asset') or v.get('symbol')
                        if name: found[str(name)]=1
                    elif isinstance(v,(list,tuple,set)): consume(v)
        for val in candidates: consume(val)
        return [{'pair':p,'category':self.categorize(p)} for p in sorted(found) if p and len(p)<40]
    async def candles(self,pair,limit=150):
        if self.cfg.mode=='mock':
            import numpy as np
            now=int(time.time())//60*60; times=np.arange(now-(limit-1)*60,now+1,60); base=150 if 'JPY' in pair else 1.1
            close=base+np.cumsum(np.sin(np.arange(len(times))/4)*.0002+np.cos(np.arange(len(times))/9)*.0001); op=np.r_[close[0],close[:-1]]
            return pd.DataFrame({'time':pd.to_datetime(times,unit='s',utc=True),'open':op,'high':np.maximum(op,close)+.00015,'low':np.minimum(op,close)-.00015,'close':close,'volume':1})
        if not self.client: raise RuntimeError('Quotex is not connected')
        async with self.lock: raw=await self.client.get_candles(pair,int(time.time()),limit*60,60)
        if not raw: raise RuntimeError(f'No candles for {pair}')
        df=pd.DataFrame(raw)
        if not {'time','open','high','low','close'}.issubset(df.columns): raise RuntimeError(f'Unknown candle schema for {pair}: {list(df.columns)}')
        df['time']=pd.to_datetime(df.time,unit='s',utc=True,errors='coerce')
        for col in ('open','high','low','close'): df[col]=pd.to_numeric(df[col],errors='coerce')
        return df.dropna(subset=['time','open','high','low','close']).sort_values('time').drop_duplicates('time').tail(limit).reset_index(drop=True)
    async def payout(self,pair):
        if self.cfg.mode=='mock': return 85.0
        for obj,name in [(self.client,'get_payment'),(self.client,'get_payout'),(getattr(self.client,'api',None),'get_payment')]:
            fn=getattr(obj,name,None) if obj else None
            if not callable(fn): continue
            try:
                val=fn(); val=await val if hasattr(val,'__await__') else val
                if isinstance(val,dict):
                    for k,v in val.items():
                        match=str(k).replace('_','').replace('-','').replace('/','').upper()==pair.replace('_','').replace('-','').replace('/','').upper()
                        if match or (isinstance(v,dict) and str(v.get('name','')).upper()==pair.upper()):
                            x=v.get('payout',v.get('payment',v.get('percent'))) if isinstance(v,dict) else v
                            if isinstance(x,(int,float)) and 0<=x<=100: return float(x*100 if 0<x<=1 else x)
                if isinstance(val,(int,float)) and 0<=val<=100: return float(val*100 if 0<val<=1 else val)
            except Exception: log.debug('Payout endpoint %s unavailable',name,exc_info=True)
        return None

class Bot:
    def __init__(self,cfg,store,provider):
        self.cfg=cfg; self.db=store; self.provider=provider; self.running=False; self.task=None; self.scan_lock=asyncio.Lock(); self.limit=None; self.sent_count=0; self.candidate_cache_minute=None; self.candidate_cache=[]
    def admin(self,uid): return uid in self.cfg.admin_ids
    def local(self,iso): return datetime.fromisoformat(iso).astimezone(ZoneInfo(self.cfg.tz)).strftime('%Y-%m-%d %H:%M:%S %Z')
    def day_start(self): return datetime.now(ZoneInfo(self.cfg.tz)).replace(hour=0,minute=0,second=0,microsecond=0).astimezone(timezone.utc).isoformat()
    def signal_text(self,s):
        direction='🔴 𝗣𝗨𝗧 ▼' if s['direction']=='PUT' else '🟢 𝗖𝗔𝗟𝗟 ▲'
        bar='█'*max(1,min(10,round(s['confidence']/10)))+'░'*(10-max(1,min(10,round(s['confidence']/10))))
        return (f'🤖 STM PRO BOT\n━━━━━━━━━━━━━━━━━━━━━\n💱 Pair: {s["pair"]}\n⏱️ Trade at: {self.local(s["entry_at"])}\n🔮 Direction: {direction}\n💰 Payout: {s["payout"]:.0f}%\n━━━━━━━━━━━━━━━━━━━━━\n🎯 𝗦𝗜𝗚𝗡𝗔𝗟 𝗖𝗢𝗡𝗙𝗜𝗗𝗘𝗡𝗖𝗘\n{bar} {s["confidence"]}%\n━━━━━━━━━━━━━━━━━━━━━\n⚠️ 1-MIN CANDLE. IF FIRST TRADE LOSES, YOU MAY USE ONLY ONE MTG.\n🧠 Strategy: {s["strategy"]}\nSignal only — no guaranteed results.')
    def result_text(self,s):
        st=self.db.stats(); op=s.get('open_price'); cp=s.get('close_price'); result=s.get('result','PENDING')
        mtg=''
        if s.get('mtg_open') is not None or s.get('mtg_close') is not None: mtg=f'\n🔁 MTG Open/Close: {s.get("mtg_open")}/{s.get("mtg_close")}'
        return (f'📊 STM PRO BOT 𝗥𝗘𝗦𝗨𝗟𝗧\n━━━━━━━━━━━━━━━━━━━━━\n💱 Pair: {s["pair"]}\n⏱️ Time: {self.local(s["entry_at"])}\n🔮 Direction: {s["direction"]}\n📈 Opening: {op if op is not None else "UNAVAILABLE"}\n📉 Closing: {cp if cp is not None else "UNAVAILABLE"}\n🌟 Result: {"🌟🌟 MTG WIN" if result=="MTG WIN" else "🌟 Non-MTG WIN" if result=="WIN" else "❌ LOSS" if result=="LOSS" else result}{mtg}\n━━━━━━━━━━━━━━━━━━━━━\n🔥 WIN: {st["wins"]} | 😣 LOSS: {st["losses"]}\n💎 This pair: {s["pair"]}\n📊 STM PRO BOT ACCURACY: {st["accuracy"]}%\n📩 Please send feedback: @{self.cfg.feedback}')
    def selected_users_for(self,s):
        matched=[]
        for u in self.db.verified_users():
            cats=json.loads(u['categories'] or '[]'); pairs=json.loads(u['pairs'] or '[]'); sts=json.loads(u['strategies'] or '[]')
            if not cats or not pairs or not sts: continue
            if s['category'] in cats and s['pair'] in pairs and s['strategy'] in sts: matched.append(int(u['uid']))
        return matched
    async def publish(self,app,s):
        # Send only to users whose saved category, pair and strategy all match.
        recipients=self.selected_users_for(s)
        if self.cfg.channel:
            try: await app.bot.send_message(self.cfg.channel,self.signal_text(s))
            except Exception: log.exception('Could not post signal to channel')
        for uid in recipients:
            try: await app.bot.send_message(uid,self.signal_text(s))
            except Exception: log.debug('Signal DM unavailable for user %s',uid)
    async def scan(self,app):
        async with self.scan_lock:
            now=datetime.now(timezone.utc)
            # Analyse before the final five seconds so provider latency does not make us miss entry.
            if now.second < 45: return
            minute_key=now.strftime('%Y-%m-%dT%H:%M')
            if self.db.setting('last_signal_minute')==minute_key: return
            if now.second < 55 and self.candidate_cache_minute == minute_key:
                return
            users=self.db.verified_users(); subscriptions=[]
            for u in users:
                cats=json.loads(u['categories'] or '[]'); pairs=json.loads(u['pairs'] or '[]'); sts=json.loads(u['strategies'] or '[]')
                if cats and pairs and sts:
                    subscriptions.append((set(cats),set(pairs),set(sts)))
            if not subscriptions: return
            provider_pairs=await self.provider.pairs(); candidates=[]
            for p in provider_pairs:
                if not any(p['category'] in cats and p['pair'] in pairs for cats,pairs,_ in subscriptions): continue
                relevant=set().union(*(sts for cats,pairs,sts in subscriptions if p['category'] in cats and p['pair'] in pairs))
                for st in relevant:
                    if st not in STRATEGIES or (st not in self.cfg.enabled and st!='RANDOM'): continue
                    try:
                        df=await self.provider.candles(p['pair'],150)
                        if df is None: continue
                        df=df.copy(); df['time']=pd.to_datetime(df.time,utc=True,errors='coerce')
                        # Never use a candle that has not fully closed.
                        now_ts=pd.Timestamp.now(tz='UTC'); df=df.loc[df.time+pd.Timedelta(seconds=60)<=now_ts].reset_index(drop=True)
                        ok,_=validate_candles(df,60)
                        if not ok: continue
                        age=(now_ts-(df.time.iloc[-1]+pd.Timedelta(seconds=60))).total_seconds()
                        if age>90: continue
                        payout=await self.provider.payout(p['pair'])
                        if payout is None or payout<self.cfg.payout: continue
                        cand=evaluate(st,df,{'allow_random':self.cfg.mode=='mock'})
                        if not cand or cand.confidence<self.cfg.confidence: continue
                        # The user must have selected this exact strategy too.
                        if not any(p['category'] in cats and p['pair'] in pairs and st in sts for cats,pairs,sts in subscriptions): continue
                        candidates.append((cand.confidence,p,cand,float(payout),float(df.close.iloc[-1])))
                    except Exception: log.exception('Candidate evaluation failed for %s/%s',p['pair'],st)
            # Cache eligible candidates found before :55, then publish only from :55 to :59.
            now=datetime.now(timezone.utc)
            minute_key=now.strftime('%Y-%m-%dT%H:%M')
            if now.second < 55:
                self.candidate_cache_minute=minute_key; self.candidate_cache=candidates
                return
            if now.second > 59 or self.db.setting('last_signal_minute')==minute_key: return
            if self.candidate_cache_minute == minute_key and self.candidate_cache:
                candidates=self.candidate_cache
            if not candidates: return
            candidates.sort(key=lambda x:(x[0],x[3]),reverse=True)
            confidence,p,cand,payout,reference=candidates[0]
            entry=(now+timedelta(minutes=1)).replace(second=0,microsecond=0); expiry=entry+timedelta(minutes=1)
            key=hashlib.sha256(f'{p["pair"]}|{entry.isoformat()}'.encode()).hexdigest()
            row={'pair':p['pair'],'category':p['category'],'strategy':cand.strategy,'direction':cand.direction,'signal_at':now.isoformat(),'entry_at':entry.isoformat(),'expiry_at':expiry.isoformat(),'payout':payout,'confidence':int(cand.confidence),'reason':cand.reason,'reference_price':reference,'dedupe':key}
            sid=self.db.add_signal(row)
            if not sid: return
            row['id']=sid
            # Persist the minute lock before sending to avoid multiple signals if Telegram send fails.
            self.db.set_setting('last_signal_minute',minute_key)
            await self.publish(app,row); self.sent_count+=1
            log.info('Published one signal id=%s pair=%s strategy=%s',sid,row['pair'],row['strategy'])
    async def resolve_results(self,app):
        for s in self.db.open_signals():
            try:
                entry=pd.Timestamp(s['entry_at']); expiry=pd.Timestamp(s['expiry_at']); now=pd.Timestamp.now(tz='UTC')
                if now < expiry: continue
                df=await self.provider.candles(s['pair'],180)
                if df is None or df.empty: continue
                df=df.copy(); df['time']=pd.to_datetime(df.time,utc=True,errors='coerce')
                first=df.loc[df.time==entry]
                if first.empty: continue
                a=first.iloc[0]; op=float(a.open); cp=float(a.close)
                if cp==op: result='TIE'
                else: result='WIN' if ((s['direction']=='CALL' and cp>op) or (s['direction']=='PUT' and cp<op)) else 'LOSS'
                updates={'open_price':op,'close_price':cp,'status':result,'result':result}
                if result=='LOSS':
                    mtg_time=expiry
                    if now < mtg_time+pd.Timedelta(seconds=60): continue
                    second=df.loc[df.time==mtg_time]
                    if second.empty: continue
                    b=second.iloc[0]; mop=float(b.open); mcp=float(b.close)
                    mw='TIE' if mcp==mop else ('WIN' if ((s['direction']=='CALL' and mcp>mop) or (s['direction']=='PUT' and mcp<mop)) else 'LOSS')
                    result='MTG WIN' if mw=='WIN' else 'LOSS'
                    updates.update(mtg_open=mop,mtg_close=mcp,status=result,result=result)
                self.db.resolve(s['id'],**updates)
                with self.db.db() as c: saved=dict(c.execute('SELECT * FROM signals WHERE id=?',(s['id'],)).fetchone())
                msg=self.result_text(saved)
                if self.cfg.channel:
                    try: await app.bot.send_message(self.cfg.channel,msg)
                    except Exception: log.exception('Could not publish result to channel')
                # Result only to verified users who had selected this signal's filters.
                for uid in self.selected_users_for(saved):
                    try: await app.bot.send_message(uid,msg)
                    except Exception: pass
                log.info('Resolved id=%s result=%s',s['id'],result)
            except Exception: log.exception('Could not resolve result id=%s',s.get('id'))
    def session_report(self):
        rows=self.db.signals_since(self.day_start()); otc=[r for r in rows if 'OTC' in r['pair'].upper()]; real=[r for r in rows if 'OTC' not in r['pair'].upper()]
        def section(title,items):
            lines=[f'💶 {title}']
            if not items: lines.append('No resolved signals.')
            for r in items:
                icon='🌟🌟' if r['result']=='MTG WIN' else ('✅' if r['result']=='WIN' else ('❌' if r['result']=='LOSS' else '➖'))
                lines.append(f'❒ {self.local(r["entry_at"])[11:16]} - {r["pair"]} - {r["direction"]} {icon}')
            return '\n'.join(lines)
        st=self.db.stats(self.day_start()); total=st['wins']+st['losses']
        return (f'========== STM PRO BOT ==========\n━━━━━━━━━・━━━━━━━━━\n📆 {datetime.now(ZoneInfo(self.cfg.tz)).strftime("%Y-%m-%d")}\n━━━━━━━━━・━━━━━━━━━\n{section("OTC MARKETS FINAL",otc)}\n━━━━━━━━━・━━━━━━━━━\n{section("REAL MARKETS FINAL",real)}\n━━━━━━━━━・━━━━━━━━━\n📊 TOTAL: {st["wins"]} WIN / {st["losses"]} LOSS ({st["accuracy"]}% accuracy; {total} resolved)\n━━━━━━━━━・━━━━━━━━━\n⚙️ TELEGRAM: @{self.cfg.admin_username}\n📳 Session report generated.\nFEEDBACK OR INQUIRIES: @{self.cfg.feedback}')
    async def stop(self,persist=True):
        self.running=False
        if persist: self.db.set_setting('running',False)
        if self.task and not self.task.done(): self.task.cancel()
        self.task=None
    async def start(self,app,limit=None):
        if self.running and self.task and not self.task.done():
            if limit is not None: self.limit=limit; self.sent_count=0
            return
        self.limit=limit; self.sent_count=0; self.running=True; self.db.set_setting('running',True)
        self.task=asyncio.create_task(self.loop(app),name='stmpro-scan-loop')
    async def loop(self,app):
        while self.running:
            try: await self.resolve_results(app)
            except Exception: log.exception('Result cycle failed')
            try:
                if self.running: await self.scan(app)
            except Exception: log.exception('Scan cycle failed')
            if self.limit is not None and self.sent_count>=self.limit:
                self.limit=None; self.running=False; self.db.set_setting('running',False)
                await self.send_session_report(app)
                break
            await asyncio.sleep(self.cfg.interval)
    async def send_session_report(self,app):
        msg=self.session_report()
        if self.cfg.channel:
            try: await app.bot.send_message(self.cfg.channel,msg)
            except Exception: log.exception('Could not send session report')
        for u in self.db.verified_users():
            try: await app.bot.send_message(int(u['uid']),msg)
            except Exception: pass

# Inline keyboards

def categories_kb(selected):
    rows=[]
    for i in range(0,4,2): rows.append([InlineKeyboardButton(('✅ ' if c in selected else '')+c,callback_data='cat:'+c) for c in CATEGORIES[i:i+2]])
    rows.append([InlineKeyboardButton('SELECT ALL',callback_data='cat:ALL')])
    rows.append([InlineKeyboardButton('NEXT → SELECT PAIRS',callback_data='pairs:0')])
    return InlineKeyboardMarkup(rows)
def pairs_kb(pairs,selected,page=0):
    chunk=pairs[page*8:(page+1)*8]; rows=[[InlineKeyboardButton(('✅ ' if p['pair'] in selected else '')+p['pair'],callback_data='pair:'+p['pair'])] for p in chunk]
    nav=[]
    if page>0: nav.append(InlineKeyboardButton('◀ Prev',callback_data=f'pairs:{page-1}'))
    if (page+1)*8<len(pairs): nav.append(InlineKeyboardButton('Next ▶',callback_data=f'pairs:{page+1}'))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton('SELECT ALL PAIRS',callback_data='pairs:ALL')])
    rows.append([InlineKeyboardButton('NEXT → SELECT STRATEGY',callback_data='strategies')])
    return InlineKeyboardMarkup(rows)
def strategies_kb(selected):
    rows=[[InlineKeyboardButton(('✅ ' if s in selected else '')+{'AI':'AI STRATEGY','STM':'STM STRATEGY','RANDOM':'RANDOM STRATEGY','BLACK4X':'BLACK4X','OPX4O':'OPX4O'}[s],callback_data='strat:'+s)] for s in STRATEGIES]
    rows.append([InlineKeyboardButton('SELECT ALL',callback_data='strat:ALL')])
    rows.append([InlineKeyboardButton('🚀 START ANALYSIS',callback_data='save')])
    return InlineKeyboardMarkup(rows)
def uto_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton(str(n),callback_data='uto:'+str(n)) for n in BATCH_SIZES]])
def get_signal_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton('🎯 GET SIGNAL',callback_data='categories')]])

async def start_cmd(update:Update,ctx:ContextTypes.DEFAULT_TYPE):
    bot=ctx.application.bot_data['bot']; u=update.effective_user; bot.db.save_user(u.id)
    user=bot.db.user(u.id)
    msg='🤖 STM PRO BOT\nChoose your market, pairs and strategy to receive eligible signals.\n\n⚠️ Signal-only service. Trading involves risk; results are not guaranteed.'
    if bot.cfg.channel_username: msg+='\nChannel: '+bot.cfg.channel_username
    if bot.cfg.referral: msg+='\nReferral: '+bot.cfg.referral
    if user['verified']:
        await update.effective_message.reply_text(msg+'\n\n✅ Membership verified. Press GET SIGNAL to configure your signals.',reply_markup=get_signal_kb()); return
    rows=[]
    if bot.cfg.channel_url: rows.append([InlineKeyboardButton('📢 Join Channel',url=bot.cfg.channel_url)])
    rows.append([InlineKeyboardButton('✅ Verify Membership',callback_data='verify')])
    await update.effective_message.reply_text(msg+'\n\nJoin the channel, then verify membership to unlock GET SIGNAL.',reply_markup=InlineKeyboardMarkup(rows))

async def callback(update:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer(); bot=ctx.application.bot_data['bot']; uid=q.from_user.id; data=q.data or ''; user=bot.db.user(uid)
    if data=='verify':
        if not bot.cfg.membership_channel:
            await q.edit_message_text('Membership verification is not configured. Ask the admin to set CHANNEL_ID in .env.'); return
        try:
            member=await ctx.bot.get_chat_member(bot.cfg.membership_channel,uid)
            if member.status not in ('member','administrator','creator'):
                await q.edit_message_text('Please join the channel, then press /start and verify again.'); return
        except Exception:
            await q.edit_message_text('Membership check failed. Make sure the bot is an admin in the configured CHANNEL_ID.'); return
        bot.db.save_user(uid,verified=True)
        await q.edit_message_text('✅ Membership verified. Press /start to open GET SIGNAL.'); return
    if data=='categories':
        if not user['verified']: await q.edit_message_text('First join and verify membership with /start.'); return
        await q.edit_message_text('Step 1/3 — Choose market categories:',reply_markup=categories_kb(user['categories'])); return
    if data.startswith('cat:'):
        v=data[4:]; sel=user['categories']
        sel=CATEGORIES.copy() if v=='ALL' else (sel+[v] if v not in sel else [x for x in sel if x!=v])
        bot.db.save_user(uid,categories=sel)
        await q.edit_message_text('Step 1/3 — Choose market categories, then continue:',reply_markup=categories_kb(sel)); return
    if data.startswith('pairs:') or data.startswith('pair:'):
        if not user['verified']: await q.edit_message_text('Please verify membership first.'); return
        allpairs=await bot.provider.pairs(); cats=user['categories']
        allpairs=[p for p in allpairs if not cats or p['category'] in cats]
        selected=user['pairs']; page=0
        if data=='pairs:ALL':
            selected=[p['pair'] for p in allpairs]; bot.db.save_user(uid,pairs=selected)
        elif data.startswith('pair:'):
            val=data[5:]; selected=selected+[val] if val not in selected else [x for x in selected if x!=val]
            bot.db.save_user(uid,pairs=selected)
        else:
            try: page=max(0,int(data.split(':',1)[1]))
            except ValueError: page=0
        await q.edit_message_text('Step 2/3 — Select pairs (8 per page):',reply_markup=pairs_kb(allpairs,selected,page)); return
    if data=='strategies':
        if not user['verified']: await q.edit_message_text('Please verify membership first.'); return
        await q.edit_message_text('Step 3/3 — Choose strategies:',reply_markup=strategies_kb(user['strategies'])); return
    if data.startswith('strat:'):
        v=data[6:]; sel=user['strategies']
        sel=STRATEGIES.copy() if v=='ALL' else (sel+[v] if v not in sel else [x for x in sel if x!=v])
        bot.db.save_user(uid,strategies=sel)
        await q.edit_message_text('Step 3/3 — Choose strategies, then start analysis:',reply_markup=strategies_kb(sel)); return
    if data=='save':
        user=bot.db.user(uid)
        if not user['categories'] or not user['pairs'] or not user['strategies']:
            await q.edit_message_text('Please select at least one category, one pair and one strategy before starting.',reply_markup=strategies_kb(user['strategies'])); return
        await bot.start(ctx.application)
        await q.edit_message_text('✅ Selection saved. Analysis started. You will receive at most ONE new signal per minute when a selected pair/strategy passes all filters (including payout ≥ 80%).')
        return
    if data.startswith('uto:'):
        if not bot.admin(uid): await q.edit_message_text('Unauthorized.'); return
        n=int(data.split(':',1)[1])
        if n not in BATCH_SIZES: await q.edit_message_text('Invalid signal count.'); return
        await bot.start(ctx.application,limit=n)
        await q.edit_message_text(f'✅ Auto signal mode started. It will stop after {n} published signals. A signal may take longer if no pair passes the filters.'); return

async def admin_guard(update,ctx):
    bot=ctx.application.bot_data['bot']; uid=update.effective_user.id if update.effective_user else 0
    if not bot.admin(uid): await update.effective_message.reply_text('Unauthorized.'); return False
    return True
async def stmop(update,ctx):
    if not await admin_guard(update,ctx): return
    bot=ctx.application.bot_data['bot']; await bot.stop(); bot.db.set_setting('running',False)
    await update.effective_message.reply_text('🛑 STMOP: new signal generation stopped. Pending result checks remain stored.')
    await bot.send_session_report(ctx.application)
async def stmon(update,ctx):
    if not await admin_guard(update,ctx): return
    await ctx.application.bot_data['bot'].start(ctx.application); await update.effective_message.reply_text('Signal scanner started.')
async def stmuto(update,ctx):
    if not await admin_guard(update,ctx): return
    await update.effective_message.reply_text('Choose how many signals to publish:',reply_markup=uto_kb())
async def status_cmd(update,ctx):
    bot=ctx.application.bot_data['bot']
    if bot.admin(update.effective_user.id):
        await update.effective_message.reply_text(f'Running: {bot.running}\nAuto limit: {bot.limit}\nSignals this run: {bot.sent_count}\nPending results: {len(bot.db.open_signals())}\nStats: {bot.db.stats()}')
    else: await update.effective_message.reply_text('Signal service status is managed by the admin. Use /start to configure your preferences.')
async def stats_cmd(update,ctx):
    bot=ctx.application.bot_data['bot']; await update.effective_message.reply_text(str(bot.db.stats()))
async def help_cmd(update,ctx):
    await update.effective_message.reply_text('/start — verify membership and select signals\n/status — service status\n/stats — resolved statistics\nAdmin only: /STMOP, /STMON, /STMUTO\nThis bot provides signals only and does not place trades.')
async def post_init(app):
    bot=app.bot_data['bot']; await bot.provider.connect(); log.info('Provider connected mode=%s',bot.cfg.mode)
    # Safer default: never start a new signal session automatically on first install.
    if bot.db.setting('running',False): await bot.start(app,limit=bot.db.setting('auto_limit',None))
async def post_shutdown(app):
    bot=app.bot_data.get('bot')
    if bot:
        await bot.stop(persist=False); await bot.provider.close()
def main():
    cfg=Settings(); store=Store(); provider=QuotexProvider(cfg); bot=Bot(cfg,store,provider)
    app=ApplicationBuilder().token(cfg.token).post_init(post_init).post_shutdown(post_shutdown).build(); app.bot_data['bot']=bot
    for cmd,fn in [('start',start_cmd),('help',help_cmd),('status',status_cmd),('stats',stats_cmd),('STMOP',stmop),('STMON',stmon),('STMUTO',stmuto)]: app.add_handler(CommandHandler(cmd,fn))
    app.add_handler(CallbackQueryHandler(callback))
    log.info('STM PRO BOT starting; mode=%s strategies=%s',cfg.mode,','.join(sorted(cfg.enabled)))
    try: asyncio.get_event_loop()
    except RuntimeError: asyncio.set_event_loop(asyncio.new_event_loop())
    app.run_polling(allowed_updates=['message','callback_query'])
if __name__=='__main__': main()
