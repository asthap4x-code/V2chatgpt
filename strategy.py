"""STM PRO BOT strategy pack. All strategies use closed OHLC candles only."""
from dataclasses import dataclass
import random
import pandas as pd

@dataclass
class Candidate:
    direction: str
    confidence: int
    reason: str
    strategy: str


def validate_candles(df, minimum=60):
    required={'time','open','high','low','close'}
    if df is None or len(df)<minimum or not required.issubset(df.columns):
        return False, 'not enough candles or missing OHLC columns'
    for col in ('open','high','low','close'):
        df[col]=pd.to_numeric(df[col],errors='coerce')
    if df[list(required-{'time'})].tail(minimum).isna().any().any():
        return False, 'invalid OHLC values'
    if (df.high < df[['open','close','low']].max(axis=1)).tail(minimum).any() or (df.low > df[['open','close','high']].min(axis=1)).tail(minimum).any():
        return False, 'malformed OHLC range'
    return True, 'ok'


def indicators(df):
    x=df.copy()
    x['ema10']=x.close.ewm(span=10,adjust=False).mean()
    x['ema20']=x.close.ewm(span=20,adjust=False).mean()
    x['ema50']=x.close.ewm(span=50,adjust=False).mean()
    delta=x.close.diff(); gain=delta.clip(lower=0).rolling(14).mean(); loss=(-delta.clip(upper=0)).rolling(14).mean()
    rs=gain/loss.replace(0,float('nan')); x['rsi']=100-(100/(1+rs))
    low=x.low.rolling(14).min(); high=x.high.rolling(14).max()
    x['stoch_k']=100*(x.close-low)/(high-low).replace(0,float('nan'))
    x['stoch_d']=x.stoch_k.rolling(3).mean()
    x['range']=x.high-x.low; x['body_pct']=100*(x.close-x.open).abs()/x['range'].replace(0,float('nan'))
    x['atr']=x['range'].rolling(14).mean()
    return x


def evaluate(strategy, candles, context=None):
    context=context or {}
    ok,_=validate_candles(candles)
    if not ok: return None
    x=indicators(candles); r=x.iloc[-1]; p=x.iloc[-2]
    name=strategy.upper()
    if name=='RANDOM':
        if not context.get('allow_random',False): return None
        return Candidate(random.choice(['CALL','PUT']),50,'Random experimental direction; not predictive','RANDOM')
    if name=='BLACK4X':
        if r.close>r.open: return Candidate('CALL',60,'Previous closed candle bullish','BLACK4X')
        if r.close<r.open: return Candidate('PUT',60,'Previous closed candle bearish','BLACK4X')
        return None
    if name=='OPX4O':
        body=float(r.close-r.open)
        if abs(body)<=max(float(r.close)*1e-7,1e-8): return None
        return Candidate('PUT' if body>0 else 'CALL',58,'Opposite direction to previous closed candle','OPX4O')
    if name=='STM':
        seq=x.iloc[-4:-1]
        if (seq.close>seq.open).all() and r.close<r.open and r.close>r.ema20:
            return Candidate('CALL',72,'STM PVIP: three bullish candles and bearish pullback in uptrend','STM PVIP')
        if (seq.close<seq.open).all() and r.close>r.open and r.close<r.ema20:
            return Candidate('PUT',72,'STM PVIP: three bearish candles and bullish pullback in downtrend','STM PVIP')
        if r.body_pct>=55 and r.body_pct<=85:
            if r.close>r.open and r.open>r.ema10 and r.close>p.high:
                return Candidate('CALL',70,'STM PVIP2: bullish body breakout above previous high','STM PVIP2')
            if r.close<r.open and r.open<r.ema10 and r.close<p.low:
                return Candidate('PUT',70,'STM PVIP2: bearish body breakout below previous low','STM PVIP2')
        return None
    if name=='AI':
        # Combined AI confluence: EMA/MA trend, stochastic momentum, price action,
        # simple swing structure, support/resistance and liquidity-sweep checks.
        score={'CALL':0,'PUT':0}; why=[]
        if r.ema10>r.ema20>r.ema50: score['CALL']+=2; why.append('EMA trend bullish')
        if r.ema10<r.ema20<r.ema50: score['PUT']+=2; why.append('EMA trend bearish')
        if pd.notna(r.stoch_k) and pd.notna(r.stoch_d):
            if 20<r.stoch_k<80 and r.stoch_k>r.stoch_d: score['CALL']+=1; why.append('stochastic momentum bullish')
            if 20<r.stoch_k<80 and r.stoch_k<r.stoch_d: score['PUT']+=1; why.append('stochastic momentum bearish')
        if r.close>p.high: score['CALL']+=1; why.append('break of previous high')
        if r.close<p.low: score['PUT']+=1; why.append('break of previous low')
        if r.close>r.ema20: score['CALL']+=1
        if r.close<r.ema20: score['PUT']+=1
        if r.ema10>r.ema20 and r.close>r.open: score['CALL']+=1
        if r.ema10<r.ema20 and r.close<r.open: score['PUT']+=1
        swing=x.iloc[-11:-1]
        if len(swing)>=8:
            resistance=float(swing.high.max()); support=float(swing.low.min())
            # Liquidity sweep/rejection: take a recent swing level, then close back inside.
            if r.low<support and r.close>support: score['CALL']+=2; why.append('support liquidity sweep/reclaim')
            if r.high>resistance and r.close<resistance: score['PUT']+=2; why.append('resistance liquidity sweep/rejection')
            if r.close>resistance: score['CALL']+=1; why.append('swing resistance breakout')
            if r.close<support: score['PUT']+=1; why.append('swing support breakdown')
            width=max(float(r.atr) if pd.notna(r.atr) else 0.0, abs(float(r.close))*1e-7)
            if abs(float(r.close)-support)<=width*0.25 and r.close>r.open: score['CALL']+=1; why.append('support price-action rejection')
            if abs(float(r.close)-resistance)<=width*0.25 and r.close<r.open: score['PUT']+=1; why.append('resistance price-action rejection')
        direction=max(score,key=score.get); best=score[direction]
        if best<3 or score['CALL']==score['PUT']: return None
        return Candidate(direction,min(90,60+best*4),'; '.join(why) or 'EMA/MA and momentum confluence','AI')
    return None
