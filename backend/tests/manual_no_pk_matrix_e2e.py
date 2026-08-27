"""196-only mixed PK/no-PK CDC matrix: 13 tables, 1,560 baseline rows."""
from __future__ import annotations

import json, os, time, urllib.request
from decimal import Decimal
from sqlalchemy import text
from app.database import make_engine
from app.models import ConnectionConfig
from app.store import build_store

API="http://127.0.0.1:8000"; PREFIX="FLOWDB_NPKM_0825"; ROWS=120

def api(path, payload=None):
    request=urllib.request.Request(API+path,data=json.dumps(payload).encode() if payload is not None else None,method="POST" if payload is not None else "GET",headers={"content-type":"application/json","x-flowdb-token":os.environ["FLOWDB_API_TOKEN"]})
    with urllib.request.urlopen(request,timeout=30) as response: return json.load(response)

def wait_job(job_id, timeout=240):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        job=api(f"/api/jobs/{job_id}")
        if job["status"]=="syncing": return job
        if job["status"]=="failed": raise RuntimeError(job.get("error"))
        time.sleep(1)
    raise TimeoutError(job_id)

def main(link_id):
    link=build_store().get_link_payload(link_id)
    source=make_engine(ConnectionConfig.model_validate(link["source"])); target=make_engine(ConnectionConfig.model_validate(link["target"]))
    tables={k:f"{PREFIX}_{k}" for k in ("PKNUM","PKTXT","PKTIME","PKCOMP","TXT","NUM","TIME","RAW","LOB","INTV","BK","NULLS","ALL")}
    ddls={
      "PKNUM":"(ID NUMBER(12) PRIMARY KEY, AMOUNT NUMBER(30,10), NAME NVARCHAR2(500), NOTE CLOB)",
      "PKTXT":"(CODE VARCHAR2(30) PRIMARY KEY, V1 VARCHAR2(1000), R1 RAW(64), B1 BLOB, NOTE CLOB)",
      "PKTIME":"(ID NUMBER(12) PRIMARY KEY, D1 DATE, TS1 TIMESTAMP(6), TZ1 TIMESTAMP(6) WITH TIME ZONE, NOTE VARCHAR2(100))",
      "PKCOMP":"(TENANT_ID NUMBER(8), ORDER_NO VARCHAR2(40), AMOUNT NUMBER(20,6), NOTE CLOB, CONSTRAINT PK_NPKM_COMP PRIMARY KEY(TENANT_ID,ORDER_NO))",
      "TXT":"(CODE VARCHAR2(30) NOT NULL UNIQUE, V_SHORT VARCHAR2(20), V_LONG VARCHAR2(2000), C_FIXED CHAR(20 CHAR), NV NVARCHAR2(500), NC NCHAR(10), NOTE CLOB)",
      "NUM":"(CODE NUMBER(10) NOT NULL UNIQUE, N_INT NUMBER(38,0), N_DEC NUMBER(30,10), N_SMALL NUMBER(6,5), F_BIN BINARY_FLOAT, D_BIN BINARY_DOUBLE, NOTE VARCHAR2(100))",
      "TIME":"(CODE VARCHAR2(30) NOT NULL UNIQUE, D DATE, TS TIMESTAMP(6), TS_TZ TIMESTAMP(6) WITH TIME ZONE, TS_LOCAL TIMESTAMP(6) WITH LOCAL TIME ZONE, NOTE VARCHAR2(100))",
      "RAW":"(CODE VARCHAR2(30) NOT NULL UNIQUE, R RAW(64), B BLOB, NOTE VARCHAR2(100))",
      "LOB":"(CODE VARCHAR2(30) NOT NULL UNIQUE, SHORT_TEXT VARCHAR2(100), BIG_TEXT CLOB, BIG_BIN BLOB, NULL_TEXT CLOB)",
      "INTV":"(CODE VARCHAR2(30) NOT NULL UNIQUE, YM INTERVAL YEAR(4) TO MONTH, DS INTERVAL DAY(3) TO SECOND(6), NOTE VARCHAR2(100))",
      "BK":"(TENANT_ID NUMBER(8), ORDER_NO VARCHAR2(40), AMOUNT NUMBER(20,6), CREATED_AT TIMESTAMP(6), NAME NVARCHAR2(500), NOTE CLOB)",
      "NULLS":"(BUSINESS_ID NUMBER(10), N1 NUMBER(20,4), V1 VARCHAR2(1000 CHAR), D1 DATE, TS1 TIMESTAMP(6), R1 RAW(32), C1 CLOB, B1 BLOB)",
      "ALL":"(CODE VARCHAR2(30), QTY NUMBER(12,2), FLAG CHAR(1), D DATE, NOTE CLOB)",
    }
    with source.begin() as c:
        c.execute(text("ALTER SESSION SET TIME_ZONE='+08:00'"))
        for key,name in tables.items():
            try: c.execute(text(f"DROP TABLE {name} PURGE"))
            except Exception: pass
            c.execute(text(f"CREATE TABLE {name} {ddls[key]}"))
        basic=[{"i":i,"code":f"K{i:04d}","short":("短" if i%2 else "short")+str(i),"long":("中文Abc🙂"*(1+i%30)),"note":None if i%7==0 else f"备注-{i}-中文"} for i in range(1,ROWS+1)]
        c.execute(text(f"INSERT INTO {tables['TXT']} VALUES (:code,:short,:long,RPAD(:short,12,' '),:long,:short,:note)"),basic)
        c.execute(text(f"INSERT INTO {tables['NUM']} VALUES (:i,POWER(10,20)+:i,(:i-60)/100000,MOD(:i,9)/100000,:i/3,:i/7,:note)"),basic)
        c.execute(text(f"INSERT INTO {tables['TIME']} VALUES (:code,DATE '2026-01-01'+:i,TIMESTAMP '2026-01-01 00:00:00.123456'+NUMTODSINTERVAL(:i,'SECOND'),TO_TIMESTAMP_TZ('2026-01-01 00:00:00.654321 +08:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')+NUMTODSINTERVAL(:i,'SECOND'),TIMESTAMP '2026-01-01 00:00:00.111111'+NUMTODSINTERVAL(:i,'SECOND'),:note)"),basic)
        raw_rows=[{**r,"hex":(f"{r['i']:08X}"*4),"blob":bytes((r['i']+j)%256 for j in range(32+r['i']%64))} for r in basic]
        c.execute(text(f"INSERT INTO {tables['RAW']} VALUES (:code,HEXTORAW(:hex),:blob,:note)"),raw_rows)
        lob_rows=[{**r,"big":("中文LOB🙂|"*(30+r['i']%80)),"blob":bytes((r['i']*3+j)%256 for j in range(512+r['i']%2048))} for r in basic]
        c.execute(text(f"INSERT INTO {tables['PKNUM']} VALUES (:i,(:i-60)/100000,:long,:note)"),basic)
        c.execute(text(f"INSERT INTO {tables['PKTXT']} VALUES (:code,:long,HEXTORAW(:hex),:blob,:note)"),raw_rows)
        c.execute(text(f"INSERT INTO {tables['PKTIME']} VALUES (:i,DATE '2025-01-01'+:i,TIMESTAMP '2025-01-01 00:00:00.123456'+NUMTODSINTERVAL(:i,'SECOND'),TO_TIMESTAMP_TZ('2025-01-01 00:00:00.654321 +08:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')+NUMTODSINTERVAL(:i,'SECOND'),:note)"),basic)
        c.execute(text(f"INSERT INTO {tables['PKCOMP']} VALUES (MOD(:i,5)+1,:code,:i/1000,:note)"),basic)
        c.execute(text(f"INSERT INTO {tables['LOB']} VALUES (:code,:short,:big,:blob,NULL)"),lob_rows)
        c.execute(text(f"INSERT INTO {tables['INTV']} VALUES (:code,NUMTOYMINTERVAL(MOD(:i,240),'MONTH'),NUMTODSINTERVAL(:i*123.456,'SECOND'),:note)"),basic)
        c.execute(text(f"INSERT INTO {tables['BK']} VALUES (MOD(:i,5)+1,:code,(:i-60)/1000,SYSTIMESTAMP,:long,:note)"),basic)
        c.execute(text(f"INSERT INTO {tables['NULLS']} VALUES (:i,CASE WHEN MOD(:i,3)=0 THEN NULL ELSE :i/100 END,CASE WHEN MOD(:i,4)=0 THEN NULL ELSE :long END,CASE WHEN MOD(:i,5)=0 THEN NULL ELSE DATE '2026-02-01'+:i END,CASE WHEN MOD(:i,6)=0 THEN NULL ELSE SYSTIMESTAMP END,CASE WHEN MOD(:i,7)=0 THEN NULL ELSE HEXTORAW(:hex) END,CASE WHEN MOD(:i,8)=0 THEN NULL ELSE :note END,CASE WHEN MOD(:i,9)=0 THEN NULL ELSE :blob END)"),raw_rows)
        c.execute(text(f"INSERT INTO {tables['ALL']} VALUES (:code,:i,CASE WHEN MOD(:i,2)=0 THEN 'Y' ELSE 'N' END,DATE '2026-03-01'+:i,:note)"),basic)
    overrides={tables["BK"]:["TENANT_ID","ORDER_NO"],tables["NULLS"]:["BUSINESS_ID"]}
    payload={"name":"QA-无主键-多表字段矩阵","link_id":link_id,"tables":list(tables.values()),"object_types":{v:"table" for v in tables.values()},"batch_size":100,"table_concurrency":4,"existing_table":"drop_and_create","migration_content":"structure_and_data","fail_policy":"stop_on_error","migrate_sequences":False,"sync_mode":"full_and_incremental","cdc_poll_seconds":1,"cdc_window_scn":20000,"cdc_key_overrides":overrides,"cdc_no_key_policy":"all_columns","cdc_allow_source_ddl":True}
    job=api("/api/jobs",payload); job_id=job["id"]; wait_job(job_id)
    with target.connect() as c:
        baseline={key:int(c.execute(text(f"SELECT COUNT(*) FROM {name.lower()}" )).scalar_one()) for key,name in tables.items()}
    if any(v!=ROWS for v in baseline.values()): raise AssertionError(baseline)
    with source.begin() as c:
        c.execute(text(f"UPDATE {tables['PKNUM']} SET ID=10001,AMOUNT=-1234567890.1234567890,NAME=N'数字主键更新🙂',NOTE='主键 CLOB 更新' WHERE ID=1")); c.execute(text(f"DELETE FROM {tables['PKNUM']} WHERE ID=2")); c.execute(text(f"INSERT INTO {tables['PKNUM']} VALUES (10002,0.0000000001,N'数字主键新增','新增')"))
        c.execute(text(f"UPDATE {tables['PKTXT']} SET CODE='K0001X',V1='字符主键更新',R1=HEXTORAW('00FFAA'),B1=HEXTORAW('DEADBEEF'),NOTE='字符主键 CLOB' WHERE CODE='K0001'")); c.execute(text(f"DELETE FROM {tables['PKTXT']} WHERE CODE='K0002'")); c.execute(text(f"INSERT INTO {tables['PKTXT']} VALUES ('K1001','新增',HEXTORAW('ABCDEF'),HEXTORAW('0001FF'),'新增 CLOB')"))
        c.execute(text(f"UPDATE {tables['PKTIME']} SET ID=10001,D1=DATE '1999-12-31',TS1=TIMESTAMP '2026-08-25 12:34:56.999999',TZ1=TO_TIMESTAMP_TZ('2026-08-25 01:02:03.123456 -05:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'),NOTE='时间主键更新' WHERE ID=1")); c.execute(text(f"DELETE FROM {tables['PKTIME']} WHERE ID=2")); c.execute(text(f"INSERT INTO {tables['PKTIME']} VALUES (10002,DATE '2000-02-29',TIMESTAMP '2000-02-29 00:00:00.000001',SYSTIMESTAMP,'闰日新增')"))
        c.execute(text(f"UPDATE {tables['PKCOMP']} SET ORDER_NO='K0001X',AMOUNT=9999999999.123456,NOTE='组合主键更新' WHERE TENANT_ID=2 AND ORDER_NO='K0001'")); c.execute(text(f"DELETE FROM {tables['PKCOMP']} WHERE TENANT_ID=3 AND ORDER_NO='K0002'")); c.execute(text(f"INSERT INTO {tables['PKCOMP']} VALUES (9,'K1001',-0.000001,'组合主键新增')"))
        c.execute(text(f"UPDATE {tables['TXT']} SET CODE='K0001X',V_LONG='更新后中文🙂',NOTE=NULL WHERE CODE='K0001'")); c.execute(text(f"DELETE FROM {tables['TXT']} WHERE CODE='K0002'")); c.execute(text(f"INSERT INTO {tables['TXT']} VALUES ('K1001','新增','新增长文本',RPAD('新增',12,' '),N'新增中文',N'中文','新 CLOB')"))
        c.execute(text(f"UPDATE {tables['NUM']} SET CODE=10001,N_INT=99999999999999999999999999999999999999,N_DEC=-1234567890.1234567890,N_SMALL=.99999,F_BIN=-1.25,D_BIN=1.23456789012345 WHERE CODE=1")); c.execute(text(f"DELETE FROM {tables['NUM']} WHERE CODE=2")); c.execute(text(f"INSERT INTO {tables['NUM']} VALUES (10002,-99999999999999999999999999999999999999,0.0000000001,.00001,3.5,7.25,'新增精度')"))
        c.execute(text(f"UPDATE {tables['TIME']} SET CODE='K0001X',D=DATE '1999-12-31',TS=TIMESTAMP '2026-08-25 23:59:59.999999',TS_TZ=TO_TIMESTAMP_TZ('2026-08-25 23:59:59.123456 -05:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'),NOTE='时间更新' WHERE CODE='K0001'")); c.execute(text(f"DELETE FROM {tables['TIME']} WHERE CODE='K0002'")); c.execute(text(f"INSERT INTO {tables['TIME']} VALUES ('K1001',DATE '2000-02-29',TIMESTAMP '2000-02-29 12:34:56.000001',SYSTIMESTAMP,SYSTIMESTAMP,'闰日')"))
        c.execute(text(f"UPDATE {tables['RAW']} SET CODE='K0001X',R=HEXTORAW('00FF10AA'),B=HEXTORAW('DEADBEEF00'),NOTE='二进制更新' WHERE CODE='K0001'")); c.execute(text(f"DELETE FROM {tables['RAW']} WHERE CODE='K0002'")); c.execute(text(f"INSERT INTO {tables['RAW']} VALUES ('K1001',HEXTORAW('ABCDEF'),HEXTORAW('000102FF'),'二进制新增')"))
        c.execute(text(f"UPDATE {tables['LOB']} SET CODE='K0001X',BIG_TEXT=TO_CLOB('中文LOB更新🙂')||RPAD('文',3900,'文'),BIG_BIN=HEXTORAW(RPAD('AB',4000,'AB')),NULL_TEXT='NULL变非NULL' WHERE CODE='K0001'")); c.execute(text(f"DELETE FROM {tables['LOB']} WHERE CODE='K0002'")); c.execute(text(f"INSERT INTO {tables['LOB']} VALUES ('K1001','新增LOB',TO_CLOB('长')||RPAD('长',3900,'长'),HEXTORAW(RPAD('CD',4000,'CD')),NULL)"))
        c.execute(text(f"UPDATE {tables['INTV']} SET CODE='K0001X',YM=INTERVAL '-12-03' YEAR(4) TO MONTH,DS=INTERVAL '-5 12:34:56.123456' DAY(3) TO SECOND(6),NOTE='间隔更新' WHERE CODE='K0001'")); c.execute(text(f"DELETE FROM {tables['INTV']} WHERE CODE='K0002'")); c.execute(text(f"INSERT INTO {tables['INTV']} VALUES ('K1001',INTERVAL '99-11' YEAR(4) TO MONTH,INTERVAL '123 23:59:59.999999' DAY(3) TO SECOND(6),'间隔新增')"))
        c.execute(text(f"UPDATE {tables['BK']} SET ORDER_NO='K0001X',AMOUNT=9999999999.123456,NAME=N'业务键更新🙂',NOTE='业务 CLOB 更新' WHERE TENANT_ID=2 AND ORDER_NO='K0001'")); c.execute(text(f"DELETE FROM {tables['BK']} WHERE TENANT_ID=3 AND ORDER_NO='K0002'")); c.execute(text(f"INSERT INTO {tables['BK']} VALUES (9,'K1001',-0.000001,SYSTIMESTAMP,N'业务新增','业务新增 CLOB')"))
        c.execute(text(f"UPDATE {tables['NULLS']} SET BUSINESS_ID=10001,N1=NULL,V1='从值改空',D1=NULL,TS1=NULL,R1=NULL,C1='空值更新',B1=NULL WHERE BUSINESS_ID=1")); c.execute(text(f"DELETE FROM {tables['NULLS']} WHERE BUSINESS_ID=2")); c.execute(text(f"INSERT INTO {tables['NULLS']} VALUES (10002,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"))
        c.execute(text(f"UPDATE {tables['ALL']} SET CODE='K0001X',QTY=999.99,FLAG='Y',D=DATE '1990-01-01',NOTE='ALL更新中文' WHERE CODE='K0001' AND QTY=1 AND FLAG='N' AND D=DATE '2026-03-02'")); c.execute(text(f"DELETE FROM {tables['ALL']} WHERE CODE='K0002' AND QTY=2 AND FLAG='Y' AND D=DATE '2026-03-03'")); c.execute(text(f"INSERT INTO {tables['ALL']} VALUES ('K1001',0.01,'N',DATE '2099-12-31',NULL)"))
    expected={"PKNUM":10001,"PKTXT":"K0001X","PKTIME":10001,"PKCOMP":"K0001X","TXT":"K0001X","NUM":10001,"TIME":"K0001X","RAW":"K0001X","LOB":"K0001X","INTV":"K0001X","BK":"K0001X","NULLS":10001,"ALL":"K0001X"}
    end=time.monotonic()+180; checks={}
    while time.monotonic()<end:
        with target.connect() as c:
            checks={}
            for key,name in tables.items():
                if key in {"PKNUM","PKTIME"}: key_col="ID"
                elif key in {"PKCOMP","BK"}: key_col="ORDER_NO"
                elif key=="NULLS": key_col="BUSINESS_ID"
                else: key_col="CODE"
                count=int(c.execute(text(f"SELECT COUNT(*) FROM {name.lower()}" )).scalar_one())
                updated=int(c.execute(text(f"SELECT COUNT(*) FROM {name.lower()} WHERE {key_col.lower()}=:v"),{"v":expected[key]}).scalar_one())
                checks[key]=(count,updated)
        if all(v==(ROWS,1) for v in checks.values()): break
        current=api(f"/api/jobs/{job_id}");
        if current["status"]=="failed": raise RuntimeError(current.get("error"))
        time.sleep(1)
    else: raise AssertionError(checks)
    with target.connect() as c:
        detail={
          "PKNUM":tuple(c.execute(text(f"SELECT ID,AMOUNT,NAME,NOTE FROM {tables['PKNUM'].lower()} WHERE ID=10001")).one()),
          "PKTXT":tuple(c.execute(text(f"SELECT CODE,HEX(R1),HEX(B1),NOTE FROM {tables['PKTXT'].lower()} WHERE CODE='K0001X'")).one()),
          "PKTIME":tuple(c.execute(text(f"SELECT ID,D1,TS1,TZ1,NOTE FROM {tables['PKTIME'].lower()} WHERE ID=10001")).one()),
          "PKCOMP":tuple(c.execute(text(f"SELECT TENANT_ID,ORDER_NO,AMOUNT,NOTE FROM {tables['PKCOMP'].lower()} WHERE ORDER_NO='K0001X'")).one()),
          "NUM":tuple(c.execute(text(f"SELECT N_INT,N_DEC,N_SMALL,F_BIN,D_BIN FROM {tables['NUM'].lower()} WHERE CODE=10001")).one()),
          "TIME":tuple(c.execute(text(f"SELECT D,TS,TS_TZ,NOTE FROM {tables['TIME'].lower()} WHERE CODE='K0001X'")).one()),
          "RAW":tuple(c.execute(text(f"SELECT HEX(R),HEX(B),NOTE FROM {tables['RAW'].lower()} WHERE CODE='K0001X'")).one()),
          "LOB":tuple(c.execute(text(f"SELECT CHAR_LENGTH(BIG_TEXT),OCTET_LENGTH(BIG_BIN),NULL_TEXT FROM {tables['LOB'].lower()} WHERE CODE='K0001X'")).one()),
          "NULLS":tuple(c.execute(text(f"SELECT N1,D1,TS1,R1,B1,C1 FROM {tables['NULLS'].lower()} WHERE BUSINESS_ID=10001")).one()),
          "ALL":tuple(c.execute(text(f"SELECT QTY,FLAG,D,NOTE FROM {tables['ALL'].lower()} WHERE CODE='K0001X'")).one()),
        }
    final=api(f"/api/jobs/{job_id}"); api(f"/api/jobs/{job_id}/finish-sync",{})
    print(json.dumps({"ok":True,"job_id":job_id,"tables":len(tables),"baseline_rows":sum(baseline.values()),"checks":checks,"detail":detail,"cdc_events":final.get("cdc_events"),"cdc_transactions":final.get("cdc_transactions"),"checkpoint_scn":final.get("checkpoint_scn")},ensure_ascii=False,default=str))
    source.dispose(); target.dispose()

if __name__=="__main__": import sys; main(sys.argv[1])
