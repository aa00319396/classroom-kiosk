'use strict';
const http=require('node:http'),crypto=require('node:crypto'),{WebSocketServer,WebSocket}=require('ws');
const {C,newGame,step,finalStep,upgrade}=require('./core.cjs');
const rooms=new Map(),scores=[],W=1280,H=720,MAX_ROOMS=80,RECONNECT_MS=60000,LOBBY_MS=600000,IDLE_MS=10000;
const zeros=()=>({x:0,y:0,dash:false,ult:false}),now=()=>Date.now();
const server=http.createServer((req,res)=>{if(req.url==='/health'){res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'no-store'});return res.end(JSON.stringify({ok:true,version:'2.71',rooms:rooms.size}))}res.writeHead(404);res.end('V2.71 game server')});
const wss=new WebSocketServer({server,maxPayload:2048,perMessageDeflate:false});
const send=(ws,m)=>{if(ws?.readyState===WebSocket.OPEN&&ws.bufferedAmount<250000)ws.send(JSON.stringify(m))};
const notify=(r,m)=>r.slots.forEach(s=>send(s?.ws,m));
const status=r=>({type:'lobby',code:r.code,slots:r.slots.map(s=>s?{connected:!!s.ws,job:s.job,ready:s.ready}:null),started:!!r.game});
const bad=(ws,message)=>send(ws,{type:'error',message});
function freshSlot(ws){return {ws,token:crypto.randomBytes(24).toString('hex'),job:0,ready:false,last:now(),leftAt:0,input:zeros(),choice:null}}
function attach(ws,r,i,slot){slot.ws=ws;slot.last=now();slot.leftAt=0;slot.input=zeros();ws.room=r;ws.index=i;send(ws,{type:'joined',code:r.code,player:i,token:slot.token,width:W,height:H});notify(r,status(r));if(r.game)send(ws,snapshot(r))}
function snapshot(r){return {type:'state',game:r.game,connected:r.slots.map(s=>!!s?.ws),choices:r.slots.map(s=>!!s?.choice),paused:r.paused,ranking:scores}}
function leave(ws,explicit=false){let r=ws.room,i=ws.index;ws.room=null;if(!r||r.slots[i]?.ws!==ws)return;let s=r.slots[i];s.ws=null;s.leftAt=now();s.input=zeros();if(explicit){if(r.game){r.game.mode='over';r.game.reason='隊友離開';r.paused=false;r.aborted=true;notify(r,snapshot(r))}else{r.slots[i]=null;notify(r,status(r));if(!r.slots.some(Boolean))rooms.delete(r.code)}}else if(!r.game)notify(r,status(r));}
function start(r){r.game=newGame(r.slots.map(s=>s.job));r.game.p.forEach((p,i)=>{p.x=W*(i?.62:.38);p.y=H*.55});r.paused=false;r.aborted=false;r.scored=false;r.last=now();r.slots.forEach(s=>{s.choice=null;s.input=zeros()});notify(r,snapshot(r))}
const upgrades=new Set(['damage','rate','speed','life','range']);
function handle(ws,m){if(!m||typeof m!=='object'||Array.isArray(m))return;
if(m.type==='create'||m.type==='join'||m.type==='rejoin'){if(ws.room)return bad(ws,'已在房間內');if(m.type==='create'){if(rooms.size>=MAX_ROOMS)return bad(ws,'伺服器房間已滿');let code;do{code=String(crypto.randomInt(1000,10000))}while(rooms.has(code));let r={code,slots:[freshSlot(ws),null],game:null,created:now(),last:now(),paused:false,scored:false};rooms.set(code,r);attach(ws,r,0,r.slots[0]);return}
let code=String(m.code||'');if(!/^\d{4}$/.test(code))return bad(ws,'請輸入四位數房號');let r=rooms.get(code);if(!r)return bad(ws,'房間不存在或已過期');if(m.type==='rejoin'){let i=r.slots.findIndex(s=>s&&s.token===m.token);if(i<0)return bad(ws,'重連憑證無效');let s=r.slots[i];if(s.ws&&s.ws!==ws){let old=s.ws;old.room=null;old.close(4001,'replaced')}attach(ws,r,i,s);return}
if(r.game||r.slots.every(Boolean))return bad(ws,'房間已滿或已開始');let i=r.slots.findIndex(s=>!s);r.slots[i]=freshSlot(ws);attach(ws,r,i,r.slots[i]);return}
let r=ws.room,i=ws.index,s=r?.slots[i];if(!r||!s||s.ws!==ws)return bad(ws,'請先加入房間');s.last=now();
if(m.type==='leave'){leave(ws,true);send(ws,{type:'left'});return}
if(m.type==='ready'&&!r.game){let j=Number(m.job);if(!Number.isInteger(j)||j<0||j>=C.length)return bad(ws,'職業無效');s.job=j;s.ready=!!m.ready;notify(r,status(r));return}
if(m.type==='start'&&i===0&&!r.game){if(!r.slots.every(s=>s?.ws&&s.ready))return bad(ws,'兩人都準備完成才能開始');start(r);return}
if(m.type==='restart'&&i===0&&r.game?.mode==='over'&&r.slots.every(s=>s?.ws)){start(r);return}
if(!r.game)return;
if(m.type==='input'){const x=Number(m.x),y=Number(m.y);if(!Number.isFinite(x)||!Number.isFinite(y))return;let n=Math.max(1,Math.hypot(x,y));s.input={x:x/n,y:y/n,dash:s.input.dash||m.dash===true,ult:s.input.ult||m.ult===true};return}
if(m.type==='choose'&&r.game.mode==='upgrade'&&upgrades.has(m.id)){s.choice=m.id;if(r.slots.every(s=>s?.choice)){r.game.p.forEach((p,i)=>upgrade(p,r.slots[i].choice));r.slots.forEach(s=>s.choice=null);r.game.mode='play'}notify(r,snapshot(r));return}
}
wss.on('connection',ws=>{ws.alive=true;ws.rate=0;ws.rateAt=now();ws.on('pong',()=>ws.alive=true);ws.on('message',raw=>{let t=now();if(t-ws.rateAt>1000){ws.rateAt=t;ws.rate=0}if(++ws.rate>80){ws.close(1008,'rate limit');return}try{handle(ws,JSON.parse(raw.toString()))}catch(e){bad(ws,'無法處理訊息')}});ws.on('close',()=>leave(ws));ws.on('error',()=>{});send(ws,{type:'hello',version:'2.71'})});
let last=now(),broadcast=0;
const tick=setInterval(()=>{let t=now(),dt=Math.min(.1,Math.max(0,(t-last)/1000));last=t;broadcast+=dt;let push=broadcast>=.066;if(push)broadcast=0;for(let [code,r] of rooms){if(!r.game){for(let i=0;i<2;i++){let s=r.slots[i];if(s&&!s.ws&&t-s.leftAt>RECONNECT_MS)r.slots[i]=null}if(t-r.created>LOBBY_MS||!r.slots.some(Boolean)){notify(r,{type:'error',message:'房間已過期'});rooms.delete(code)}continue}
if(r.game.mode==='over'){if(!r.scored){r.scored=true;if(!r.aborted){scores.push({score:r.game.score,stage:r.game.stage,time:Math.round(r.game.t)});scores.sort((a,b)=>b.score-a.score);scores.splice(10)}}if(!r.overAt)r.overAt=t;if(t-r.overAt>LOBBY_MS){rooms.delete(code);continue}if(push)notify(r,snapshot(r));continue}
r.overAt=0;
for(let s of r.slots){if(s.ws&&t-s.last>IDLE_MS){let ws=s.ws;leave(ws);ws.close(4002,'timeout')}}
let missing=r.slots.some(s=>!s.ws);r.paused=missing;if(missing){if(r.slots.some(s=>!s.ws&&t-s.leftAt>RECONNECT_MS)){r.game.mode='over';r.game.reason='隊友重連逾時';r.aborted=true;r.paused=false}}else if(r.game.mode==='play'){let inputs=r.slots.map(s=>t-s.last>300?zeros():s.input);step(r.game,dt,W,H,inputs);r.slots.forEach(s=>{s.input.dash=false;s.input.ult=false})}else if(r.game.mode==='ending')finalStep(r.game,dt);
if(push)notify(r,snapshot(r))}},33);
const ping=setInterval(()=>{for(let ws of wss.clients){if(!ws.alive){ws.terminate();continue}ws.alive=false;ws.ping()}},15000);
server.listen(Number(process.env.PORT)||10000,'0.0.0.0',()=>console.log('COOP 2.71 listening'));
process.on('SIGTERM',()=>{clearInterval(tick);clearInterval(ping);for(let ws of wss.clients)ws.close(1012);server.close(()=>process.exit(0))});