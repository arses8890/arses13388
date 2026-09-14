import { connect } from "cloudflare:sockets";
const CONTROL_TOKEN="__PANEL_TOKEN__";
const enc=new TextEncoder();
const json=(d,s=200)=>new Response(JSON.stringify(d),{status:s,headers:{"content-type":"application/json; charset=utf-8"}});
const auth=r=>r.headers.get("authorization")===`Bearer ${CONTROL_TOKEN}`;
const norm=u=>String(u||"").trim().toLowerCase();
function endpoint(v){
  const s=String(v||"").trim(); if(!s)return null;
  const m=s.match(/^\[([^\]]+)\](?::(\d+))?$/); if(m)return {hostname:m[1],port:+(m[2]||443)};
  const i=s.lastIndexOf(":");
  return i>0&&/^\d+$/.test(s.slice(i+1))?{hostname:s.slice(0,i),port:+s.slice(i+1)}:{hostname:s,port:443};
}
async function allUsers(){return await SPIDER_KV.get("users","json")||{};}
async function choose(country){
  const key=`country:${norm(country)}`;
  const loc=await SPIDER_KV.get(key,"json");
  if(!loc||!Array.isArray(loc.proxies)||!loc.proxies.length)return null;
  const list=loc.proxies.filter(Boolean);
  const start=(Number(loc.cursor)||0)%list.length;
  for(let n=0;n<list.length;n++){
    const i=(start+n)%list.length, ep=endpoint(list[i]); if(!ep)continue;
    try{
      const sock=await Promise.race([connect(ep),new Promise((_,rej)=>setTimeout(()=>rej(new Error("proxy timeout")),5000))]);
      loc.cursor=(i+1)%list.length;
      await SPIDER_KV.put(key,JSON.stringify(loc));
      return sock;
    }catch{}
  }
  return null;
}
function route(path){
  const p=String(path||"").split("/").filter(Boolean);
  if(p.length!==4||p[0]!=="ws"||p[2]!=="route")return null;
  const uuid=p[1].trim(), country=p[3].trim().toLowerCase();
  if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(uuid)||!/^[a-z]{2}$/i.test(country))return null;
  return {uuid,country};
}
async function relay(request, r){
  const users=await allUsers(), user=users[norm(r.uuid)];
  if(!user||user.disabled)return new Response("Forbidden",{status:403});
  if(user.expire&&Date.now()/1000>=Number(user.expire))return new Response("Expired",{status:403});
  const countries=Array.isArray(user.countries)?user.countries.map(norm):[];
  if(countries.length&&!countries.includes(r.country))return new Response("Country not allowed",{status:403});
  const sock=await choose(r.country); if(!sock)return new Response("No healthy proxy for country",{status:503});
  const pair=new WebSocketPair(), client=pair[0], server=pair[1]; server.accept();
  const writer=sock.writable.getWriter(), reader=sock.readable.getReader();
  server.addEventListener("message",async ev=>{
    try{
      let d=ev.data;
      if(typeof d==="string")d=enc.encode(d); else if(d instanceof ArrayBuffer)d=new Uint8Array(d); else if(d instanceof Blob)d=new Uint8Array(await d.arrayBuffer());
      if(d&&d.byteLength)await writer.write(d);
    }catch{try{server.close(1011,"write error")}catch{}}
  });
  const toServer=(async()=>{try{while(true){const x=await reader.read();if(x.done)break;if(x.value)server.send(x.value)}}catch{}finally{try{server.close()}catch{}}})();
  const cleanup=async()=>{try{await writer.close()}catch{}try{await reader.cancel()}catch{}};
  server.addEventListener("close",cleanup,{once:true});
  return new Response(null,{status:101,webSocket:client});
}
export default {async fetch(request){
  const url=new URL(request.url);
  if(url.pathname==="/"||url.pathname==="/health")return new Response("ok");
  if(url.pathname==="/panel/status"){
    if(!auth(request))return json({ok:false},401);
    const u=await allUsers(); return json({ok:true,users:Object.keys(u).length,online:0,traffic:Number(await SPIDER_KV.get("traffic")||0)});
  }
  if(url.pathname==="/panel/config"&&request.method==="POST"){
    if(!auth(request))return json({ok:false},401);
    const b=await request.json(), u={};
    for(const x of b.users||[]){if(x.uuid)u[norm(x.uuid)]={...x,uuid:x.uuid};}
    await SPIDER_KV.put("users",JSON.stringify(u));
    for(const l of b.routes?.locations||[]){
      const c=norm(l.code); if(!c)continue;
      const ps=Array.isArray(l.proxies)?l.proxies.filter(Boolean):(l.proxy?[l.proxy]:[]);
      await SPIDER_KV.put(`country:${c}`,JSON.stringify({code:c,country:l.country||c.toUpperCase(),proxies:ps,cursor:0}));
    }
    return json({ok:true,users:Object.keys(u).length});
  }
  if(url.pathname.startsWith("/api/user/")&&auth(request)){
    const u=await allUsers(), x=u[norm(url.pathname.slice(10))]; return x?json({ok:true,user:x}):json({ok:false},404);
  }
  if(request.headers.get("Upgrade")!=="websocket")return new Response("Not found",{status:404});
  const r=route(url.pathname); if(!r)return new Response("Invalid route",{status:404});
  return relay(request,r);
}};\n