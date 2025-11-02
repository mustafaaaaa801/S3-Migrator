export async function saveConfig(cfg){
const res = await fetch('/api/config', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(cfg)});
return res.json();
}
export async function start(name){
const res = await fetch(`/api/start/${name}`, {method:'POST'});
return res.json();
}
export async function status(taskId){
const res = await fetch(`/api/status/${taskId}`);
return res.json();
}