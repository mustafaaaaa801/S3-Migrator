import React, {useState} from 'react'
import { saveConfig, start } from './api'

export default function App(){
  const [cfg, setCfg] = useState({name:'demo', src_bucket:'', target_endpoint:'', dry_run:true});
  return (
    <div style={{maxWidth:800, margin:'20px auto', fontFamily:'Arial'}}>
      <h2>S3 Migrator — واجهة إدارية</h2>
      <div>
        <label>Config name</label>
        <input value={cfg.name} onChange={e=>setCfg({...cfg, name:e.target.value})} />
      </div>
      <div>
        <label>Source Bucket</label>
        <input value={cfg.src_bucket} onChange={e=>setCfg({...cfg, src_bucket:e.target.value})} />
      </div>
      <div>
        <label>Target Endpoint</label>
        <input value={cfg.target_endpoint} onChange={e=>setCfg({...cfg, target_endpoint