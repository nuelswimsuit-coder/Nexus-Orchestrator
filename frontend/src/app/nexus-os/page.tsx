"use client";
import NexusOsGodMode from "@/components/NexusOsGodMode";
import { useEffect } from "react";

export default function NexusOsPage() {
  useEffect(() => {
    // #region agent log
    const logEvent = (type: string, e: MouseEvent) => {
      const el = document.elementFromPoint(e.clientX, e.clientY) as HTMLElement;
      fetch('http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'c21539'},body:JSON.stringify({sessionId:'c21539',location:'page.tsx:'+type,message:type+' event',data:{tag:el?.tagName,cls:el?.className?.toString?.()?.slice(0,80),x:e.clientX,y:e.clientY,topEl:el?.outerHTML?.slice(0,150),pointerEvents:el?window.getComputedStyle(el).pointerEvents:'?'},timestamp:Date.now(),hypothesisId:'H'})}).catch(()=>{});
    };
    const onClick = (e: MouseEvent) => logEvent('CLICK', e);
    const onDown  = (e: MouseEvent) => logEvent('MOUSEDOWN', e);
    document.addEventListener('click',     onClick, true);
    document.addEventListener('mousedown', onDown,  true);
    // Send a heartbeat immediately to confirm the listener is registered
    fetch('http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'c21539'},body:JSON.stringify({sessionId:'c21539',location:'page.tsx:MOUNT',message:'listeners registered',data:{},timestamp:Date.now(),hypothesisId:'H'})}).catch(()=>{});
    return () => {
      document.removeEventListener('click',     onClick, true);
      document.removeEventListener('mousedown', onDown,  true);
    };
    // #endregion
  }, []);
  return <NexusOsGodMode />;
}
