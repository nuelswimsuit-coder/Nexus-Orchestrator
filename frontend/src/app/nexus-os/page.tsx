"use client";
import NexusOsGodMode from "@/components/NexusOsGodMode";
import { useEffect } from "react";

export default function NexusOsPage() {
  useEffect(() => {
    // #region agent log — capture ALL clicks at document level
    const handler = (e: MouseEvent) => {
      const el = e.target as HTMLElement;
      fetch('http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'c21539'},body:JSON.stringify({sessionId:'c21539',location:'page.tsx:DOC_CLICK',message:'document click captured',data:{tag:el?.tagName,id:el?.id,cls:el?.className?.toString?.()?.slice(0,80),x:e.clientX,y:e.clientY,target:el?.outerHTML?.slice(0,120)},timestamp:Date.now(),hypothesisId:'F'})}).catch(()=>{});
    };
    document.addEventListener('click', handler, true); // capture phase
    return () => document.removeEventListener('click', handler, true);
    // #endregion
  }, []);
  return <NexusOsGodMode />;
}
