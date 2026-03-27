"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function PolymarketDeckPage() {
  const router = useRouter();
  useEffect(() => { router.replace("/nexus-os?tab=poly-trading"); }, [router]);
  return null;
}
