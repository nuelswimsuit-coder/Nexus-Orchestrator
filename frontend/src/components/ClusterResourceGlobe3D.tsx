"use client";

import { Canvas } from "@react-three/fiber";
import { OrbitControls, PerspectiveCamera } from "@react-three/drei";
import { useMemo } from "react";
import * as THREE from "three";

export interface HeatNode {
  node_id: string;
  role: string;
  cpu_percent: number;
  ram_percent: number;
  online?: boolean;
}

function ResourcePillar({
  x,
  cpu,
  ram,
  color,
}: {
  x: number;
  cpu: number;
  ram: number;
  color: string;
}) {
  const cpuH = useMemo(() => 0.35 + (Math.min(100, cpu) / 100) * 2.2, [cpu]);
  const ramH = useMemo(() => 0.35 + (Math.min(100, ram) / 100) * 2.2, [ram]);
  return (
    <group position={[x, 0, 0]}>
      <mesh position={[-0.35, cpuH / 2, 0]} castShadow>
        <boxGeometry args={[0.55, Math.max(cpuH, 0.2), 0.55]} />
        <meshStandardMaterial
          color={color}
          metalness={0.35}
          roughness={0.25}
          emissive={color}
          emissiveIntensity={0.35}
        />
      </mesh>
      <mesh position={[0.35, ramH / 2, 0]} castShadow>
        <boxGeometry args={[0.55, Math.max(ramH, 0.2), 0.55]} />
        <meshStandardMaterial
          color="#9b4dff"
          metalness={0.4}
          roughness={0.2}
          emissive="#6b21a8"
          emissiveIntensity={0.28}
        />
      </mesh>
      {/* floor label plane */}
      <mesh position={[0, -0.05, 0.6]} rotation={[-Math.PI / 2, 0, 0]}>
        <planeGeometry args={[2.2, 0.5]} />
        <meshBasicMaterial color="#0a0e1a" transparent opacity={0.85} />
      </mesh>
    </group>
  );
}

function Scene({ nodes }: { nodes: HeatNode[] }) {
  const palette = useMemo(
    () => ["#00b4ff", "#38d4ff", "#00e096", "#ffb800", "#ff3355"],
    [],
  );
  const spaced = useMemo(() => {
    const n = Math.max(nodes.length, 1);
    const step = n <= 1 ? 0 : 2.4 / (n - 1);
    return nodes.map((node, i) => ({
      node,
      x: (i - (n - 1) / 2) * (step || 0),
      color: palette[i % palette.length],
    }));
  }, [nodes, palette]);

  return (
    <>
      <color attach="background" args={["#040608"]} />
      <ambientLight intensity={0.35} />
      <directionalLight position={[4, 8, 4]} intensity={1.1} castShadow />
      <pointLight position={[-3, 2, 2]} intensity={0.6} color="#00b4ff" />
      <PerspectiveCamera makeDefault position={[0, 2.1, 5.2]} fov={48} />
      <OrbitControls enablePan={false} minPolarAngle={0.55} maxPolarAngle={1.35} />
      <gridHelper args={[10, 20, "#1a2235", "#0d1120"]} position={[0, -0.2, 0]} />
      {spaced.map(({ node, x, color }) => (
        <ResourcePillar key={node.node_id} x={x} cpu={node.cpu_percent} ram={node.ram_percent} color={color} />
      ))}
    </>
  );
}

export default function ClusterResourceGlobe3D({ nodes }: { nodes: HeatNode[] }) {
  const safe = nodes.length ? nodes : [
    { node_id: "master", role: "master", cpu_percent: 12, ram_percent: 40, online: true },
    { node_id: "gaming-95", role: "worker", cpu_percent: 70, ram_percent: 95, online: true },
    { node_id: "linux-90", role: "worker", cpu_percent: 55, ram_percent: 90, online: true },
  ];

  return (
    <div className="h-[260px] w-full overflow-hidden rounded-xl border border-[var(--color-surface-4)] bg-black/40">
      <Canvas shadows dpr={[1, 2]} gl={{ antialias: true, toneMapping: THREE.ACESFilmicToneMapping }}>
        <Scene nodes={safe} />
      </Canvas>
      <div className="flex flex-wrap gap-2 border-t border-[var(--color-surface-4)]/60 bg-[var(--color-surface-0)]/80 px-3 py-2 font-mono text-[0.58rem] text-[var(--color-text-muted)]">
        <span className="text-[var(--color-accent)]">CPU</span>
        <span>· left column</span>
        <span className="text-[#9b4dff]">RAM</span>
        <span>· right column</span>
      </div>
    </div>
  );
}
