"use client";

/**
 * CyberGrid — Three.js WebGL perspective grid background.
 *
 * Renders a glowing blue/purple infinite-scroll floor grid using WebGL.
 * Features: dual-layer grid with additive blending, floating particles,
 * atmospheric exponential fog, pulsing horizon glow, and animated camera.
 *
 * Replaces the original canvas/2D implementation with full WebGL rendering
 * for richer visual fidelity (glow, fog, depth).
 */

import { useEffect, useRef } from "react";
import * as THREE from "three";

interface CyberGridProps {
  /** Opacity 0–1. Default 0.65 */
  opacity?: number;
  /** Primary grid colour (hex). Default electric blue #00b4ff */
  color?: string;
  /** Accent glow colour (hex). Default violet #9b4dff */
  glowColor?: string;
  /** Animation speed multiplier. Default 1 */
  speed?: number;
}

function applyLineMat(
  obj: THREE.LineSegments,
  apply: (m: THREE.LineBasicMaterial) => void,
) {
  const mats = Array.isArray(obj.material) ? obj.material : [obj.material];
  mats.forEach((m) => apply(m as THREE.LineBasicMaterial));
}

export default function CyberGrid({
  opacity = 0.65,
  color = "#00b4ff",
  glowColor = "#9b4dff",
  speed = 1,
}: CyberGridProps) {
  const mountRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const mount = mountRef.current;
    if (!mount) return;

    /* ── Scene ─────────────────────────────────────────────────────── */
    const scene = new THREE.Scene();
    // Exponential fog fades the grid into the obsidian void
    scene.fog = new THREE.FogExp2(0x050505, 0.016);

    /* ── Camera ────────────────────────────────────────────────────── */
    const camera = new THREE.PerspectiveCamera(
      62,
      window.innerWidth / window.innerHeight,
      0.1,
      600,
    );
    camera.position.set(0, 5.5, 18);
    camera.lookAt(0, 0, -12);

    /* ── Renderer ──────────────────────────────────────────────────── */
    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(window.innerWidth, window.innerHeight);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setClearColor(0x000000, 0);
    mount.appendChild(renderer.domElement);

    const c1 = new THREE.Color(color);
    const c2 = new THREE.Color(glowColor);

    /* ── Coarse grid (primary blue) ────────────────────────────────── */
    const GRID_SIZE = 500;
    const COARSE_DIV = 50;
    const CELL = GRID_SIZE / COARSE_DIV; // 10 units per cell

    const grid1 = new THREE.GridHelper(GRID_SIZE, COARSE_DIV, c1, c1);
    applyLineMat(grid1, (m) => {
      m.transparent = true;
      m.opacity = 0.45;
      m.blending = THREE.AdditiveBlending;
      m.depthWrite = false;
    });
    scene.add(grid1);

    /* ── Fine grid (accent purple) ─────────────────────────────────── */
    const grid2 = new THREE.GridHelper(GRID_SIZE, COARSE_DIV * 4, c2, c2);
    applyLineMat(grid2, (m) => {
      m.transparent = true;
      m.opacity = 0.13;
      m.blending = THREE.AdditiveBlending;
      m.depthWrite = false;
    });
    scene.add(grid2);

    /* ── Horizon glow strip ────────────────────────────────────────── */
    const horizonGeo = new THREE.PlaneGeometry(GRID_SIZE, 3);
    const horizonMat = new THREE.MeshBasicMaterial({
      color: c2,
      transparent: true,
      opacity: 0.07,
      side: THREE.DoubleSide,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });
    const horizonMesh = new THREE.Mesh(horizonGeo, horizonMat);
    horizonMesh.rotation.x = -Math.PI / 2;
    horizonMesh.position.set(0, 0.1, -20);
    scene.add(horizonMesh);

    /* ── Centre vanishing-point orb ───────────────────────────────── */
    const orbGeo = new THREE.SphereGeometry(0.5, 16, 16);
    const orbMat = new THREE.MeshBasicMaterial({
      color: c2,
      transparent: true,
      opacity: 0.6,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });
    const orb = new THREE.Mesh(orbGeo, orbMat);
    orb.position.set(0, 0.5, -40);
    scene.add(orb);

    // Orb halo (large transparent sphere for bloom-like effect)
    const haloGeo = new THREE.SphereGeometry(3, 16, 16);
    const haloMat = new THREE.MeshBasicMaterial({
      color: c2,
      transparent: true,
      opacity: 0.04,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
      side: THREE.BackSide,
    });
    const halo = new THREE.Mesh(haloGeo, haloMat);
    halo.position.copy(orb.position);
    scene.add(halo);

    /* ── Floating particles ────────────────────────────────────────── */
    const PARTICLE_COUNT = 500;
    const pPositions = new Float32Array(PARTICLE_COUNT * 3);
    for (let i = 0; i < PARTICLE_COUNT; i++) {
      pPositions[i * 3]     = (Math.random() - 0.5) * 280;
      pPositions[i * 3 + 1] = Math.random() * 28 + 0.5;
      pPositions[i * 3 + 2] = (Math.random() - 0.5) * 280;
    }
    const particleGeo = new THREE.BufferGeometry();
    particleGeo.setAttribute("position", new THREE.BufferAttribute(pPositions, 3));
    const particleMat = new THREE.PointsMaterial({
      color: c1,
      size: 0.18,
      transparent: true,
      opacity: 0.55,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
      sizeAttenuation: true,
    });
    const particles = new THREE.Points(particleGeo, particleMat);
    scene.add(particles);

    /* ── Atmospheric point lights ──────────────────────────────────── */
    const pLight1 = new THREE.PointLight(c2, 5, 120);
    pLight1.position.set(0, 4, -30);
    scene.add(pLight1);

    const pLight2 = new THREE.PointLight(c1, 2.5, 60);
    pLight2.position.set(0, 3, -8);
    scene.add(pLight2);

    /* ── Animation loop ────────────────────────────────────────────── */
    let rafId: number;
    let t = 0;

    const animate = () => {
      rafId = requestAnimationFrame(animate);
      t += 0.016 * speed;

      // Scroll grids toward viewer — seamless cell-sized loop
      const scrollZ = (t * 2.8) % CELL;
      grid1.position.z = scrollZ;
      grid2.position.z = scrollZ;

      // Pulse atmosphere
      const pulse = 0.5 + 0.5 * Math.sin(t * 0.65);
      const pulse2 = 0.5 + 0.5 * Math.sin(t * 0.4 + 1.2);

      pLight1.intensity = 4 + pulse * 5;
      pLight2.intensity = 2 + pulse2 * 2;
      horizonMat.opacity = 0.04 + pulse * 0.07;

      // Orb pulse
      orbMat.opacity = 0.45 + pulse * 0.35;
      haloMat.opacity = 0.025 + pulse * 0.03;
      orb.scale.setScalar(0.85 + pulse * 0.3);
      halo.scale.setScalar(0.9 + pulse2 * 0.2);

      // Slow particle rotation + slight camera drift
      particles.rotation.y = t * 0.012;
      camera.position.x = Math.sin(t * 0.08) * 0.8;

      renderer.render(scene, camera);
    };

    animate();

    /* ── Resize handler ────────────────────────────────────────────── */
    const onResize = () => {
      const w = window.innerWidth;
      const h = window.innerHeight;
      camera.aspect = w / h;
      camera.updateProjectionMatrix();
      renderer.setSize(w, h);
    };
    window.addEventListener("resize", onResize);

    /* ── Cleanup ───────────────────────────────────────────────────── */
    return () => {
      cancelAnimationFrame(rafId);
      window.removeEventListener("resize", onResize);
      if (mount.contains(renderer.domElement)) {
        mount.removeChild(renderer.domElement);
      }
      renderer.dispose();
      grid1.geometry.dispose();
      grid2.geometry.dispose();
      horizonGeo.dispose();
      orbGeo.dispose();
      haloGeo.dispose();
      particleGeo.dispose();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [color, glowColor, speed]);

  return (
    <div
      ref={mountRef}
      aria-hidden="true"
      style={{
        position: "fixed",
        inset: 0,
        width: "100vw",
        height: "100vh",
        zIndex: 0,
        pointerEvents: "none",
        opacity,
      }}
    />
  );
}
