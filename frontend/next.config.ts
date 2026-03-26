import type { NextConfig } from "next";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001";

const nextConfig: NextConfig = {
  reactCompiler: true,

  // Proxy all /api/* requests to the FastAPI backend.
  // This allows components to use relative fetch("/api/...") without CORS issues
  // and without needing to know the backend URL at build time.
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${API_URL}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
