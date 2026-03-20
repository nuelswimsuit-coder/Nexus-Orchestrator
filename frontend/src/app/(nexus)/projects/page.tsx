import type { Metadata } from "next";
import ProjectsClient from "./ProjectsClient";

export const metadata: Metadata = { title: "Project Hub — TeleFix OS" };

export default function ProjectHubPage() {
  return <ProjectsClient />;
}
