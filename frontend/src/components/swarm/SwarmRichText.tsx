"use client";

import isoPurify, { addHook as swarmHtmlAddHook } from "isomorphic-dompurify";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeSanitize from "rehype-sanitize";

let purifyLinkHookInstalled = false;

function sanitizeSwarmHtml(raw: string): string {
  if (!purifyLinkHookInstalled) {
    purifyLinkHookInstalled = true;
    swarmHtmlAddHook("afterSanitizeAttributes", (node) => {
      if (node.tagName === "A" && node instanceof Element) {
        const href = node.getAttribute("href") ?? "";
        if (/^\s*javascript:/i.test(href) || /^\s*data:/i.test(href)) {
          node.removeAttribute("href");
          return;
        }
        node.setAttribute("target", "_blank");
        node.setAttribute("rel", "noopener noreferrer");
      }
    });
  }
  return isoPurify.sanitize(raw, {
    ALLOWED_TAGS: [
      "a",
      "b",
      "strong",
      "i",
      "em",
      "u",
      "code",
      "pre",
      "br",
      "p",
      "span",
      "ul",
      "ol",
      "li",
      "blockquote",
    ],
    ALLOWED_ATTR: ["href", "title", "target", "rel", "class"],
  });
}

function looksLikeHtmlSnippet(s: string): boolean {
  const t = s.trim();
  if (!t) return false;
  if (/<[a-z][\s\S]*>/i.test(t)) return true;
  if (/&lt;[a-z]/i.test(t)) return true;
  return false;
}

export function SwarmRichText({
  text,
  className = "",
}: {
  text: string;
  className?: string;
}) {
  const raw = (text ?? "").trim();
  if (!raw) return null;

  if (looksLikeHtmlSnippet(raw)) {
    const html = sanitizeSwarmHtml(raw);
    if (!html.trim()) return null;
    return (
      <div
        className={`swarm-rich-html break-words [&_a]:text-cyan-400 [&_a]:underline [&_a]:underline-offset-2 hover:[&_a]:text-cyan-300 [&_p]:my-1 [&_p]:last:mb-0 [&_ul]:my-1 [&_ul]:list-disc [&_ul]:ps-4 [&_ol]:my-1 [&_ol]:list-decimal [&_ol]:ps-4 [&_code]:rounded [&_code]:bg-slate-950/80 [&_code]:px-1 [&_code]:text-[0.9em] ${className}`}
        dir="auto"
        // eslint-disable-next-line react/no-danger -- sanitized with DOMPurify
        dangerouslySetInnerHTML={{ __html: html }}
      />
    );
  }

  return (
    <div className={`swarm-rich-md break-words ${className}`} dir="auto">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypeSanitize]}
        components={{
          a: ({ href, children, ...props }) => (
            <a
              {...props}
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="text-cyan-400 underline underline-offset-2 hover:text-cyan-300"
            >
              {children}
            </a>
          ),
          p: ({ children }) => <p className="my-1 last:mb-0 leading-relaxed">{children}</p>,
          code: ({ className: codeClass, children, ...props }) => {
            const inline = !codeClass;
            if (inline) {
              return (
                <code
                  {...props}
                  className="rounded bg-slate-950/80 px-1 py-0.5 text-[0.9em] font-mono text-slate-200"
                >
                  {children}
                </code>
              );
            }
            return (
              <code
                {...props}
                className={`${codeClass ?? ""} block overflow-x-auto rounded-lg bg-slate-950/90 p-2 text-[0.85em] font-mono text-slate-200`}
              >
                {children}
              </code>
            );
          },
          ul: ({ children }) => <ul className="my-1 list-disc ps-4">{children}</ul>,
          ol: ({ children }) => <ol className="my-1 list-decimal ps-4">{children}</ol>,
          li: ({ children }) => <li className="leading-relaxed">{children}</li>,
        }}
      >
        {raw}
      </ReactMarkdown>
    </div>
  );
}
