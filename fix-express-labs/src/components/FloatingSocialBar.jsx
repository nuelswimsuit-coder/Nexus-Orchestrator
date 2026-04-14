import { motion } from 'framer-motion'

// ─── Brand SVG Icons ──────────────────────────────────────────────────────────
const InstagramIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"
    strokeLinecap="round" strokeLinejoin="round" className="w-5 h-5">
    <rect x="2" y="2" width="20" height="20" rx="5" ry="5" />
    <circle cx="12" cy="12" r="4" />
    <circle cx="17.5" cy="6.5" r="1" fill="currentColor" stroke="none" />
  </svg>
)

const FacebookIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
    <path d="M18 2h-3a5 5 0 0 0-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 0 1 1-1h3z" />
  </svg>
)

const TelegramIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
    <path d="M11.944 0A12 12 0 1 0 24 12 12 12 0 0 0 11.944 0zm5.788 8.087-1.97 9.29c-.145.658-.537.818-1.084.508l-3-2.21-1.447 1.394c-.16.16-.295.295-.605.295l.213-3.053 5.56-5.023c.242-.213-.054-.333-.373-.12L7.17 13.695l-2.965-.924c-.644-.203-.658-.644.136-.953l11.57-4.461c.537-.194 1.006.131.821.73z" />
  </svg>
)

const TikTokIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
    <path d="M19.59 6.69a4.83 4.83 0 0 1-3.77-4.25V2h-3.45v13.67a2.89 2.89 0 0 1-2.88 2.5 2.89 2.89 0 0 1-2.89-2.89 2.89 2.89 0 0 1 2.89-2.89c.28 0 .54.04.79.1V9.01a6.33 6.33 0 0 0-.79-.05 6.34 6.34 0 0 0-6.34 6.34 6.34 6.34 0 0 0 6.34 6.34 6.34 6.34 0 0 0 6.33-6.34V8.69a8.18 8.18 0 0 0 4.78 1.52V6.76a4.85 4.85 0 0 1-1.01-.07z" />
  </svg>
)

// ─── Social links config ──────────────────────────────────────────────────────
const SOCIALS = [
  {
    id: 'instagram',
    label: 'Instagram',
    href: 'https://instagram.com/fix_express_labs',
    icon: InstagramIcon,
    color: 'from-purple-500 via-pink-500 to-orange-400',
    glow: 'rgba(236,72,153,0.7)',
  },
  {
    id: 'facebook',
    label: 'Facebook',
    href: 'https://facebook.com/fixexpresslabs',
    icon: FacebookIcon,
    color: 'from-blue-600 to-blue-400',
    glow: 'rgba(59,130,246,0.7)',
  },
  {
    id: 'telegram',
    label: 'Telegram',
    href: 'https://t.me/fix_express_labs',
    icon: TelegramIcon,
    color: 'from-sky-400 to-cyan-400',
    glow: 'rgba(34,211,238,0.7)',
  },
  {
    id: 'tiktok',
    label: 'TikTok',
    href: 'https://tiktok.com/@fix_express_labs',
    icon: TikTokIcon,
    color: 'from-slate-100 to-white',
    glow: 'rgba(255,255,255,0.5)',
  },
]

// ─── Single icon button ───────────────────────────────────────────────────────
function SocialIcon({ item, index, variant }) {
  const Icon = item.icon

  return (
    <motion.a
      href={item.href}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={item.label}
      title={item.label}
      variants={{
        hidden: variant === 'bar'
          ? { opacity: 0, x: 40 }
          : { opacity: 0, y: 20 },
        visible: {
          opacity: 1,
          x: 0,
          y: 0,
          transition: { delay: 0.6 + index * 0.1, duration: 0.5, ease: [0.22, 1, 0.36, 1] },
        },
      }}
      whileHover={{
        scale: 1.25,
        transition: { type: 'spring', stiffness: 400, damping: 15 },
      }}
      className="relative group cursor-pointer"
      style={{ WebkitTapHighlightColor: 'transparent' }}
    >
      {/* Glow ring */}
      <span
        className="absolute inset-0 rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-300 blur-sm"
        style={{ background: item.glow }}
      />

      {/* Icon container */}
      <span
        className={`relative flex items-center justify-center w-9 h-9 rounded-full
          bg-white/8 border border-white/10
          group-hover:border-white/30 text-white/60 group-hover:text-white
          transition-all duration-300
          group-hover:drop-shadow-[0_0_8px_#0070f3]`}
        style={{
          backdropFilter: 'blur(8px)',
        }}
      >
        <Icon />
      </span>

      {/* Tooltip — only on bar variant */}
      {variant === 'bar' && (
        <span
          className="absolute right-12 top-1/2 -translate-y-1/2
            bg-slate-900/90 border border-white/10 text-white text-xs font-medium
            px-2.5 py-1 rounded-lg whitespace-nowrap
            opacity-0 group-hover:opacity-100 pointer-events-none
            transition-opacity duration-200"
          style={{ backdropFilter: 'blur(8px)' }}
        >
          {item.label}
        </span>
      )}
    </motion.a>
  )
}

// ─── Vertical bar (desktop) ───────────────────────────────────────────────────
function VerticalBar() {
  return (
    <motion.div
      initial={{ opacity: 0, x: 60 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.6, delay: 0.4, ease: [0.22, 1, 0.36, 1] }}
      className="hidden md:flex fixed right-4 top-1/2 -translate-y-1/2 z-50
        flex-col gap-3 items-center
        bg-white/5 backdrop-blur-md border border-white/10
        rounded-full py-4 px-2.5"
    >
      {/* Decorative top line */}
      <span className="w-px h-6 bg-gradient-to-b from-transparent to-white/20 rounded-full" />

      <motion.div
        initial="hidden"
        animate="visible"
        className="flex flex-col gap-3"
      >
        {SOCIALS.map((item, i) => (
          <SocialIcon key={item.id} item={item} index={i} variant="bar" />
        ))}
      </motion.div>

      {/* Decorative bottom line */}
      <span className="w-px h-6 bg-gradient-to-b from-white/20 to-transparent rounded-full" />
    </motion.div>
  )
}

// ─── Horizontal strip (mobile — rendered inside Hero) ─────────────────────────
export function MobileSocialStrip() {
  return (
    <motion.div
      initial="hidden"
      animate="visible"
      className="flex md:hidden items-center justify-center gap-4 mt-6"
    >
      {SOCIALS.map((item, i) => (
        <SocialIcon key={item.id} item={item} index={i} variant="strip" />
      ))}
    </motion.div>
  )
}

// ─── Default export — the fixed vertical bar ──────────────────────────────────
export default function FloatingSocialBar() {
  return <VerticalBar />
}
