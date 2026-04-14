import { motion } from 'framer-motion'

// ─── Brand SVG Icons ──────────────────────────────────────────────────────────
const InstagramIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
    strokeLinecap="round" strokeLinejoin="round" className="w-[18px] h-[18px]">
    <rect x="2" y="2" width="20" height="20" rx="5" ry="5" />
    <circle cx="12" cy="12" r="4" />
    <circle cx="17.5" cy="6.5" r="1" fill="currentColor" stroke="none" />
  </svg>
)

const FacebookIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-[18px] h-[18px]">
    <path d="M18 2h-3a5 5 0 0 0-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 0 1 1-1h3z" />
  </svg>
)

const TelegramIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-[18px] h-[18px]">
    <path d="M11.944 0A12 12 0 1 0 24 12 12 12 0 0 0 11.944 0zm5.788 8.087-1.97 9.29c-.145.658-.537.818-1.084.508l-3-2.21-1.447 1.394c-.16.16-.295.295-.605.295l.213-3.053 5.56-5.023c.242-.213-.054-.333-.373-.12L7.17 13.695l-2.965-.924c-.644-.203-.658-.644.136-.953l11.57-4.461c.537-.194 1.006.131.821.73z" />
  </svg>
)

const TikTokIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-[18px] h-[18px]">
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
    // Instagram gradient background
    bg: 'linear-gradient(135deg, #833ab4, #fd1d1d, #fcb045)',
    glow: 'rgba(253,29,29,0.6)',
  },
  {
    id: 'facebook',
    label: 'Facebook',
    href: 'https://facebook.com/fixexpresslabs',
    icon: FacebookIcon,
    bg: 'linear-gradient(135deg, #1877f2, #42a5f5)',
    glow: 'rgba(24,119,242,0.6)',
  },
  {
    id: 'telegram',
    label: 'Telegram',
    href: 'https://t.me/fix_express_labs',
    icon: TelegramIcon,
    bg: 'linear-gradient(135deg, #0088cc, #29b6f6)',
    glow: 'rgba(0,136,204,0.6)',
  },
  {
    id: 'tiktok',
    label: 'TikTok',
    href: 'https://tiktok.com/@fix_express_labs',
    icon: TikTokIcon,
    bg: 'linear-gradient(135deg, #010101, #69c9d0)',
    glow: 'rgba(105,201,208,0.6)',
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
        hidden: variant === 'bar' ? { opacity: 0, x: 40 } : { opacity: 0, y: 20 },
        visible: {
          opacity: 1, x: 0, y: 0,
          transition: { delay: 0.5 + index * 0.1, duration: 0.5, ease: [0.22, 1, 0.36, 1] },
        },
      }}
      whileHover={{
        scale: 1.2,
        transition: { type: 'spring', stiffness: 400, damping: 15 },
      }}
      className="relative group cursor-pointer"
    >
      {/* Glow halo on hover */}
      <span
        className="absolute inset-0 rounded-full opacity-0 group-hover:opacity-100 transition-opacity duration-300"
        style={{
          background: item.glow,
          filter: 'blur(10px)',
          transform: 'scale(1.3)',
        }}
      />

      {/* Icon circle with brand gradient */}
      <span
        className="relative flex items-center justify-center w-10 h-10 rounded-full text-white shadow-lg
          transition-shadow duration-300 group-hover:shadow-[0_0_16px_rgba(0,112,243,0.7)]"
        style={{ background: item.bg }}
      >
        <Icon />
      </span>

      {/* Tooltip label — bar variant only */}
      {variant === 'bar' && (
        <span
          className="absolute right-14 top-1/2 -translate-y-1/2
            bg-slate-900/95 border border-white/10 text-white text-xs font-semibold
            px-3 py-1.5 rounded-lg whitespace-nowrap pointer-events-none
            opacity-0 group-hover:opacity-100 transition-opacity duration-200
            shadow-xl"
        >
          {item.label}
          {/* Arrow */}
          <span className="absolute right-[-5px] top-1/2 -translate-y-1/2 w-0 h-0
            border-t-4 border-b-4 border-l-4 border-transparent border-l-slate-900/95" />
        </span>
      )}
    </motion.a>
  )
}

// ─── Vertical pill bar — desktop ─────────────────────────────────────────────
function VerticalBar() {
  return (
    <motion.div
      initial={{ opacity: 0, x: 70 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.7, delay: 0.3, ease: [0.22, 1, 0.36, 1] }}
      className="hidden md:flex fixed right-4 top-1/2 -translate-y-1/2 z-50
        flex-col gap-3 items-center
        py-4 px-2.5 rounded-full
        border border-white/15"
      style={{
        background: 'rgba(10,15,30,0.65)',
        backdropFilter: 'blur(16px)',
        WebkitBackdropFilter: 'blur(16px)',
        boxShadow: '0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.08)',
      }}
    >
      {/* Top accent line */}
      <span className="w-px h-5 rounded-full"
        style={{ background: 'linear-gradient(to bottom, transparent, rgba(0,112,243,0.5))' }} />

      <motion.div initial="hidden" animate="visible" className="flex flex-col gap-3">
        {SOCIALS.map((item, i) => (
          <SocialIcon key={item.id} item={item} index={i} variant="bar" />
        ))}
      </motion.div>

      {/* Bottom accent line */}
      <span className="w-px h-5 rounded-full"
        style={{ background: 'linear-gradient(to bottom, rgba(0,112,243,0.5), transparent)' }} />
    </motion.div>
  )
}

// ─── Horizontal strip — mobile (inside Hero) ─────────────────────────────────
export function MobileSocialStrip() {
  return (
    <motion.div
      initial="hidden"
      animate="visible"
      className="flex md:hidden items-center justify-center gap-5 mt-8 pb-2"
    >
      {SOCIALS.map((item, i) => (
        <SocialIcon key={item.id} item={item} index={i} variant="strip" />
      ))}
    </motion.div>
  )
}

// ─── Default export ───────────────────────────────────────────────────────────
export default function FloatingSocialBar() {
  return <VerticalBar />
}
