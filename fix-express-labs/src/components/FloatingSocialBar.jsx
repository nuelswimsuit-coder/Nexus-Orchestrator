import { motion } from 'framer-motion'

const WhatsAppIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
    <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/>
    <path d="M12 0C5.373 0 0 5.373 0 12c0 2.124.558 4.118 1.529 5.845L.057 23.522a.75.75 0 0 0 .921.921l5.677-1.472A11.95 11.95 0 0 0 12 24c6.627 0 12-5.373 12-12S18.627 0 12 0zm0 22c-1.891 0-3.667-.523-5.188-1.433l-.372-.217-3.853.999 1.02-3.735-.236-.386A9.956 9.956 0 0 1 2 12C2 6.477 6.477 2 12 2s10 4.477 10 10-4.477 10-10 10z"/>
  </svg>
)

const InstagramIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="w-5 h-5">
    <rect x="2" y="2" width="20" height="20" rx="5" ry="5"/>
    <circle cx="12" cy="12" r="4"/>
    <circle cx="17.5" cy="6.5" r="1" fill="currentColor" stroke="none"/>
  </svg>
)

const FacebookIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
    <path d="M18 2h-3a5 5 0 0 0-5 5v3H7v4h3v8h4v-8h3l1-4h-4V7a1 1 0 0 1 1-1h3z"/>
  </svg>
)

const TelegramIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
    <path d="M11.944 0A12 12 0 1 0 24 12 12 12 0 0 0 11.944 0zm5.788 8.087-1.97 9.29c-.145.658-.537.818-1.084.508l-3-2.21-1.447 1.394c-.16.16-.295.295-.605.295l.213-3.053 5.56-5.023c.242-.213-.054-.333-.373-.12L7.17 13.695l-2.965-.924c-.644-.203-.658-.644.136-.953l11.57-4.461c.537-.194 1.006.131.821.73z"/>
  </svg>
)

const TikTokIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
    <path d="M19.59 6.69a4.83 4.83 0 0 1-3.77-4.25V2h-3.45v13.67a2.89 2.89 0 0 1-2.88 2.5 2.89 2.89 0 0 1-2.89-2.89 2.89 2.89 0 0 1 2.89-2.89c.28 0 .54.04.79.1V9.01a6.33 6.33 0 0 0-.79-.05 6.34 6.34 0 0 0-6.34 6.34 6.34 6.34 0 0 0 6.34 6.34 6.34 6.34 0 0 0 6.33-6.34V8.69a8.18 8.18 0 0 0 4.78 1.52V6.76a4.85 4.85 0 0 1-1.01-.07z"/>
  </svg>
)

const WA_HREF = 'https://wa.me/972504951109?text=' + encodeURIComponent('היי! אני מעוניין בתיקון מהיר 🔧')

const SOCIALS = [
  {
    id: 'whatsapp', label: 'WhatsApp', href: WA_HREF,
    icon: WhatsAppIcon,
    base: 'bg-[#25D366]',
    hover: '#20ba5a',
    shadow: '0 0 20px rgba(37,211,102,0.6)',
  },
  {
    id: 'instagram', label: 'Instagram',
    href: 'https://instagram.com/fix_express_labs',
    icon: InstagramIcon,
    base: 'bg-gradient-to-tr from-yellow-400 via-pink-500 to-purple-600',
    hover: null,
    shadow: '0 0 20px rgba(236,72,153,0.6)',
    style: { background: 'linear-gradient(135deg, #f9a825, #e91e8c, #7b1fa2)' },
  },
  {
    id: 'facebook', label: 'Facebook',
    href: 'https://facebook.com/fixexpresslabs',
    icon: FacebookIcon,
    base: 'bg-[#1877F2]',
    hover: '#1565d8',
    shadow: '0 0 20px rgba(24,119,242,0.6)',
  },
  {
    id: 'telegram', label: 'Telegram',
    href: 'https://t.me/fix_express_labs',
    icon: TelegramIcon,
    base: 'bg-[#0088cc]',
    hover: '#0077b5',
    shadow: '0 0 20px rgba(0,136,204,0.6)',
  },
  {
    id: 'tiktok', label: 'TikTok',
    href: 'https://tiktok.com/@fix_express_labs',
    icon: TikTokIcon,
    base: 'bg-[#010101]',
    hover: '#1a1a1a',
    shadow: '0 0 20px rgba(105,201,208,0.6)',
    border: '1px solid rgba(105,201,208,0.4)',
  },
]

function SocialBtn({ item, index, variant }) {
  const Icon = item.icon
  return (
    <motion.a
      href={item.href}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={item.label}
      initial={variant === 'bar' ? { opacity: 0, x: 30 } : { opacity: 0, y: 16 }}
      animate={{ opacity: 1, x: 0, y: 0 }}
      transition={{ delay: 0.4 + index * 0.08, duration: 0.4, ease: [0.22,1,0.36,1] }}
      whileHover={{ scale: 1.18 }}
      whileTap={{ scale: 0.95 }}
      className="relative group cursor-pointer flex-shrink-0"
    >
      {/* Glow on hover */}
      <span
        className="absolute inset-0 rounded-xl opacity-0 group-hover:opacity-100 transition-opacity duration-300"
        style={{ boxShadow: item.shadow, borderRadius: 12 }}
      />
      {/* Icon button */}
      <span
        className="relative flex items-center justify-center w-10 h-10 rounded-xl text-white transition-all duration-200"
        style={item.style || {}}
      >
        {!item.style && (
          <span className={`absolute inset-0 rounded-xl ${item.base}`} style={item.border ? { border: item.border } : {}} />
        )}
        {item.style && (
          <span className="absolute inset-0 rounded-xl" style={item.style} />
        )}
        <span className="relative z-10"><Icon /></span>
      </span>

      {/* Tooltip */}
      {variant === 'bar' && (
        <span className="absolute right-14 top-1/2 -translate-y-1/2 bg-zinc-900 border border-white/10 text-white text-xs font-medium px-2.5 py-1 rounded-lg whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity duration-150 shadow-xl">
          {item.label}
        </span>
      )}
    </motion.a>
  )
}

// ── Desktop: fixed vertical pill on the right ─────────────────────────────────
function VerticalBar() {
  return (
    <motion.div
      initial={{ opacity: 0, x: 60 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.6, delay: 0.3 }}
      className="hidden md:flex fixed right-4 top-1/2 -translate-y-1/2 z-50 flex-col gap-2.5 items-center py-4 px-2.5 rounded-2xl"
      style={{
        background: 'rgba(9,9,11,0.75)',
        backdropFilter: 'blur(20px)',
        border: '1px solid rgba(255,255,255,0.1)',
        boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
      }}
    >
      <span className="w-px h-4 rounded-full bg-white/10" />
      {SOCIALS.map((item, i) => (
        <SocialBtn key={item.id} item={item} index={i} variant="bar" />
      ))}
      <span className="w-px h-4 rounded-full bg-white/10" />
    </motion.div>
  )
}

// ── Mobile: horizontal strip inside Hero ──────────────────────────────────────
export function MobileSocialStrip() {
  return (
    <div className="flex md:hidden items-center justify-center gap-3 mt-8">
      {SOCIALS.map((item, i) => (
        <SocialBtn key={item.id} item={item} index={i} variant="strip" />
      ))}
    </div>
  )
}

export default function FloatingSocialBar() {
  return <VerticalBar />
}
