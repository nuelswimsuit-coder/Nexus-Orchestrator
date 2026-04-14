import { motion } from 'framer-motion'
import { MessageCircle, Send, ExternalLink } from 'lucide-react'

const WA_NUMBER = '972504951109'
const IG_HANDLE = 'fix_express_labs'
const TG_HANDLE = 'fix_express_labs'

// Instagram SVG
const InstagramIcon = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zM12 0C8.741 0 8.333.014 7.053.072 2.695.272.273 2.69.073 7.052.014 8.333 0 8.741 0 12c0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98C8.333 23.986 8.741 24 12 24c3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98C15.668.014 15.259 0 12 0zm0 5.838a6.162 6.162 0 100 12.324 6.162 6.162 0 000-12.324zM12 16a4 4 0 110-8 4 4 0 010 8zm6.406-11.845a1.44 1.44 0 100 2.881 1.44 1.44 0 000-2.881z"/>
  </svg>
)

const TelegramIcon = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/>
  </svg>
)

const TikTokIcon = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M19.59 6.69a4.83 4.83 0 01-3.77-4.25V2h-3.45v13.67a2.89 2.89 0 01-2.88 2.5 2.89 2.89 0 01-2.89-2.89 2.89 2.89 0 012.89-2.89c.28 0 .54.04.79.1V9.01a6.27 6.27 0 00-.79-.05 6.34 6.34 0 00-6.34 6.34 6.34 6.34 0 006.34 6.34 6.34 6.34 0 006.33-6.34V8.69a8.15 8.15 0 004.77 1.52V6.75a4.85 4.85 0 01-1-.06z"/>
  </svg>
)

const SOCIALS = [
  {
    id: 'whatsapp',
    name: 'WhatsApp',
    handle: '050-495-1109',
    tagline: 'הדרך הכי מהירה לקבוע תיקון',
    Icon: MessageCircle,
    gradient: 'from-[#25D366] to-[#128C7E]',
    border: 'border-[#25D366]/30',
    btnBg: 'bg-[#25D366] hover:bg-[#1fb957]',
    viewUrl: `https://wa.me/${WA_NUMBER}`,
    msgUrl: `https://wa.me/${WA_NUMBER}?text=${encodeURIComponent('היי! אני מעוניין בתיקון 🔧')}`,
    viewLabel: 'פתח צ׳אט',
    msgLabel: 'שלח הודעה',
  },
  {
    id: 'instagram',
    name: 'Instagram',
    handle: `@${IG_HANDLE}`,
    tagline: 'לפני/אחרי, טיפים ותיקונים מדהימים',
    Icon: InstagramIcon,
    gradient: 'from-[#833ab4] via-[#fd1d1d] to-[#fcb045]',
    border: 'border-purple-500/30',
    btnBg: 'bg-gradient-to-r from-purple-600 to-pink-500 hover:opacity-90',
    viewUrl: `https://instagram.com/${IG_HANDLE}`,
    msgUrl: `https://ig.me/m/${IG_HANDLE}`,
    viewLabel: 'צפה בפרופיל',
    msgLabel: 'שלח DM',
  },
  {
    id: 'telegram',
    name: 'Telegram',
    handle: `@${TG_HANDLE}`,
    tagline: 'עדכונים, דילים ומבצעים ראשון לגרסאות',
    Icon: TelegramIcon,
    gradient: 'from-[#0088cc] to-[#00a0e6]',
    border: 'border-[#0088cc]/30',
    btnBg: 'bg-[#0088cc] hover:bg-[#0077bb]',
    viewUrl: `https://t.me/${TG_HANDLE}`,
    msgUrl: `https://t.me/${TG_HANDLE}`,
    viewLabel: 'פתח ערוץ',
    msgLabel: 'שלח הודעה',
  },
  {
    id: 'tiktok',
    name: 'TikTok',
    handle: `@${IG_HANDLE}`,
    tagline: 'סרטוני תיקון מהירים שישאירו אתכם ער בלילה',
    Icon: TikTokIcon,
    gradient: 'from-[#010101] via-[#ff0050] to-[#00f2ea]',
    border: 'border-[#ff0050]/30',
    btnBg: 'bg-[#ff0050] hover:bg-[#e00046]',
    viewUrl: `https://tiktok.com/@${IG_HANDLE}`,
    msgUrl: `https://tiktok.com/@${IG_HANDLE}`,
    viewLabel: 'צפה בפרופיל',
    msgLabel: 'עקוב',
  },
]

export default function SocialHub() {
  return (
    <section className="py-20 px-6">
      <div className="max-w-5xl mx-auto">
        <div className="text-center mb-12">
          <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
            צור קשר
          </div>
          <h2 className="text-4xl sm:text-5xl font-black mb-4 tracking-tight">
            מצא אותנו <span className="text-gradient">בכל מקום</span>
          </h2>
          <p className="text-slate-400 text-lg">בחר את הדרך שנוחה לך ביותר ליצור קשר.</p>
        </div>

        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
          {SOCIALS.map((s, i) => (
            <motion.div
              key={s.id}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.08 }}
              whileHover={{ y: -4 }}
              className={`group rounded-2xl border ${s.border} overflow-hidden flex flex-col`}
              style={{ background: 'linear-gradient(135deg, rgba(255,255,255,0.04), rgba(0,0,0,0.2))' }}
            >
              {/* Icon header with gradient */}
              <div className={`bg-gradient-to-br ${s.gradient} p-5 flex items-center gap-3`}>
                <s.Icon className="w-7 h-7 text-white" />
                <div>
                  <p className="font-bold text-white text-sm">{s.name}</p>
                  <p className="text-white/70 text-xs">{s.handle}</p>
                </div>
              </div>

              {/* Body */}
              <div className="p-4 flex-1 flex flex-col gap-4">
                <p className="text-slate-400 text-xs leading-relaxed">{s.tagline}</p>

                <div className="flex flex-col gap-2 mt-auto">
                  <a
                    href={s.viewUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center justify-center gap-1.5 py-2 rounded-lg glass border border-white/10 hover:border-white/25 text-white text-xs font-semibold transition-all"
                  >
                    <ExternalLink className="w-3.5 h-3.5" />
                    {s.viewLabel}
                  </a>
                  <a
                    href={s.msgUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={`flex items-center justify-center gap-1.5 py-2.5 rounded-lg ${s.btnBg} text-white text-xs font-bold transition-all`}
                  >
                    <Send className="w-3.5 h-3.5" />
                    {s.msgLabel}
                  </a>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  )
}
