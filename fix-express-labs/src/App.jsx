import { useState, useRef, useEffect, useCallback } from 'react'
import { motion, AnimatePresence, useInView } from 'framer-motion'
import {
  Zap, MessageCircle, Clock, Shield, MapPin, CheckCircle2, Star,
  ChevronDown, Phone, Cpu, Thermometer, Wrench, Search, X,
  Smartphone, Laptop, Gamepad2, Tablet,
} from 'lucide-react'
import HeroVideo from './components/HeroVideo'
import { SmartphoneRepairIcon, LaptopRepairIcon, ConsoleRepairIcon, MicrosolderingIcon } from './components/Icons'
import PriceEstimator from './components/PriceEstimator'
import FAQ from './components/FAQ'
import SocialHub from './components/SocialHub'
import About from './pages/About'
import Store from './pages/Store'
import AdminDashboard from './pages/AdminDashboard'

// ─── Constants ────────────────────────────────────────────────────────────────
const WA_NUMBER = '972504951109'
const WA_DISPLAY = '050-495-1109'
const WA_URL = `https://wa.me/${WA_NUMBER}?text=${encodeURIComponent('היי! אני מעוניין בתיקון מהיר עד הבית 🔧')}`
const ADDRESS = 'רחוב סיני, פתח תקווה'
const MAPS_URL = 'https://www.google.com/maps/search/?api=1&query=רחוב+סיני+פתח+תקווה'
const isAdmin = typeof window !== 'undefined' && window.location.pathname === '/admin'

// ─── Tabs ─────────────────────────────────────────────────────────────────────
const TABS = [
  { id: 'home',    label: 'בית' },
  { id: 'about',   label: 'קצת עלינו' },
  { id: 'pricing', label: 'מחירון' },
  { id: 'faq',     label: 'שאלות' },
  { id: 'store',   label: 'קנה / מכור' },
]

// ─── Animation helpers ────────────────────────────────────────────────────────
const tabVariants = {
  enter:  (dir) => ({ x: dir > 0 ? 60 : -60, opacity: 0 }),
  center: { x: 0, opacity: 1, transition: { duration: 0.4, ease: [0.22, 1, 0.36, 1] } },
  exit:   (dir) => ({ x: dir > 0 ? -60 : 60, opacity: 0, transition: { duration: 0.25 } }),
}

const fadeUp = {
  hidden: { opacity: 0, y: 28 },
  visible: (i = 0) => ({
    opacity: 1, y: 0,
    transition: { duration: 0.55, delay: i * 0.08, ease: [0.22, 1, 0.36, 1] },
  }),
}
const stagger = { visible: { transition: { staggerChildren: 0.08 } } }

function Reveal({ children, className = '', delay = 0 }) {
  const ref = useRef(null)
  const inView = useInView(ref, { once: true, margin: '-70px' })
  return (
    <motion.div
      ref={ref}
      variants={stagger}
      initial="hidden"
      animate={inView ? 'visible' : 'hidden'}
      className={className}
      style={{ '--delay': delay }}
    >
      {children}
    </motion.div>
  )
}

// ─── Custom Cursor ────────────────────────────────────────────────────────────
function CustomCursor() {
  const dot   = useRef(null)
  const ring  = useRef(null)
  const pos   = useRef({ x: 0, y: 0 })
  const ring_pos = useRef({ x: 0, y: 0 })

  useEffect(() => {
    let raf
    const onMove = (e) => { pos.current = { x: e.clientX, y: e.clientY } }
    const onEnter = () => ring.current?.classList.add('scale-[2.5]', 'border-electric', 'bg-electric/10')
    const onLeave = () => ring.current?.classList.remove('scale-[2.5]', 'border-electric', 'bg-electric/10')

    document.addEventListener('mousemove', onMove)
    document.querySelectorAll('a,button,[role="button"]').forEach((el) => {
      el.addEventListener('mouseenter', onEnter)
      el.addEventListener('mouseleave', onLeave)
    })

    const loop = () => {
      if (dot.current) {
        dot.current.style.transform = `translate(${pos.current.x - 4}px, ${pos.current.y - 4}px)`
      }
      if (ring.current) {
        ring_pos.current.x += (pos.current.x - ring_pos.current.x) * 0.12
        ring_pos.current.y += (pos.current.y - ring_pos.current.y) * 0.12
        ring.current.style.transform = `translate(${ring_pos.current.x - 18}px, ${ring_pos.current.y - 18}px)`
      }
      raf = requestAnimationFrame(loop)
    }
    raf = requestAnimationFrame(loop)
    return () => {
      cancelAnimationFrame(raf)
      document.removeEventListener('mousemove', onMove)
    }
  }, [])

  return (
    <>
      <div ref={dot} className="fixed top-0 left-0 w-2 h-2 bg-electric rounded-full z-[9999] pointer-events-none hidden sm:block" style={{ willChange: 'transform' }} />
      <div ref={ring} className="fixed top-0 left-0 w-9 h-9 rounded-full border border-electric/40 z-[9998] pointer-events-none hidden sm:block transition-all duration-200" style={{ willChange: 'transform' }} />
    </>
  )
}

// ─── Magnetic Button ──────────────────────────────────────────────────────────
function MagneticBtn({ children, href, className = '', strength = 0.35, onClick }) {
  const ref = useRef(null)
  const [offset, setOffset] = useState({ x: 0, y: 0 })

  const onMove = useCallback((e) => {
    const el = ref.current
    if (!el) return
    const rect = el.getBoundingClientRect()
    const cx = rect.left + rect.width / 2
    const cy = rect.top + rect.height / 2
    setOffset({ x: (e.clientX - cx) * strength, y: (e.clientY - cy) * strength })
  }, [strength])

  const onLeave = () => setOffset({ x: 0, y: 0 })

  const Tag = href ? motion.a : motion.button
  const extra = href ? { href, target: '_blank', rel: 'noopener noreferrer' } : { onClick }

  return (
    <Tag
      ref={ref}
      onMouseMove={onMove}
      onMouseLeave={onLeave}
      animate={{ x: offset.x, y: offset.y }}
      transition={{ type: 'spring', stiffness: 200, damping: 15 }}
      className={className}
      {...extra}
    >
      {children}
    </Tag>
  )
}

// ─── Navbar ───────────────────────────────────────────────────────────────────
function Navbar({ activeTab, setTab }) {
  const [scrolled, setScrolled] = useState(false)
  const [mobileOpen, setMobileOpen] = useState(false)

  useEffect(() => {
    const fn = () => setScrolled(window.scrollY > 20)
    window.addEventListener('scroll', fn)
    return () => window.removeEventListener('scroll', fn)
  }, [])

  return (
    <header className={`fixed top-0 inset-x-0 z-50 transition-all duration-300 ${scrolled ? 'glass border-b border-white/10' : ''}`}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between gap-4">
        {/* Logo */}
        <button onClick={() => setTab('home')} className="flex items-center gap-2.5 flex-shrink-0">
          <div className="w-8 h-8 rounded-lg bg-electric flex items-center justify-center glow-blue">
            <Zap className="w-4 h-4 text-white" fill="white" />
          </div>
          <span className="font-black text-base tracking-tight leading-tight">
            Fix <span className="text-gradient">Express</span> Labs
          </span>
        </button>

        {/* Desktop tabs */}
        <nav className="hidden md:flex items-center gap-1 glass rounded-full px-2 py-1.5 border border-white/10">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setTab(tab.id)}
              className={`relative px-4 py-1.5 rounded-full text-sm font-semibold transition-colors ${
                activeTab === tab.id ? 'text-white' : 'text-slate-400 hover:text-white'
              }`}
            >
              {activeTab === tab.id && (
                <motion.div
                  layoutId="tab-pill"
                  className="absolute inset-0 bg-electric rounded-full"
                  style={{ zIndex: -1 }}
                  transition={{ type: 'spring', stiffness: 300, damping: 30 }}
                />
              )}
              {tab.label}
            </button>
          ))}
        </nav>

        {/* CTA + mobile */}
        <div className="flex items-center gap-2">
          <MagneticBtn
            href={WA_URL}
            className="hidden sm:flex items-center gap-2 px-4 py-2 rounded-full bg-[#25D366] hover:bg-[#1fb957] text-white font-bold text-sm shadow-lg shadow-green-900/30"
          >
            <MessageCircle className="w-4 h-4" />
            {WA_DISPLAY}
          </MagneticBtn>

          {/* Hamburger */}
          <button
            className="md:hidden glass border border-white/10 rounded-lg p-2"
            onClick={() => setMobileOpen((v) => !v)}
            aria-label="תפריט"
          >
            <div className="w-4 h-0.5 bg-white mb-1 transition-all" style={{ transformOrigin: 'center', transform: mobileOpen ? 'rotate(45deg) translate(1px, 6px)' : '' }} />
            <div className={`w-4 h-0.5 bg-white mb-1 transition-all ${mobileOpen ? 'opacity-0' : ''}`} />
            <div className="w-4 h-0.5 bg-white transition-all" style={{ transformOrigin: 'center', transform: mobileOpen ? 'rotate(-45deg) translate(1px, -6px)' : '' }} />
          </button>
        </div>
      </div>

      {/* Mobile menu */}
      <AnimatePresence>
        {mobileOpen && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.25 }}
            className="md:hidden glass border-t border-white/10 overflow-hidden"
          >
            <div className="px-4 py-3 flex flex-col gap-1">
              {TABS.map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => { setTab(tab.id); setMobileOpen(false) }}
                  className={`text-right px-4 py-2.5 rounded-xl text-sm font-semibold transition-all ${
                    activeTab === tab.id ? 'bg-electric text-white' : 'text-slate-400 hover:bg-white/5'
                  }`}
                >
                  {tab.label}
                </button>
              ))}
              <a href={WA_URL} target="_blank" rel="noopener noreferrer"
                className="flex items-center justify-center gap-2 py-3 rounded-xl bg-[#25D366] text-white font-bold text-sm mt-1">
                <MessageCircle className="w-4 h-4" fill="white" />
                WhatsApp — {WA_DISPLAY}
              </a>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </header>
  )
}

// ─── Hero ─────────────────────────────────────────────────────────────────────
function Hero({ setTab }) {
  return (
    <section className="relative min-h-screen">
      <HeroVideo intervalMs={8000} fadeDurationMs={1400}>
        {/* Parallax grid */}
        <div
          className="absolute inset-0 pointer-events-none"
          style={{
            backgroundImage: 'linear-gradient(rgba(0,112,243,0.1) 1px,transparent 1px),linear-gradient(90deg,rgba(0,112,243,0.1) 1px,transparent 1px)',
            backgroundSize: '60px 60px',
            zIndex: 4,
          }}
        />
        <div className="relative min-h-screen flex items-center justify-center pt-16 px-6" style={{ zIndex: 5 }}>
          <div className="max-w-5xl mx-auto text-center">
            <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}
              className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-6 text-sm text-electric-light font-medium">
              <span className="w-2 h-2 rounded-full bg-electric animate-pulse" />
              מעבדה ניידת | פתח תקווה | {WA_DISPLAY}
            </motion.div>

            <motion.h1 initial={{ opacity: 0, y: 30 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.35 }}
              className="text-5xl sm:text-6xl md:text-7xl lg:text-8xl font-black leading-[1.05] mb-5 tracking-tight drop-shadow-2xl">
              המעבדה המתקדמת
              <br /><span className="text-gradient">בישראל — אצלך בחנייה</span>
            </motion.h1>

            <motion.p initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.5 }}
              className="text-lg sm:text-xl text-slate-200 max-w-2xl mx-auto mb-10 leading-relaxed drop-shadow-lg">
              תיקון סמארטפונים, מחשבים וקונסולות עם ציוד קצה טכנולוגי — ישירות אליך.
            </motion.p>

            <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.65 }}
              className="flex flex-col sm:flex-row gap-4 justify-center items-center">
              <MagneticBtn href={WA_URL}
                className="flex items-center gap-2 px-10 py-4 rounded-full bg-electric hover:bg-electric-light text-white font-black text-lg glow-blue shadow-2xl shadow-electric/40">
                <MessageCircle className="w-5 h-5" />
                קבע תור עכשיו
              </MagneticBtn>
              <MagneticBtn onClick={() => setTab('pricing')}
                className="flex items-center gap-2 px-10 py-4 rounded-full glass border border-white/30 hover:border-electric/60 text-white font-bold text-lg backdrop-blur">
                מחירון מלא
              </MagneticBtn>
            </motion.div>

            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.85 }}
              className="flex flex-wrap justify-center gap-4 mt-12 text-sm text-slate-300">
              {[
                { icon: Clock, label: 'תגובה תוך שעה' },
                { icon: Shield, label: 'אחריות 90 יום' },
                { icon: MapPin, label: ADDRESS },
              ].map(({ icon: Icon, label }) => (
                <div key={label} className="flex items-center gap-1.5 glass rounded-full px-3 py-1 backdrop-blur border border-white/20">
                  <Icon className="w-3.5 h-3.5 text-electric" />
                  {label}
                </div>
              ))}
            </motion.div>
          </div>
        </div>
      </HeroVideo>
    </section>
  )
}

// ─── Bento Services ───────────────────────────────────────────────────────────
const BENTO = [
  {
    Icon: SmartphoneRepairIcon, title: 'סמארטפונים',
    desc: 'iPhone, Samsung, Pixel, Xiaomi — מסכים, סוללות, לוחות אם, שחזור FaceID.',
    span: 'md:col-span-2 md:row-span-2',
    brands: ['Apple', 'Samsung', 'Google Pixel', 'Xiaomi'],
    color: 'from-blue-600/20 to-electric/10', border: 'border-blue-500/30',
  },
  {
    Icon: LaptopRepairIcon, title: 'מחשבים ולפטופים',
    desc: 'MacBook, Dell, Lenovo — מיקרו-לחמה, שדרוגים, תיקוני לוח.',
    span: 'md:col-span-1',
    brands: ['Apple', 'Dell', 'Lenovo', 'HP'],
    color: 'from-violet-600/20 to-purple-500/10', border: 'border-violet-500/30',
  },
  {
    Icon: ConsoleRepairIcon, title: 'קונסולות',
    desc: 'PS5, Xbox, Switch — HDMI, ג׳ויסטיק, פן, firmware.',
    span: 'md:col-span-1',
    brands: ['PS5', 'Xbox Series', 'Nintendo'],
    color: 'from-rose-600/20 to-pink-500/10', border: 'border-rose-500/30',
  },
  {
    Icon: MicrosolderingIcon, title: 'מיקרו-לחמה',
    desc: 'BGA, PMIC, IC — תיקון ברמת שבב עם JBC + מיקרוסקופ 40X.',
    span: 'md:col-span-1',
    brands: ['ציוד JBC', 'מיקרוסקופ 40X', 'FLIR Thermal'],
    color: 'from-amber-600/20 to-orange-500/10', border: 'border-amber-500/30',
  },
  {
    Icon: () => <Tablet className="w-10 h-10 text-electric" />, title: 'טאבלטים ולבישים',
    desc: 'iPad Pro/Air/Mini, Apple Watch — מסכים, סוללות, חיבורים.',
    span: 'md:col-span-1',
    brands: ['iPad Pro', 'iPad Air', 'Apple Watch'],
    color: 'from-teal-600/20 to-cyan-500/10', border: 'border-teal-500/30',
  },
]

function BentoCard({ item }) {
  const [mouse, setMouse] = useState({ x: 0, y: 0 })
  const ref = useRef(null)

  const onMove = (e) => {
    const rect = ref.current?.getBoundingClientRect()
    if (!rect) return
    setMouse({ x: e.clientX - rect.left, y: e.clientY - rect.top })
  }

  return (
    <motion.div
      ref={ref}
      variants={fadeUp}
      onMouseMove={onMove}
      whileHover={{ scale: 1.02 }}
      transition={{ duration: 0.2 }}
      className={`group relative rounded-2xl p-6 bg-gradient-to-br ${item.color} border ${item.border} hover:border-electric/60 transition-all duration-300 overflow-hidden flex flex-col gap-4 ${item.span}`}
      style={{ cursor: 'none' }}
    >
      {/* Mouse-tracking glow */}
      <div
        className="absolute w-40 h-40 rounded-full bg-electric/10 blur-2xl pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity"
        style={{ left: mouse.x - 80, top: mouse.y - 80 }}
      />

      <item.Icon className="w-12 h-12 flex-shrink-0 relative z-10" />
      <div className="relative z-10">
        <h3 className="text-xl font-black mb-2">{item.title}</h3>
        <p className="text-slate-400 text-sm leading-relaxed mb-3">{item.desc}</p>
        <div className="flex flex-wrap gap-1.5">
          {item.brands.map((b) => (
            <span key={b} className="text-xs px-2 py-0.5 rounded-full glass text-slate-300 border border-white/10">{b}</span>
          ))}
        </div>
      </div>
    </motion.div>
  )
}

function Services() {
  return (
    <section id="services" className="py-24 px-6">
      <div className="max-w-6xl mx-auto">
        <Reveal>
          <motion.div variants={fadeUp} className="text-center mb-14">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
              השירותים שלנו
            </div>
            <h2 className="text-5xl font-black mb-4 tracking-tight">כל מכשיר. כל תקלה.</h2>
            <p className="text-slate-400 text-lg">ציוד מקצועי שנוסע אליך — לא מעבדה שגרתית.</p>
          </motion.div>
        </Reveal>
        <Reveal className="grid md:grid-cols-3 grid-rows-auto gap-5">
          {BENTO.map((item, i) => (
            <motion.div key={item.title} variants={fadeUp} custom={i} className={item.span}>
              <BentoCard item={item} />
            </motion.div>
          ))}
        </Reveal>
      </div>
    </section>
  )
}

// ─── Tech Gear ────────────────────────────────────────────────────────────────
const GEAR = [
  { Icon: MicrosolderingIcon, title: 'JBC T210-A', subtitle: 'מיקרו-לחמה BGA', desc: 'בקרת טמפרטורה ±1°C לתיקון שבבי BGA ורכיבי SMD.', spec: '±1°C | 0.1mm tip' },
  { Icon: () => <Thermometer className="w-7 h-7 text-electric" />, title: 'FLIR ONE Pro', subtitle: 'הדמיה תרמית', desc: 'סריקה תרמית לאיתור נקודות חום, קצרים ורכיבים פגומים.', spec: '0.07°C | 160×120px' },
  { Icon: () => <Zap className="w-7 h-7 text-electric" />, title: 'Laser Unit', subtitle: 'תיקון לייזר', desc: 'ניקוי ותיקון מסלולי PCB, הסרת זכוכית אחורית ועיבוד עדין.', spec: '0.01mm | 1–5W' },
  { Icon: () => <Cpu className="w-7 h-7 text-electric" />, title: 'DC Power Supply', subtitle: 'ספק כוח', desc: 'מדידת זרם בזמן אמת לאבחון קצרים ורכיבי כוח פגומים.', spec: '1mA accuracy | 0–30V' },
]

function TechGear() {
  return (
    <section className="py-24 px-6 relative overflow-hidden">
      <div className="absolute inset-0 bg-gradient-to-b from-transparent via-electric/4 to-transparent pointer-events-none" />
      <div className="max-w-6xl mx-auto relative z-10">
        <Reveal>
          <motion.div variants={fadeUp} className="text-center mb-14">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">ציוד המעבדה</div>
            <h2 className="text-5xl font-black mb-4 tracking-tight">טכנולוגיה תעשייתית, ישירות אצלך</h2>
            <p className="text-slate-400 text-lg max-w-lg mx-auto">ציוד שלא תמצא בשום מעבדה שגרתית בישראל — נוסע אליך.</p>
          </motion.div>
        </Reveal>
        <Reveal className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
          {GEAR.map((item, i) => (
            <motion.div key={item.title} variants={fadeUp} custom={i}
              whileHover={{ scale: 1.04, transition: { duration: 0.2 } }}
              className="group relative noise rounded-2xl overflow-hidden">
              <div className="h-full p-6 glass rounded-2xl border border-white/10 hover:border-electric/50 hover:shadow-xl hover:shadow-electric/15 transition-all flex flex-col gap-4">
                <div className="relative w-14 h-14">
                  <div className="absolute inset-0 rounded-xl bg-electric/20 blur-md group-hover:bg-electric/35 transition-all" />
                  <div className="relative w-14 h-14 rounded-xl bg-electric/20 border border-electric/30 flex items-center justify-center">
                    <item.Icon />
                  </div>
                </div>
                <div>
                  <p className="text-xs text-electric font-semibold mb-0.5">{item.subtitle}</p>
                  <h4 className="text-base font-bold mb-1">{item.title}</h4>
                  <p className="text-slate-400 text-sm leading-relaxed">{item.desc}</p>
                </div>
                <div className="mt-auto pt-3 border-t border-white/10">
                  <p className="text-xs text-slate-500 font-mono">{item.spec}</p>
                </div>
              </div>
            </motion.div>
          ))}
        </Reveal>
      </div>
    </section>
  )
}

// ─── Why Us ───────────────────────────────────────────────────────────────────
const WHY_US = [
  'תיקון ב-90% מהמקרים ביום הפנייה',
  'שקיפות מלאה — מחיר ידוע לפני התיקון',
  'אחריות 90 יום על כל עבודה',
  'רכיבי חילוף מקוריים בלבד',
  'טכנאים מוסמכים עם ניסיון 5+ שנים',
  'כל הציוד בשטח — ללא צורך למסור מכשיר',
]

function WhyUs() {
  return (
    <section className="py-20 px-6">
      <div className="max-w-4xl mx-auto">
        <Reveal>
          <motion.div variants={fadeUp} className="text-center mb-12">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">למה אנחנו?</div>
            <h2 className="text-5xl font-black mb-4 tracking-tight">לא סתם עוד מעבדה</h2>
          </motion.div>
          <div className="grid sm:grid-cols-2 gap-4">
            {WHY_US.map((item, i) => (
              <motion.div key={item} variants={fadeUp} custom={i}
                className="flex items-start gap-3 glass rounded-xl p-4 border border-white/10 hover:border-electric/30 transition-all">
                <CheckCircle2 className="w-5 h-5 text-electric flex-shrink-0 mt-0.5" />
                <span className="text-slate-200 text-sm font-medium">{item}</span>
              </motion.div>
            ))}
          </div>
        </Reveal>
      </div>
    </section>
  )
}

// ─── Reviews ──────────────────────────────────────────────────────────────────
const REVIEWS = [
  { name: 'מיכל ל.', text: 'הטכנאי הגיע תוך שעתיים, החליף את המסך ב-iPhone שלי והכל עבד מושלם.', stars: 5 },
  { name: 'דוד כ.', text: 'הלפטופ שלי נשרף פנימית. עם FLIR מצאו בדיוק מה שבור — חסכו לי קניית מחשב חדש.', stars: 5 },
  { name: 'יוסי מ.', text: 'PS5 עם בעיית HDMI — טיפלו בזה בבית תוך 45 דקות. שירות מדהים.', stars: 5 },
]

function Reviews() {
  return (
    <section className="py-20 px-6">
      <div className="max-w-6xl mx-auto">
        <Reveal>
          <motion.div variants={fadeUp} className="text-center mb-12">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">ביקורות</div>
            <h2 className="text-5xl font-black tracking-tight">מה אומרים עלינו</h2>
          </motion.div>
          <div className="grid md:grid-cols-3 gap-6">
            {REVIEWS.map((r, i) => (
              <motion.div key={r.name} variants={fadeUp} custom={i}
                whileHover={{ y: -4 }}
                className="glass rounded-2xl p-6 border border-white/10 hover:border-electric/30 transition-all flex flex-col gap-4">
                <div className="flex gap-1">
                  {Array.from({ length: r.stars }).map((_, j) => (
                    <Star key={j} className="w-4 h-4 text-yellow-400" fill="currentColor" />
                  ))}
                </div>
                <p className="text-slate-300 text-sm leading-relaxed flex-1">"{r.text}"</p>
                <span className="text-slate-500 text-sm font-semibold">{r.name}</span>
              </motion.div>
            ))}
          </div>
        </Reveal>
      </div>
    </section>
  )
}

// ─── Pricing Table (for pricing tab) ─────────────────────────────────────────
const PRICING_DATA = [
  // Phones
  { category: 'סמארטפונים', device: 'iPhone 15 Pro Max', repair: 'החלפת מסך OLED',    from: 500, to: 620 },
  { category: 'סמארטפונים', device: 'iPhone 14 Pro',     repair: 'החלפת מסך',          from: 420, to: 520 },
  { category: 'סמארטפונים', device: 'iPhone 14/15',       repair: 'החלפת סוללה',        from: 180, to: 250 },
  { category: 'סמארטפונים', device: 'iPhone כל דגם',      repair: 'פורט טעינה',         from: 180, to: 260 },
  { category: 'סמארטפונים', device: 'Samsung S24 Ultra',  repair: 'החלפת מסך',          from: 350, to: 480 },
  { category: 'סמארטפונים', device: 'Samsung כל דגם',     repair: 'סוללה',              from: 150, to: 220 },
  { category: 'סמארטפונים', device: 'כל דגם',             repair: 'תיקון לוח אם (BGA)', from: 450, to: 800 },
  { category: 'סמארטפונים', device: 'כל דגם',             repair: 'שחזור נוזלים',       from: 350, to: 650 },
  // Laptops
  { category: 'מחשבים',     device: 'MacBook Air M2/M3',  repair: 'החלפת מסך',          from: 550, to: 750 },
  { category: 'מחשבים',     device: 'MacBook Pro M2/M3',  repair: 'מיקרו-לחמה לוח',    from: 600, to: 1200 },
  { category: 'מחשבים',     device: 'MacBook כל דגם',     repair: 'החלפת סוללה',        from: 350, to: 500 },
  { category: 'מחשבים',     device: 'Dell / Lenovo / HP', repair: 'שדרוג SSD',           from: 180, to: 320 },
  { category: 'מחשבים',     device: 'Dell / Lenovo / HP', repair: 'תיקון לוח אם',        from: 450, to: 900 },
  { category: 'מחשבים',     device: 'כל דגם',             repair: 'תרמופסטה Liquid Metal', from: 200, to: 380 },
  // Consoles
  { category: 'קונסולות',   device: 'PS5',                repair: 'תיקון HDMI',          from: 280, to: 380 },
  { category: 'קונסולות',   device: 'PS5',                repair: 'תיקון ספק כוח',       from: 300, to: 450 },
  { category: 'קונסולות',   device: 'Xbox Series X/S',    repair: 'תיקון HDMI',          from: 250, to: 350 },
  { category: 'קונסולות',   device: 'Nintendo Switch',    repair: 'תיקון ג׳ויסטיק',     from: 180, to: 280 },
]

const CAT_FILTERS = ['הכל', 'סמארטפונים', 'מחשבים', 'קונסולות']

function PricingTab() {
  const [q, setQ] = useState('')
  const [cat, setCat] = useState('הכל')

  const filtered = PRICING_DATA.filter((row) => {
    const matchCat = cat === 'הכל' || row.category === cat
    const matchQ = !q || row.device.includes(q) || row.repair.includes(q)
    return matchCat && matchQ
  })

  return (
    <div className="py-16 px-6 max-w-5xl mx-auto">
      <div className="text-center mb-12">
        <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">מחירון</div>
        <h2 className="text-5xl font-black mb-4 tracking-tight">
          מחירים <span className="text-gradient">שקופים</span>
        </h2>
        <p className="text-slate-400 text-lg mb-8">מחיר מוסכם לפני כל תיקון. ללא הפתעות.</p>

        {/* Controls */}
        <div className="flex flex-col sm:flex-row gap-3 max-w-lg mx-auto">
          <div className="relative flex-1">
            <Search className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <input value={q} onChange={(e) => setQ(e.target.value)}
              placeholder="חפש מכשיר או תיקון..."
              className="w-full pr-10 pl-8 py-2.5 rounded-xl glass border border-white/10 focus:border-electric/50 bg-transparent text-white placeholder-slate-500 text-sm focus:outline-none transition-all" />
            {q && <button onClick={() => setQ('')} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400"><X className="w-3.5 h-3.5" /></button>}
          </div>
          <div className="flex gap-1.5">
            {CAT_FILTERS.map((c) => (
              <button key={c} onClick={() => setCat(c)}
                className={`px-3 py-2 rounded-xl text-xs font-bold border transition-all ${cat === c ? 'bg-electric text-white border-electric' : 'glass border-white/10 text-slate-400 hover:border-electric/40'}`}>
                {c}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="glass rounded-2xl border border-white/10 overflow-hidden mb-10">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/10 text-slate-400 text-xs uppercase tracking-wider">
                <th className="text-right p-4">קטגוריה</th>
                <th className="text-right p-4">מכשיר</th>
                <th className="text-right p-4">תיקון</th>
                <th className="text-right p-4">מחיר</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((row, i) => (
                <motion.tr key={i} initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: i * 0.02 }}
                  className="border-b border-white/5 hover:bg-white/3 transition-colors">
                  <td className="p-4"><span className="glass-blue border border-electric/20 text-electric text-xs px-2 py-0.5 rounded-full">{row.category}</span></td>
                  <td className="p-4 font-semibold text-slate-200">{row.device}</td>
                  <td className="p-4 text-slate-400">{row.repair}</td>
                  <td className="p-4 font-black text-gradient">₪{row.from}–{row.to}</td>
                </motion.tr>
              ))}
              {filtered.length === 0 && (
                <tr><td colSpan={4} className="py-12 text-center text-slate-500">לא נמצאו תוצאות</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
      <p className="text-xs text-slate-500 text-center mb-12">* מחירים הם הערכה. המחיר הסופי נקבע לאחר אבחון. כולל חלקים + עבודה.</p>
      <PriceEstimator />
    </div>
  )
}

// ─── Footer ───────────────────────────────────────────────────────────────────
function Footer({ setTab }) {
  return (
    <footer className="py-12 px-6 border-t border-white/10 mb-16 sm:mb-0">
      <div className="max-w-6xl mx-auto">
        <div className="grid sm:grid-cols-3 gap-8 mb-8">
          {/* Brand */}
          <div>
            <div className="flex items-center gap-2 font-black text-white mb-3">
              <Zap className="w-5 h-5 text-electric" fill="currentColor" />
              Fix Express Labs
            </div>
            <p className="text-slate-500 text-sm leading-relaxed">מעבדה ניידת מתקדמת — ציוד תעשייתי שמגיע אליך.</p>
          </div>

          {/* Address */}
          <div>
            <p className="text-slate-300 font-semibold mb-3 text-sm">מיקום</p>
            <a href={MAPS_URL} target="_blank" rel="noopener noreferrer"
              className="flex items-start gap-2 text-slate-400 hover:text-electric transition-colors text-sm">
              <MapPin className="w-4 h-4 mt-0.5 flex-shrink-0 text-electric" />
              <span>{ADDRESS}<br /><span className="text-xs text-slate-500">מרכז מעבדה קבוע — תיקוני לוח אם מורכבים</span></span>
            </a>
          </div>

          {/* Contact */}
          <div>
            <p className="text-slate-300 font-semibold mb-3 text-sm">יצירת קשר</p>
            <div className="flex flex-col gap-2">
              <a href={WA_URL} target="_blank" rel="noopener noreferrer"
                className="flex items-center gap-2 text-slate-400 hover:text-electric transition-colors text-sm">
                <MessageCircle className="w-4 h-4 text-[#25D366]" />
                {WA_DISPLAY} (WhatsApp)
              </a>
              <a href={`tel:+${WA_NUMBER}`}
                className="flex items-center gap-2 text-slate-400 hover:text-electric transition-colors text-sm">
                <Phone className="w-4 h-4 text-electric" />
                {WA_DISPLAY}
              </a>
            </div>
          </div>
        </div>

        <div className="flex flex-col sm:flex-row items-center justify-between gap-3 pt-6 border-t border-white/10 text-xs text-slate-600">
          <span>© {new Date().getFullYear()} Fix Express Labs | ירין הלילי</span>
          <div className="flex gap-4">
            {TABS.map((t) => (
              <button key={t.id} onClick={() => setTab(t.id)} className="hover:text-electric transition-colors">{t.label}</button>
            ))}
          </div>
        </div>
      </div>
    </footer>
  )
}

// ─── Floating CTA ─────────────────────────────────────────────────────────────
function FloatingCTA() {
  return (
    <>
      <motion.a href={WA_URL} target="_blank" rel="noopener noreferrer"
        initial={{ scale: 0 }} animate={{ scale: 1 }}
        transition={{ delay: 1.5, type: 'spring', stiffness: 200 }}
        whileHover={{ scale: 1.1 }}
        className="fixed bottom-24 left-6 sm:bottom-8 z-50 w-14 h-14 rounded-full bg-[#25D366] flex items-center justify-center shadow-2xl shadow-green-900/50 animate-float">
        <MessageCircle className="w-7 h-7 text-white" fill="white" />
      </motion.a>
      {/* Mobile bar */}
      <motion.div initial={{ y: 80 }} animate={{ y: 0 }} transition={{ delay: 1 }}
        className="fixed bottom-0 inset-x-0 z-40 sm:hidden glass border-t border-white/15 px-4 py-3 flex gap-3">
        <a href={`tel:+${WA_NUMBER}`}
          className="flex-1 flex items-center justify-center gap-2 py-3 rounded-xl glass border border-white/15 text-white font-semibold text-sm">
          <Phone className="w-4 h-4 text-electric" />{WA_DISPLAY}
        </a>
        <a href={WA_URL} target="_blank" rel="noopener noreferrer"
          className="flex-1 flex items-center justify-center gap-2 py-3 rounded-xl bg-[#25D366] text-white font-bold text-sm">
          <MessageCircle className="w-4 h-4" fill="white" />WhatsApp
        </a>
      </motion.div>
    </>
  )
}

// ─── Home tab ─────────────────────────────────────────────────────────────────
function HomeTab({ setTab }) {
  return (
    <>
      <Hero setTab={setTab} />
      <Services />
      <TechGear />
      <WhyUs />
      <Reviews />
      <SocialHub />
    </>
  )
}

// ─── App ──────────────────────────────────────────────────────────────────────
export default function App() {
  if (isAdmin) return <AdminDashboard />

  const [activeTab, setActiveTab] = useState('home')
  const [prevTab, setPrevTab] = useState('home')

  const tabIdx   = (id) => TABS.findIndex((t) => t.id === id)
  const direction = tabIdx(activeTab) >= tabIdx(prevTab) ? 1 : -1

  const setTab = (id) => {
    if (id === activeTab) return
    setPrevTab(activeTab)
    setActiveTab(id)
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  const renderTab = () => {
    switch (activeTab) {
      case 'home':    return <HomeTab setTab={setTab} />
      case 'about':   return <About />
      case 'pricing': return <PricingTab />
      case 'faq':     return <FAQ />
      case 'store':   return <Store />
      default:        return <HomeTab setTab={setTab} />
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-white overflow-x-hidden" dir="rtl" style={{ cursor: 'none' }}>
      <CustomCursor />
      <Navbar activeTab={activeTab} setTab={setTab} />

      <AnimatePresence mode="wait" custom={direction}>
        <motion.main
          key={activeTab}
          custom={direction}
          variants={tabVariants}
          initial="enter"
          animate="center"
          exit="exit"
          className={activeTab === 'home' ? '' : 'pt-16'}
        >
          {renderTab()}
        </motion.main>
      </AnimatePresence>

      <Footer setTab={setTab} />
      <FloatingCTA />
    </div>
  )
}
