import { useState, useRef, useEffect } from 'react'
import {
  motion, AnimatePresence, useInView, useScroll, useTransform,
} from 'framer-motion'
import {
  Zap, MessageCircle, Clock, Shield, MapPin, CheckCircle2, Star,
  ChevronDown, ChevronLeft, Thermometer, Cpu, Wrench, Search,
  Phone, Smartphone, Laptop, Gamepad2, Tablet, X,
} from 'lucide-react'
import HeroVideo from './components/HeroVideo'
import {
  SmartphoneRepairIcon, LaptopRepairIcon,
  ConsoleRepairIcon, MicrosolderingIcon,
} from './components/Icons'
import AdminDashboard from './pages/AdminDashboard'

// ─── Constants ────────────────────────────────────────────────────────────────
const WA_NUMBER = '972504951109'
const WA_DISPLAY = '050-495-1109'
const WA_MSG = encodeURIComponent('היי! אני מעוניין בתיקון מהיר עד הבית 🔧')
const WA_URL = `https://wa.me/${WA_NUMBER}?text=${WA_MSG}`

const isAdmin = typeof window !== 'undefined' && window.location.pathname === '/admin'

// ─── Animation helpers ────────────────────────────────────────────────────────
const fadeUp = {
  hidden: { opacity: 0, y: 30 },
  visible: (i = 0) => ({
    opacity: 1, y: 0,
    transition: { duration: 0.55, delay: i * 0.1, ease: [0.22, 1, 0.36, 1] },
  }),
}
const stagger = { visible: { transition: { staggerChildren: 0.08 } } }
const cardHover = {
  rest: { scale: 1, rotateX: 0, rotateY: 0 },
  hover: { scale: 1.03, transition: { duration: 0.25, ease: 'easeOut' } },
}

function FadeSection({ children, className = '' }) {
  const ref = useRef(null)
  const inView = useInView(ref, { once: true, margin: '-80px' })
  return (
    <motion.div
      ref={ref}
      variants={stagger}
      initial="hidden"
      animate={inView ? 'visible' : 'hidden'}
      className={className}
    >
      {children}
    </motion.div>
  )
}

// ─── Services Data ────────────────────────────────────────────────────────────
const SERVICES = [
  {
    id: 'phones',
    Icon: SmartphoneRepairIcon,
    LucideIcon: Smartphone,
    title: 'סמארטפונים',
    desc: 'תיקון מקצועי לכל דגמי האייפון, סמסונג, פיקסל ועוד.',
    color: 'from-blue-500/20 to-electric/10',
    border: 'border-blue-500/30',
    hoverGlow: 'hover:shadow-blue-500/20',
    brands: [
      {
        name: 'Apple iPhone',
        models: ['iPhone 11', 'iPhone 12', 'iPhone 13', 'iPhone 14', 'iPhone 14 Pro/Max', 'iPhone 15', 'iPhone 15 Pro/Max'],
      },
      {
        name: 'Samsung',
        models: ['S21 / S22 / S23 / S24 Ultra', 'Fold 3/4/5/6', 'Flip 3/4/5/6', 'A-Series (A54, A34, A14)'],
      },
      { name: 'Google Pixel', models: ['Pixel 6', 'Pixel 7', 'Pixel 8', 'Pixel 8 Pro'] },
      { name: 'Xiaomi / OnePlus', models: ['Xiaomi 13/14', 'OnePlus 11/12'] },
    ],
    repairs: [
      'החלפת מסך OLED / LCD',
      'החלפת סוללה',
      'תיקון פורט טעינה',
      'תיקון IC לוח אם (מיקרו-לחמה)',
      'שחזור FaceID / TouchID',
      'הסרת זכוכית אחורית בלייזר',
      'שחזור נזקי נוזלים',
    ],
  },
  {
    id: 'laptops',
    Icon: LaptopRepairIcon,
    LucideIcon: Laptop,
    title: 'מחשבים ולפטופים',
    desc: 'מיקרו-לחמה, שדרוגים ותיקוני לוח אם לכל הדגמים.',
    color: 'from-violet-500/20 to-purple-500/10',
    border: 'border-violet-500/30',
    hoverGlow: 'hover:shadow-violet-500/20',
    brands: [
      { name: 'MacBook', models: ['MacBook Air M1/M2/M3', 'MacBook Pro 14"/16" M1/M2/M3', 'MacBook Pro Intel'] },
      { name: 'Dell', models: ['XPS 13/15/17', 'Latitude Series', 'Inspiron Series'] },
      { name: 'Lenovo', models: ['ThinkPad X1 / T Series', 'Yoga Series', 'IdeaPad'] },
      { name: 'HP / ASUS', models: ['HP Spectre / Pavilion', 'ASUS ROG / Zenbook'] },
    ],
    repairs: [
      'מיקרו-לחמה לוח לוגי',
      'שדרוג SSD / RAM',
      'החלפת תרמופסטה (Liquid Metal)',
      'החלפת מסך',
      'החלפת מקלדת',
      'שחזור מערכת הפעלה',
      'תיקון פורט USB-C / MagSafe',
    ],
  },
  {
    id: 'consoles',
    Icon: ConsoleRepairIcon,
    LucideIcon: Gamepad2,
    title: 'קונסולות גיימינג',
    desc: 'תיקון HDMI, ג׳ויסטיק, פן וכל בעיה אחרת לPS5, Xbox ו-Switch.',
    color: 'from-rose-500/20 to-pink-500/10',
    border: 'border-rose-500/30',
    hoverGlow: 'hover:shadow-rose-500/20',
    brands: [
      { name: 'PlayStation', models: ['PS5 Disc / Digital', 'PS4 / PS4 Pro', 'PS4 Slim'] },
      { name: 'Xbox', models: ['Xbox Series X / S', 'Xbox One X / S'] },
      { name: 'Nintendo', models: ['Switch OLED', 'Switch Lite', 'Switch V1/V2'] },
    ],
    repairs: [
      'החלפת פורט HDMI',
      'תיקון Drift ג׳ויסטיק (חיישני Hall Effect)',
      'תיקון פן והתחממות יתר',
      'תיקון ספק כוח',
      'תיקון כונן דיסקים',
      'עדכון / שחזור Firmware',
    ],
  },
  {
    id: 'tablets',
    Icon: () => <Tablet className="w-12 h-12 text-electric" />,
    LucideIcon: Tablet,
    title: 'טאבלטים ולבישים',
    desc: 'iPad, Apple Watch ועוד — תיקון מסכים, סוללות ולוחות.',
    color: 'from-teal-500/20 to-cyan-500/10',
    border: 'border-teal-500/30',
    hoverGlow: 'hover:shadow-teal-500/20',
    brands: [
      { name: 'iPad', models: ['iPad Pro 11" / 12.9"', 'iPad Air (4th/5th Gen)', 'iPad Mini (5th/6th Gen)', 'iPad (9th/10th Gen)'] },
      { name: 'Apple Watch', models: ['Series 6 / 7 / 8 / 9', 'Ultra / Ultra 2', 'SE (1st/2nd Gen)'] },
    ],
    repairs: [
      'החלפת מסך LCD / OLED',
      'החלפת סוללה',
      'תיקון פורט טעינה / Smart Connector',
      'תיקון זכוכית',
      'תיקון Apple Pencil connector',
    ],
  },
]

// ─── Gear Data ────────────────────────────────────────────────────────────────
const GEAR = [
  {
    Icon: MicrosolderingIcon,
    title: 'JBC Soldering Station',
    subtitle: 'מיקרו-לחמה BGA',
    desc: 'תחנת לחמה JBC T210-A עם בקרת טמפרטורה ±1°C. מאפשרת תיקון רכיבים זעירים ברמת שבב שאף מעבדה שגרתית לא נוגעת בה.',
    spec: 'דיוק: ±1°C | נקודת לחמה: 0.1mm',
  },
  {
    Icon: () => <Thermometer className="w-7 h-7 text-electric" />,
    title: 'FLIR ONE Pro',
    subtitle: 'הדמיה תרמית',
    desc: 'מצלמת FLIR ONE Pro עם רזולוציה תרמית 160×120px. מאתרת נקודות חום, קצרים ורכיבים פגומים ללא פירוק מלא.',
    spec: 'רגישות: 0.07°C | טווח: -20°C עד 400°C',
  },
  {
    Icon: () => <Zap className="w-7 h-7 text-electric" />,
    title: 'Laser Repair Unit',
    subtitle: 'תיקון לייזר',
    desc: 'יחידת לייזר לניקוי ותיקון מסלולי PCB, הסרת זכוכית אחורית ועיבוד עדין של רכיבים ללא מגע פיזי.',
    spec: 'דיוק: 0.01mm | עוצמה: 1–5W',
  },
  {
    Icon: () => <Cpu className="w-7 h-7 text-electric" />,
    title: 'DC Power Supply',
    subtitle: 'ספק כוח מדויק',
    desc: 'ספק כוח DCPS עם מדידת זרם בזמן אמת לאבחון קצרים, בעיות PMIC ורכיבי כוח — ללא צורך בהפעלת הסוללה.',
    spec: 'דיוק: 1mA | טווח: 0–30V',
  },
]

// ─── Navbar ───────────────────────────────────────────────────────────────────
function Navbar() {
  const [scrolled, setScrolled] = useState(false)
  useEffect(() => {
    const fn = () => setScrolled(window.scrollY > 20)
    window.addEventListener('scroll', fn)
    return () => window.removeEventListener('scroll', fn)
  }, [])

  return (
    <header className={`fixed top-0 inset-x-0 z-50 transition-all duration-300 ${scrolled ? 'glass border-b border-white/10' : ''}`}>
      <div className="max-w-6xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
        <motion.div
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.5 }}
          className="flex items-center gap-3"
        >
          <div className="relative w-9 h-9 flex items-center justify-center rounded-lg bg-electric glow-blue">
            <Zap className="w-5 h-5 text-white" fill="white" />
          </div>
          <span className="font-bold text-lg tracking-tight">
            Fix <span className="text-gradient">Express</span> Labs
          </span>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="flex items-center gap-3"
        >
          <a href="/admin" className="text-slate-400 hover:text-white text-sm transition-colors hidden sm:block">
            ניהול
          </a>
          <a
            href={WA_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-4 py-2 rounded-full bg-[#25D366] hover:bg-[#1fb957] transition-all text-white font-semibold text-sm shadow-lg shadow-green-900/30"
          >
            <MessageCircle className="w-4 h-4" />
            <span className="hidden sm:inline">WhatsApp</span>
            <span className="sm:hidden">{WA_DISPLAY}</span>
          </a>
        </motion.div>
      </div>
    </header>
  )
}

// ─── Hero ─────────────────────────────────────────────────────────────────────
function Hero() {
  const ref = useRef(null)
  const { scrollYProgress } = useScroll({ target: ref, offset: ['start start', 'end start'] })
  const gridY = useTransform(scrollYProgress, [0, 1], ['0%', '30%'])

  return (
    <section ref={ref} className="relative min-h-screen">
      <HeroVideo intervalMs={8000} fadeDurationMs={1400}>
        {/* Parallax grid overlay */}
        <motion.div
          className="absolute inset-0 pointer-events-none"
          style={{
            y: gridY,
            backgroundImage:
              'linear-gradient(rgba(0,112,243,0.12) 1px, transparent 1px), linear-gradient(90deg, rgba(0,112,243,0.12) 1px, transparent 1px)',
            backgroundSize: '60px 60px',
            zIndex: 4,
          }}
        />

        <div className="relative min-h-screen flex items-center justify-center pt-16 px-6" style={{ zIndex: 5 }}>
          <div className="max-w-4xl mx-auto text-center">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: 0.2 }}
              className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-6 text-sm text-electric-light font-medium"
            >
              <span className="w-2 h-2 rounded-full bg-electric animate-pulse" />
              מעבדה ניידת | שירות עד הבית | {WA_DISPLAY}
            </motion.div>

            <motion.h1
              initial={{ opacity: 0, y: 30 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.7, delay: 0.35 }}
              className="text-5xl sm:text-6xl md:text-7xl font-black leading-tight mb-5 tracking-tight drop-shadow-2xl"
            >
              המעבדה המתקדמת
              <br />
              <span className="text-gradient">בישראל — אצלך בחנייה</span>
            </motion.h1>

            <motion.p
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: 0.5 }}
              className="text-lg sm:text-xl text-slate-200 max-w-2xl mx-auto mb-10 leading-relaxed drop-shadow-lg"
            >
              תיקון סמארטפונים, מחשבים וקונסולות עם ציוד קצה טכנולוגי —
              ישירות אליך, ללא צורך לנסוע.
            </motion.p>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: 0.65 }}
              className="flex flex-col sm:flex-row gap-4 justify-center items-center"
            >
              <a
                href={WA_URL}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2 px-8 py-4 rounded-full bg-electric hover:bg-electric-light transition-all text-white font-bold text-lg glow-blue shadow-2xl shadow-electric/40"
              >
                <MessageCircle className="w-5 h-5" />
                קבע תור עכשיו
              </a>
              <a
                href="#services"
                className="flex items-center gap-2 px-8 py-4 rounded-full glass border border-white/30 hover:border-electric/60 transition-all text-white font-semibold text-lg backdrop-blur"
              >
                השירותים שלנו
                <ChevronLeft className="w-5 h-5" />
              </a>
            </motion.div>

            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.7, delay: 0.85 }}
              className="flex flex-wrap justify-center gap-4 mt-12 text-sm text-slate-300"
            >
              {[
                { icon: Clock, label: 'תגובה תוך שעה' },
                { icon: Shield, label: 'אחריות 90 יום' },
                { icon: MapPin, label: 'עד הבית / עבודה' },
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

// ─── Service Card with expandable accordion ───────────────────────────────────
function ServiceCard({ service, searchQuery }) {
  const [open, setOpen] = useState(false)
  const [openBrand, setOpenBrand] = useState(null)

  // Filter by search
  const matches =
    !searchQuery ||
    service.title.includes(searchQuery) ||
    service.desc.includes(searchQuery) ||
    service.repairs.some((r) => r.includes(searchQuery)) ||
    service.brands.some(
      (b) =>
        b.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        b.models.some((m) => m.toLowerCase().includes(searchQuery.toLowerCase()))
    )

  if (!matches) return null

  return (
    <motion.div
      variants={fadeUp}
      whileHover="hover"
      initial="rest"
      animate="rest"
      className={`group rounded-2xl border ${service.border} hover:border-electric/60 transition-all duration-300 hover:shadow-2xl ${service.hoverGlow} overflow-hidden`}
      style={{ background: 'linear-gradient(135deg, rgba(255,255,255,0.04) 0%, rgba(0,112,243,0.06) 100%)' }}
    >
      {/* Card header */}
      <button
        className="w-full p-6 text-right flex items-start gap-4"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        <div className="flex-1 text-right">
          <div className="mb-3">
            <service.Icon className="w-12 h-12" />
          </div>
          <h3 className="text-xl font-bold mb-1">{service.title}</h3>
          <p className="text-slate-400 text-sm">{service.desc}</p>
          <div className="flex flex-wrap gap-1.5 mt-3">
            {service.brands.map((b) => (
              <span key={b.name} className="text-xs px-2 py-0.5 rounded-full glass text-slate-300 border border-white/10">
                {b.name}
              </span>
            ))}
          </div>
        </div>
        <motion.div animate={{ rotate: open ? 180 : 0 }} transition={{ duration: 0.25 }}>
          <ChevronDown className="w-5 h-5 text-slate-400 flex-shrink-0 mt-1" />
        </motion.div>
      </button>

      {/* Expandable body */}
      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            key="body"
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="px-6 pb-6 border-t border-white/10 pt-4 space-y-5">
              {/* Repairs list */}
              <div>
                <p className="text-xs text-slate-500 uppercase tracking-wider mb-2 font-semibold">תיקונים</p>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-1.5">
                  {service.repairs.map((r) => (
                    <div key={r} className="flex items-center gap-2 text-sm text-slate-300">
                      <CheckCircle2 className="w-3.5 h-3.5 text-electric flex-shrink-0" />
                      {r}
                    </div>
                  ))}
                </div>
              </div>

              {/* Models accordion */}
              <div>
                <p className="text-xs text-slate-500 uppercase tracking-wider mb-2 font-semibold">דגמים נתמכים</p>
                <div className="space-y-1.5">
                  {service.brands.map((brand) => (
                    <div key={brand.name}>
                      <button
                        className="w-full text-right flex items-center justify-between px-3 py-2 rounded-lg glass-blue border border-electric/20 text-sm font-semibold text-electric-light hover:border-electric/50 transition-all"
                        onClick={() => setOpenBrand(openBrand === brand.name ? null : brand.name)}
                      >
                        {brand.name}
                        <motion.div animate={{ rotate: openBrand === brand.name ? 180 : 0 }} transition={{ duration: 0.2 }}>
                          <ChevronDown className="w-4 h-4" />
                        </motion.div>
                      </button>
                      <AnimatePresence initial={false}>
                        {openBrand === brand.name && (
                          <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: 'auto', opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            transition={{ duration: 0.25, ease: 'easeOut' }}
                            className="overflow-hidden"
                          >
                            <div className="px-3 pt-2 pb-1 flex flex-wrap gap-1.5">
                              {brand.models.map((m) => (
                                <span key={m} className="text-xs px-2.5 py-1 rounded-full bg-white/5 border border-white/10 text-slate-300">
                                  {m}
                                </span>
                              ))}
                            </div>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </div>
                  ))}
                </div>
              </div>

              {/* CTA */}
              <a
                href={WA_URL}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-center gap-2 w-full py-3 rounded-xl bg-electric hover:bg-electric-light transition-all text-white font-semibold text-sm glow-blue"
              >
                <MessageCircle className="w-4 h-4" />
                קבע תיקון עכשיו
              </a>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  )
}

// ─── Services Section ─────────────────────────────────────────────────────────
function Services() {
  const [query, setQuery] = useState('')

  return (
    <section id="services" className="py-24 px-6">
      <div className="max-w-6xl mx-auto">
        <FadeSection>
          <motion.div variants={fadeUp} className="text-center mb-10">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
              השירותים שלנו
            </div>
            <h2 className="text-4xl sm:text-5xl font-black mb-4 tracking-tight">תיקון לכל מכשיר</h2>
            <p className="text-slate-400 max-w-xl mx-auto text-lg mb-8">
              Apple, Samsung, Sony, Microsoft — הכל בציוד מקצועי, עד הבית.
            </p>

            {/* Search bar */}
            <div className="relative max-w-md mx-auto">
              <Search className="absolute right-4 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="חפש מכשיר, דגם או תיקון..."
                className="w-full pr-11 pl-10 py-3 rounded-full glass border border-white/15 focus:border-electric/50 bg-transparent text-white placeholder-slate-500 text-sm focus:outline-none transition-all"
              />
              {query && (
                <button
                  onClick={() => setQuery('')}
                  className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 hover:text-white"
                >
                  <X className="w-4 h-4" />
                </button>
              )}
            </div>
          </motion.div>
        </FadeSection>

        <FadeSection className="grid sm:grid-cols-2 lg:grid-cols-2 xl:grid-cols-4 gap-5 mt-4">
          {SERVICES.map((s, i) => (
            <motion.div key={s.id} variants={fadeUp} custom={i}>
              <ServiceCard service={s} searchQuery={query} />
            </motion.div>
          ))}
        </FadeSection>
      </div>
    </section>
  )
}

// ─── Tech Gear ────────────────────────────────────────────────────────────────
function TechGear() {
  return (
    <section id="gear" className="py-24 px-6 relative overflow-hidden">
      <div className="absolute inset-0 bg-gradient-to-b from-transparent via-electric/5 to-transparent pointer-events-none" />

      <div className="max-w-6xl mx-auto relative z-10">
        <FadeSection>
          <motion.div variants={fadeUp} className="text-center mb-14">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
              ציוד המעבדה
            </div>
            <h2 className="text-4xl sm:text-5xl font-black mb-4 tracking-tight">טכנולוגיה מתקדמת, ישירות אצלך</h2>
            <p className="text-slate-400 max-w-xl mx-auto text-lg">ציוד ברמה תעשייתית שנוסע אליך — לא מעבדה שגרתית.</p>
          </motion.div>
        </FadeSection>

        <FadeSection className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
          {GEAR.map((item, i) => (
            <motion.div
              key={item.title}
              variants={fadeUp}
              custom={i}
              whileHover={{ scale: 1.04, transition: { duration: 0.2 } }}
              className="group relative noise rounded-2xl overflow-hidden cursor-default"
            >
              <div className="h-full p-6 glass rounded-2xl border border-white/10 hover:border-electric/50 transition-all duration-300 hover:shadow-xl hover:shadow-electric/15 flex flex-col gap-4">
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
        </FadeSection>

        <FadeSection className="mt-14 flex justify-center">
          <motion.div variants={fadeUp} className="glass-blue rounded-2xl p-8 max-w-2xl w-full text-center border border-electric/30">
            <h3 className="text-2xl font-bold mb-2">
              מוכן לתיקון? <span className="text-gradient">אנחנו בדרך.</span>
            </h3>
            <p className="text-slate-400 mb-6">שלח הודעה וניצור קשר תוך שעה לתיאום הגעה.</p>
            <a
              href={WA_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 px-8 py-3.5 rounded-full bg-electric hover:bg-electric-light transition-all font-bold text-white glow-blue"
            >
              <MessageCircle className="w-5 h-5" />
              שלח הודעה ב-WhatsApp
            </a>
          </motion.div>
        </FadeSection>
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
        <FadeSection>
          <motion.div variants={fadeUp} className="text-center mb-12">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
              למה Fix Express Labs?
            </div>
            <h2 className="text-4xl sm:text-5xl font-black mb-4 tracking-tight">לא סתם עוד מעבדה</h2>
            <p className="text-slate-400 max-w-xl mx-auto text-lg">ניסיון אמיתי, ציוד רציני ושקיפות מלאה — אצלך.</p>
          </motion.div>
          <div className="grid sm:grid-cols-2 gap-4">
            {WHY_US.map((item, i) => (
              <motion.div
                key={item}
                variants={fadeUp}
                custom={i}
                className="flex items-start gap-3 glass rounded-xl p-4 border border-white/10 hover:border-electric/30 transition-all"
              >
                <CheckCircle2 className="w-5 h-5 text-electric flex-shrink-0 mt-0.5" />
                <span className="text-slate-200 text-sm font-medium">{item}</span>
              </motion.div>
            ))}
          </div>
        </FadeSection>
      </div>
    </section>
  )
}

// ─── Reviews ──────────────────────────────────────────────────────────────────
const REVIEWS = [
  { name: 'מיכל ל.', text: 'הטכנאי הגיע תוך שעתיים, החליף את המסך ב-iPhone שלי והכל עבד מושלם. ממליצה בחום!', stars: 5 },
  { name: 'דוד כ.', text: 'הלפטופ שלי נשרף פנימית. עם הדמיה תרמית FLIR מצאו בדיוק מה שבור — חסכו לי קניית מחשב חדש.', stars: 5 },
  { name: 'יוסי מ.', text: 'PS5 עם בעיית HDMI מוכרת — טיפלו בזה בבית תוך 45 דקות. שירות מדהים.', stars: 5 },
]

function Reviews() {
  return (
    <section className="py-20 px-6">
      <div className="max-w-6xl mx-auto">
        <FadeSection>
          <motion.div variants={fadeUp} className="text-center mb-12">
            <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
              ביקורות לקוחות
            </div>
            <h2 className="text-4xl sm:text-5xl font-black tracking-tight">מה אומרים עלינו</h2>
          </motion.div>
          <div className="grid md:grid-cols-3 gap-6">
            {REVIEWS.map((r, i) => (
              <motion.div
                key={r.name}
                variants={fadeUp}
                custom={i}
                whileHover={{ y: -4, transition: { duration: 0.2 } }}
                className="glass rounded-2xl p-6 border border-white/10 hover:border-electric/30 transition-all flex flex-col gap-4"
              >
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
        </FadeSection>
      </div>
    </section>
  )
}

// ─── Footer ───────────────────────────────────────────────────────────────────
function Footer() {
  return (
    <footer className="py-10 px-6 border-t border-white/10 mb-16 sm:mb-0">
      <div className="max-w-6xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-4 text-sm text-slate-500">
        <div className="flex items-center gap-2 font-bold text-white">
          <Zap className="w-4 h-4 text-electric" fill="currentColor" />
          Fix Express Labs
        </div>
        <span>© {new Date().getFullYear()} ירין הלילי | {WA_DISPLAY}</span>
        <a href={WA_URL} target="_blank" rel="noopener noreferrer" className="text-electric hover:text-electric-light transition-colors">
          https://wa.me/{WA_NUMBER}
        </a>
      </div>
    </footer>
  )
}

// ─── Floating WhatsApp ────────────────────────────────────────────────────────
function FloatingWhatsApp() {
  return (
    <motion.a
      href={WA_URL}
      target="_blank"
      rel="noopener noreferrer"
      aria-label="WhatsApp"
      initial={{ scale: 0, opacity: 0 }}
      animate={{ scale: 1, opacity: 1 }}
      transition={{ delay: 1.5, type: 'spring', stiffness: 200 }}
      whileHover={{ scale: 1.12 }}
      className="fixed bottom-24 left-6 sm:bottom-6 z-50 w-14 h-14 rounded-full bg-[#25D366] hover:bg-[#1fb957] flex items-center justify-center shadow-2xl shadow-green-900/50 animate-float"
    >
      <MessageCircle className="w-7 h-7 text-white" fill="white" />
    </motion.a>
  )
}

// ─── Sticky Mobile Contact Bar ────────────────────────────────────────────────
function StickyMobileBar() {
  return (
    <motion.div
      initial={{ y: 80, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      transition={{ delay: 1, duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
      className="fixed bottom-0 inset-x-0 z-40 sm:hidden"
    >
      <div className="glass border-t border-white/15 px-4 py-3 flex gap-3">
        <a
          href={`tel:+${WA_NUMBER}`}
          className="flex-1 flex items-center justify-center gap-2 py-3 rounded-xl bg-white/10 border border-white/15 text-white font-semibold text-sm hover:bg-white/15 transition-all"
        >
          <Phone className="w-4 h-4 text-electric" />
          {WA_DISPLAY}
        </a>
        <a
          href={WA_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="flex-1 flex items-center justify-center gap-2 py-3 rounded-xl bg-[#25D366] hover:bg-[#1fb957] text-white font-bold text-sm transition-all"
        >
          <MessageCircle className="w-4 h-4" fill="white" />
          WhatsApp
        </a>
      </div>
    </motion.div>
  )
}

// ─── App ──────────────────────────────────────────────────────────────────────
export default function App() {
  if (isAdmin) return <AdminDashboard />

  return (
    <div className="min-h-screen bg-slate-950 text-white overflow-x-hidden" dir="rtl">
      <Navbar />
      <Hero />
      <Services />
      <TechGear />
      <WhyUs />
      <Reviews />
      <Footer />
      <FloatingWhatsApp />
      <StickyMobileBar />
    </div>
  )
}
