import { useState, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Zap, MessageCircle, Upload, CheckCircle2, Star, ShieldCheck,
  Smartphone, Laptop, Gamepad2, ArrowLeft, Camera, X,
} from 'lucide-react'

const WA_NUMBER = '972504951109'
const WA_DISPLAY = '050-495-1109'

// ─── Refurbished inventory ────────────────────────────────────────────────────
const REFURBISHED = [
  {
    id: 1,
    title: 'MacBook Pro 14" M2 Pro',
    condition: 'מצב מושלם',
    conditionEn: 'Flawless',
    grade: 'A+',
    specs: ['16GB RAM', '512GB SSD', 'Space Gray', 'מסך 120Hz ProMotion'],
    price: 6800,
    originalPrice: 9200,
    category: 'laptop',
    icon: Laptop,
    badge: 'הכי נמכר',
    badgeColor: 'bg-electric text-white',
  },
  {
    id: 2,
    title: 'iPhone 14 Pro Max',
    condition: 'מצב מעולה',
    conditionEn: 'Excellent',
    grade: 'A',
    specs: ['256GB', 'Deep Purple', 'סוללה 97%', 'ללא שריטות'],
    price: 3200,
    originalPrice: 4800,
    category: 'phone',
    icon: Smartphone,
    badge: null,
  },
  {
    id: 3,
    title: 'PS5 Disc Edition',
    condition: 'מצב טוב מאוד',
    conditionEn: 'Very Good',
    grade: 'A',
    specs: ['כולל שלט DualSense', 'תרמופסטה חדשה', 'ניקוי מלא', 'אחריות 90 יום'],
    price: 1800,
    originalPrice: 2500,
    category: 'console',
    icon: Gamepad2,
    badge: 'חדש במלאי',
    badgeColor: 'bg-green-500 text-white',
  },
  {
    id: 4,
    title: 'iPhone 15',
    condition: 'מצב מושלם',
    conditionEn: 'Flawless',
    grade: 'A+',
    specs: ['128GB', 'Black', 'סוללה 100%', 'מסך OLED חדש'],
    price: 3600,
    originalPrice: 4400,
    category: 'phone',
    icon: Smartphone,
    badge: null,
  },
  {
    id: 5,
    title: 'MacBook Air M2',
    condition: 'מצב טוב',
    conditionEn: 'Good',
    grade: 'B+',
    specs: ['8GB RAM', '256GB SSD', 'Midnight', 'מסך Liquid Retina'],
    price: 4200,
    originalPrice: 6000,
    category: 'laptop',
    icon: Laptop,
    badge: 'מחיר מיוחד',
    badgeColor: 'bg-amber-500 text-black',
  },
  {
    id: 6,
    title: 'Samsung Galaxy S24 Ultra',
    condition: 'מצב מעולה',
    conditionEn: 'Excellent',
    grade: 'A',
    specs: ['256GB', 'Titanium Black', 'S-Pen כלול', 'סוללה 95%'],
    price: 3900,
    originalPrice: 5600,
    category: 'phone',
    icon: Smartphone,
    badge: null,
  },
]

const GRADE_COLOR = {
  'A+': 'text-emerald-400 border-emerald-400/40 bg-emerald-400/10',
  'A':  'text-electric border-electric/40 bg-electric/10',
  'B+': 'text-yellow-400 border-yellow-400/40 bg-yellow-400/10',
}

const CATEGORIES = [
  { id: 'all', label: 'הכל' },
  { id: 'phone', label: 'סמארטפונים' },
  { id: 'laptop', label: 'מחשבים' },
  { id: 'console', label: 'קונסולות' },
]

// ─── Trade-in form ────────────────────────────────────────────────────────────
const DEVICE_TYPES = [
  'iPhone', 'Samsung', 'Google Pixel', 'MacBook', 'לפטופ אחר', 'PS5', 'Xbox', 'Nintendo Switch', 'iPad', 'אחר',
]

function TradeInForm() {
  const [formData, setFormData] = useState({
    deviceType: '',
    model: '',
    issue: '',
    description: '',
    contactName: '',
    contactPhone: '',
  })
  const [photos, setPhotos] = useState([])
  const [submitted, setSubmitted] = useState(false)
  const fileRef = useRef(null)

  function handleChange(e) {
    setFormData((p) => ({ ...p, [e.target.name]: e.target.value }))
  }

  function handleFiles(e) {
    const files = Array.from(e.target.files || [])
    const previews = files.map((f) => ({ name: f.name, url: URL.createObjectURL(f) }))
    setPhotos((p) => [...p, ...previews].slice(0, 5))
  }

  function removePhoto(idx) {
    setPhotos((p) => p.filter((_, i) => i !== idx))
  }

  function handleSubmit(e) {
    e.preventDefault()
    const msg = encodeURIComponent(
      `🔧 הצעת Trade-In חדשה!\n\n👤 ${formData.contactName} | 📞 ${formData.contactPhone}\n📱 מכשיר: ${formData.deviceType} ${formData.model}\n🛠️ תקלה: ${formData.issue}\n📝 תיאור: ${formData.description}\n\n(שולח/ת ${photos.length} תמונות בנפרד)`
    )
    window.open(`https://wa.me/${WA_NUMBER}?text=${msg}`, '_blank')
    setSubmitted(true)
  }

  if (submitted) {
    return (
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        className="flex flex-col items-center justify-center py-16 text-center gap-4"
      >
        <div className="w-16 h-16 rounded-full bg-green-500/20 border border-green-500/40 flex items-center justify-center">
          <CheckCircle2 className="w-8 h-8 text-green-400" />
        </div>
        <h3 className="text-2xl font-bold">הבקשה נשלחה!</h3>
        <p className="text-slate-400">ניצור קשר תוך שעה עם הצעת מחיר. בינתיים שלח את התמונות ב-WhatsApp.</p>
        <a
          href={`https://wa.me/${WA_NUMBER}`}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-2 px-6 py-3 rounded-full bg-[#25D366] text-white font-bold"
        >
          <MessageCircle className="w-4 h-4" fill="white" />
          פתח WhatsApp
        </a>
        <button onClick={() => setSubmitted(false)} className="text-slate-500 text-sm hover:text-white transition-colors">
          שלח בקשה נוספת
        </button>
      </motion.div>
    )
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div className="grid sm:grid-cols-2 gap-4">
        {/* Device type */}
        <div>
          <label className="block text-sm font-semibold text-slate-300 mb-1.5">סוג מכשיר *</label>
          <select
            name="deviceType"
            required
            value={formData.deviceType}
            onChange={handleChange}
            className="w-full px-4 py-3 rounded-xl glass border border-white/10 focus:border-electric/50 bg-slate-900 text-white text-sm focus:outline-none transition-all"
          >
            <option value="">בחר...</option>
            {DEVICE_TYPES.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
        </div>
        {/* Model */}
        <div>
          <label className="block text-sm font-semibold text-slate-300 mb-1.5">דגם *</label>
          <input
            name="model"
            required
            value={formData.model}
            onChange={handleChange}
            placeholder="למשל: iPhone 14 Pro, MacBook M2..."
            className="w-full px-4 py-3 rounded-xl glass border border-white/10 focus:border-electric/50 bg-transparent text-white placeholder-slate-500 text-sm focus:outline-none transition-all"
          />
        </div>
      </div>

      {/* Issue */}
      <div>
        <label className="block text-sm font-semibold text-slate-300 mb-1.5">סוג התקלה *</label>
        <input
          name="issue"
          required
          value={formData.issue}
          onChange={handleChange}
          placeholder="למשל: מסך שבור, לא נדלק, פגם בלוח..."
          className="w-full px-4 py-3 rounded-xl glass border border-white/10 focus:border-electric/50 bg-transparent text-white placeholder-slate-500 text-sm focus:outline-none transition-all"
        />
      </div>

      {/* Description */}
      <div>
        <label className="block text-sm font-semibold text-slate-300 mb-1.5">תיאור מפורט</label>
        <textarea
          name="description"
          rows={3}
          value={formData.description}
          onChange={handleChange}
          placeholder="תאר את מצב המכשיר, היסטוריית הנפילות/נזקים, ניסיונות תיקון קודמים..."
          className="w-full px-4 py-3 rounded-xl glass border border-white/10 focus:border-electric/50 bg-transparent text-white placeholder-slate-500 text-sm focus:outline-none transition-all resize-none"
        />
      </div>

      {/* Photo upload */}
      <div>
        <label className="block text-sm font-semibold text-slate-300 mb-1.5">תמונות המכשיר (עד 5)</label>
        <div
          onClick={() => fileRef.current?.click()}
          className="border-2 border-dashed border-white/15 hover:border-electric/50 rounded-xl p-6 text-center cursor-pointer transition-all group"
        >
          <Camera className="w-8 h-8 text-slate-500 group-hover:text-electric mx-auto mb-2 transition-colors" />
          <p className="text-slate-500 text-sm group-hover:text-slate-300 transition-colors">לחץ להעלאת תמונות</p>
          <input ref={fileRef} type="file" accept="image/*" multiple className="hidden" onChange={handleFiles} />
        </div>
        {photos.length > 0 && (
          <div className="flex flex-wrap gap-2 mt-3">
            {photos.map((p, i) => (
              <div key={i} className="relative w-16 h-16 rounded-lg overflow-hidden">
                <img src={p.url} alt="" className="w-full h-full object-cover" />
                <button
                  type="button"
                  onClick={() => removePhoto(i)}
                  className="absolute top-0.5 right-0.5 w-4 h-4 rounded-full bg-red-500 flex items-center justify-center"
                >
                  <X className="w-2.5 h-2.5 text-white" />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="grid sm:grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-semibold text-slate-300 mb-1.5">שם *</label>
          <input
            name="contactName"
            required
            value={formData.contactName}
            onChange={handleChange}
            placeholder="שם מלא"
            className="w-full px-4 py-3 rounded-xl glass border border-white/10 focus:border-electric/50 bg-transparent text-white placeholder-slate-500 text-sm focus:outline-none transition-all"
          />
        </div>
        <div>
          <label className="block text-sm font-semibold text-slate-300 mb-1.5">טלפון *</label>
          <input
            name="contactPhone"
            required
            value={formData.contactPhone}
            onChange={handleChange}
            placeholder="05X-XXX-XXXX"
            className="w-full px-4 py-3 rounded-xl glass border border-white/10 focus:border-electric/50 bg-transparent text-white placeholder-slate-500 text-sm focus:outline-none transition-all"
          />
        </div>
      </div>

      <button
        type="submit"
        className="w-full py-4 rounded-2xl bg-electric hover:bg-electric-light transition-all text-white font-bold text-lg glow-blue flex items-center justify-center gap-2"
      >
        <Upload className="w-5 h-5" />
        שלח לקבלת הצעת מחיר
      </button>
    </form>
  )
}

// ─── Refurbished card ─────────────────────────────────────────────────────────
function RefurbishedCard({ item }) {
  const savings = item.originalPrice - item.price
  const savingsPct = Math.round((savings / item.originalPrice) * 100)
  const waMsg = encodeURIComponent(`היי! אני מעוניין ב-${item.title} ב-₪${item.price}. האם עדיין זמין? 📱`)

  return (
    <motion.div
      whileHover={{ y: -4, scale: 1.01 }}
      transition={{ duration: 0.2 }}
      className="glass rounded-2xl border border-white/10 hover:border-electric/40 transition-all overflow-hidden flex flex-col"
    >
      {/* Header */}
      <div className="p-5 bg-gradient-to-br from-electric/10 to-transparent border-b border-white/5">
        <div className="flex items-start justify-between mb-3">
          <div className="flex flex-col gap-1.5">
            {item.badge && (
              <span className={`text-xs px-2.5 py-0.5 rounded-full font-bold ${item.badgeColor}`}>
                {item.badge}
              </span>
            )}
            <span className={`text-xs px-2.5 py-1 rounded-full border font-bold ${GRADE_COLOR[item.grade]}`}>
              דרגה {item.grade}
            </span>
          </div>
          <item.icon className="w-10 h-10 text-electric/60" />
        </div>
        <h3 className="text-lg font-bold leading-tight">{item.title}</h3>
        <p className="text-slate-400 text-sm mt-0.5">{item.condition}</p>
      </div>

      {/* Specs */}
      <div className="px-5 py-4 flex-1">
        <div className="flex flex-wrap gap-1.5 mb-4">
          {item.specs.map((s) => (
            <span key={s} className="text-xs px-2 py-0.5 rounded-full bg-white/5 border border-white/10 text-slate-300">
              {s}
            </span>
          ))}
        </div>

        {/* Certified badge */}
        <div className="flex items-center gap-2 p-2.5 rounded-xl bg-electric/8 border border-electric/20 mb-4">
          <ShieldCheck className="w-4 h-4 text-electric flex-shrink-0" />
          <span className="text-xs text-electric font-semibold">Certified by Fix Express Labs</span>
        </div>

        {/* Price */}
        <div className="flex items-end gap-2">
          <span className="text-3xl font-black text-gradient">₪{item.price.toLocaleString()}</span>
          <div className="mb-1">
            <span className="text-slate-500 text-sm line-through">₪{item.originalPrice.toLocaleString()}</span>
            <span className="block text-green-400 text-xs font-bold">חסכת {savingsPct}%</span>
          </div>
        </div>
      </div>

      {/* CTA */}
      <div className="px-5 pb-5">
        <a
          href={`https://wa.me/${WA_NUMBER}?text=${waMsg}`}
          target="_blank"
          rel="noopener noreferrer"
          className="w-full flex items-center justify-center gap-2 py-3 rounded-xl bg-[#25D366] hover:bg-[#1fb957] transition-all text-white font-bold text-sm"
        >
          <MessageCircle className="w-4 h-4" fill="white" />
          רכוש דרך WhatsApp
        </a>
      </div>
    </motion.div>
  )
}

// ─── Store page ───────────────────────────────────────────────────────────────
export default function Store() {
  const [tab, setTab] = useState('buy')
  const [category, setCategory] = useState('all')

  const filtered = category === 'all'
    ? REFURBISHED
    : REFURBISHED.filter((i) => i.category === category)

  return (
    <div className="min-h-screen bg-slate-950 text-white font-sans pb-20" dir="rtl">
      {/* Header */}
      <div className="glass border-b border-white/10">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
          <a href="/" className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-electric flex items-center justify-center glow-blue">
              <Zap className="w-4 h-4 text-white" fill="white" />
            </div>
            <span className="font-bold tracking-tight">Fix <span className="text-gradient">Express</span> Labs</span>
          </a>
          <a href="/" className="flex items-center gap-1.5 text-slate-400 hover:text-white text-sm transition-colors">
            <ArrowLeft className="w-4 h-4" />
            חזור לאתר
          </a>
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-6 py-16">
        {/* Hero */}
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-center mb-14"
        >
          <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
            <ShieldCheck className="w-3.5 h-3.5" />
            חנות מכשירים מחודשים
          </div>
          <h1 className="text-5xl sm:text-6xl font-black tracking-tight mb-4">
            קנה, מכור,{' '}
            <span className="text-gradient">החלף</span>
          </h1>
          <p className="text-slate-400 text-xl max-w-xl mx-auto">
            מכשירים מחודשים עם אחריות, ובצד השני — מכור לנו את המכשיר הפגוע שלך ב-Cash.
          </p>
        </motion.div>

        {/* Tab switcher */}
        <div className="flex justify-center mb-12">
          <div className="glass rounded-2xl p-1.5 border border-white/10 flex gap-1">
            {[
              { id: 'buy', label: '🛒 קנה מחודש' },
              { id: 'sell', label: '💰 מכור לנו' },
            ].map((t) => (
              <button
                key={t.id}
                onClick={() => setTab(t.id)}
                className={`relative px-8 py-3 rounded-xl text-sm font-bold transition-all ${
                  tab === t.id ? 'bg-electric text-white glow-blue' : 'text-slate-400 hover:text-white'
                }`}
              >
                {t.label}
                {tab === t.id && (
                  <motion.div
                    layoutId="tab-bg"
                    className="absolute inset-0 bg-electric rounded-xl -z-10"
                    transition={{ type: 'spring', stiffness: 300, damping: 30 }}
                  />
                )}
              </button>
            ))}
          </div>
        </div>

        <AnimatePresence mode="wait">
          {/* Buy tab */}
          {tab === 'buy' && (
            <motion.div
              key="buy"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.35 }}
            >
              {/* Category filter */}
              <div className="flex justify-center gap-2 mb-8 flex-wrap">
                {CATEGORIES.map((c) => (
                  <button
                    key={c.id}
                    onClick={() => setCategory(c.id)}
                    className={`px-4 py-2 rounded-xl text-sm font-semibold border transition-all ${
                      category === c.id
                        ? 'bg-electric text-white border-electric'
                        : 'glass border-white/10 text-slate-400 hover:border-electric/40'
                    }`}
                  >
                    {c.label}
                  </button>
                ))}
              </div>

              <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-6">
                {filtered.map((item, i) => (
                  <motion.div
                    key={item.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: i * 0.06 }}
                  >
                    <RefurbishedCard item={item} />
                  </motion.div>
                ))}
              </div>

              {/* Trust row */}
              <div className="mt-12 grid sm:grid-cols-3 gap-4">
                {[
                  { icon: ShieldCheck, label: 'כל מכשיר מאושר', sub: 'בדיקה מלאה לפני מכירה' },
                  { icon: Star, label: 'אחריות 90 יום', sub: 'על כל מכשיר מחודש' },
                  { icon: CheckCircle2, label: 'מחיר שקוף', sub: 'ללא הפתעות' },
                ].map(({ icon: Icon, label, sub }) => (
                  <div key={label} className="glass rounded-xl p-4 border border-white/10 flex items-center gap-3">
                    <Icon className="w-5 h-5 text-electric flex-shrink-0" />
                    <div>
                      <p className="text-sm font-bold">{label}</p>
                      <p className="text-xs text-slate-500">{sub}</p>
                    </div>
                  </div>
                ))}
              </div>
            </motion.div>
          )}

          {/* Sell tab */}
          {tab === 'sell' && (
            <motion.div
              key="sell"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.35 }}
              className="max-w-2xl mx-auto"
            >
              {/* How it works */}
              <div className="glass rounded-2xl border border-white/10 p-6 mb-8">
                <h3 className="text-lg font-bold mb-4">איך זה עובד?</h3>
                <div className="grid sm:grid-cols-3 gap-4">
                  {[
                    { n: '1', label: 'מלא את הטופס', sub: 'צרף תמונות של המכשיר' },
                    { n: '2', label: 'קבל הצעה', sub: 'תוך שעה — ב-WhatsApp' },
                    { n: '3', label: 'תשלום Cash', sub: 'נסגור בפגישה קצרה' },
                  ].map((s) => (
                    <div key={s.n} className="flex items-start gap-3">
                      <div className="w-7 h-7 rounded-full bg-electric text-white text-xs font-black flex items-center justify-center flex-shrink-0">
                        {s.n}
                      </div>
                      <div>
                        <p className="text-sm font-semibold">{s.label}</p>
                        <p className="text-xs text-slate-500">{s.sub}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="glass rounded-2xl border border-white/10 p-6">
                <h3 className="text-xl font-bold mb-6">טופס Trade-In</h3>
                <TradeInForm />
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  )
}
