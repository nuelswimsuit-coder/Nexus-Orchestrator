import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Smartphone, Laptop, Gamepad2, Tablet,
  ChevronLeft, MessageCircle, Zap, Info,
} from 'lucide-react'

const WA_NUMBER = '972504951109'
const WA_DISPLAY = '050-495-1109'

// ─── Pricing data ─────────────────────────────────────────────────────────────
const DEVICES = [
  {
    id: 'iphone',
    label: 'iPhone',
    icon: Smartphone,
    color: 'from-blue-500/20 to-electric/10',
    border: 'border-blue-500/40',
    models: [
      'iPhone 11 / 11 Pro',
      'iPhone 12 / 12 Pro',
      'iPhone 13 / 13 Pro',
      'iPhone 14 / 14 Pro',
      'iPhone 14 Pro Max',
      'iPhone 15 / 15 Pro',
      'iPhone 15 Pro Max',
    ],
  },
  {
    id: 'samsung',
    label: 'Samsung',
    icon: Smartphone,
    color: 'from-cyan-500/20 to-blue-500/10',
    border: 'border-cyan-500/40',
    models: [
      'Galaxy S21 / S22',
      'Galaxy S23 / S24',
      'Galaxy S24 Ultra',
      'Galaxy Fold / Flip',
      'Galaxy A-Series',
    ],
  },
  {
    id: 'macbook',
    label: 'MacBook',
    icon: Laptop,
    color: 'from-violet-500/20 to-purple-500/10',
    border: 'border-violet-500/40',
    models: [
      'MacBook Air M1',
      'MacBook Air M2 / M3',
      'MacBook Pro 14" M1/M2',
      'MacBook Pro 16" M2/M3',
      'MacBook Pro Intel',
    ],
  },
  {
    id: 'laptop',
    label: 'לפטופ אחר',
    icon: Laptop,
    color: 'from-indigo-500/20 to-blue-500/10',
    border: 'border-indigo-500/40',
    models: ['Dell XPS', 'Lenovo ThinkPad', 'HP Spectre', 'ASUS ROG', 'אחר'],
  },
  {
    id: 'ps5',
    label: 'PS5',
    icon: Gamepad2,
    color: 'from-rose-500/20 to-pink-500/10',
    border: 'border-rose-500/40',
    models: ['PS5 Disc Edition', 'PS5 Digital Edition', 'PS4 / PS4 Pro'],
  },
  {
    id: 'xbox',
    label: 'Xbox',
    icon: Gamepad2,
    color: 'from-green-500/20 to-emerald-500/10',
    border: 'border-green-500/40',
    models: ['Xbox Series X', 'Xbox Series S', 'Xbox One X / S'],
  },
  {
    id: 'ipad',
    label: 'iPad',
    icon: Tablet,
    color: 'from-teal-500/20 to-cyan-500/10',
    border: 'border-teal-500/40',
    models: ['iPad Pro 11" / 12.9"', 'iPad Air', 'iPad Mini', 'iPad (9th/10th Gen)'],
  },
]

// issue key → { label, prices: { deviceId: [min, max] | null } }
const ISSUES = [
  {
    id: 'screen',
    label: 'מסך שבור / לא נדלק',
    emoji: '📱',
    prices: {
      iphone:  { '11': [280, 350], '12': [320, 400], '13': [360, 450], '14': [420, 520], '14pro': [500, 620], '15': [450, 560], default: [350, 550] },
      samsung: { default: [250, 450] },
      macbook: { default: [550, 900] },
      laptop:  { default: [300, 650] },
      ps5:     { default: [300, 450] },
      xbox:    { default: [250, 400] },
      ipad:    { default: [300, 550] },
    },
    note: 'כולל מסך מקורי OEM',
  },
  {
    id: 'battery',
    label: 'סוללה — ניקוז מהיר / לא נטענת',
    emoji: '🔋',
    prices: {
      iphone:  { default: [180, 250] },
      samsung: { default: [150, 220] },
      macbook: { default: [350, 500] },
      laptop:  { default: [200, 380] },
      ps5:     { default: [200, 280] },
      xbox:    { default: [180, 260] },
      ipad:    { default: [200, 320] },
    },
    note: 'סוללה מקורית, כיול מלא',
  },
  {
    id: 'charging',
    label: 'פורט טעינה / לא מטעין',
    emoji: '⚡',
    prices: {
      iphone:  { default: [180, 260] },
      samsung: { default: [150, 230] },
      macbook: { default: [300, 480] },
      laptop:  { default: [200, 350] },
      ps5:     { default: [220, 320] },
      xbox:    { default: [180, 280] },
      ipad:    { default: [180, 280] },
    },
    note: 'מיקרו-לחמה / החלפת פורט',
  },
  {
    id: 'motherboard',
    label: 'לוח אם / לא נדלק כלל',
    emoji: '🔬',
    prices: {
      iphone:  { default: [450, 800] },
      samsung: { default: [400, 750] },
      macbook: { default: [600, 1200] },
      laptop:  { default: [450, 900] },
      ps5:     { default: [400, 700] },
      xbox:    { default: [350, 650] },
      ipad:    { default: [400, 700] },
    },
    note: 'מיקרו-לחמה BGA, ציוד JBC מקצועי',
    premium: true,
  },
  {
    id: 'water',
    label: 'נזק נוזלים',
    emoji: '💧',
    prices: {
      iphone:  { default: [350, 650] },
      samsung: { default: [300, 600] },
      macbook: { default: [500, 1000] },
      laptop:  { default: [350, 750] },
      ps5:     { default: [300, 600] },
      xbox:    { default: [280, 550] },
      ipad:    { default: [320, 600] },
    },
    note: 'אולטרה-סוני + הדמיה תרמית',
  },
  {
    id: 'hdmi',
    label: 'פורט HDMI / אין תמונה',
    emoji: '🖥️',
    prices: {
      iphone:  null,
      samsung: null,
      macbook: { default: [300, 450] },
      laptop:  { default: [250, 400] },
      ps5:     { default: [280, 380] },
      xbox:    { default: [250, 350] },
      ipad:    null,
    },
    note: 'לחמה מדויקת + בדיקת זרם',
  },
  {
    id: 'camera',
    label: 'מצלמה / FaceID לא עובד',
    emoji: '📷',
    prices: {
      iphone:  { default: [300, 500] },
      samsung: { default: [250, 420] },
      macbook: { default: [200, 350] },
      laptop:  { default: [180, 300] },
      ps5:     null,
      xbox:    null,
      ipad:    { default: [250, 400] },
    },
    note: 'שחזור FaceID כולל',
  },
]

function getPrice(issue, deviceId) {
  const priceMap = issue.prices[deviceId]
  if (!priceMap) return null
  return priceMap.default
}

// ─── Step indicators ──────────────────────────────────────────────────────────
function StepDot({ n, active, done }) {
  return (
    <div className="flex items-center gap-2">
      <div className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold border transition-all duration-300 ${
        done ? 'bg-electric border-electric text-white' :
        active ? 'bg-electric/20 border-electric text-electric' :
        'bg-white/5 border-white/20 text-slate-500'
      }`}>
        {done ? '✓' : n}
      </div>
    </div>
  )
}

// ─── Main component ───────────────────────────────────────────────────────────
export default function PriceEstimator() {
  const [step, setStep] = useState(1)          // 1 = device, 2 = model, 3 = issue, 4 = result
  const [device, setDevice] = useState(null)
  const [model, setModel] = useState(null)
  const [issue, setIssue] = useState(null)

  const selectedDevice = DEVICES.find((d) => d.id === device)
  const selectedIssue = ISSUES.find((i) => i.id === issue)
  const priceRange = selectedDevice && selectedIssue ? getPrice(selectedIssue, device) : null

  function reset() {
    setStep(1); setDevice(null); setModel(null); setIssue(null)
  }

  const waMsg = encodeURIComponent(
    `היי! אני מעוניין בתיקון ${selectedDevice?.label || ''} ${model || ''} — ${selectedIssue?.label || ''}. מה המחיר המדויק? 🔧`
  )
  const waUrl = `https://wa.me/${WA_NUMBER}?text=${waMsg}`

  return (
    <section id="estimator" className="py-24 px-6">
      <div className="max-w-3xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
            <Zap className="w-3.5 h-3.5" />
            מחשבון מחירים
          </div>
          <h2 className="text-4xl sm:text-5xl font-black mb-4 tracking-tight">
            כמה יעלה <span className="text-gradient">התיקון שלך?</span>
          </h2>
          <p className="text-slate-400 text-lg">בחר מכשיר ותקלה — קבל הערכת מחיר מיידית</p>
        </div>

        {/* Step indicators */}
        <div className="flex items-center justify-center gap-3 mb-10">
          {['מכשיר', 'דגם', 'תקלה', 'מחיר'].map((label, i) => (
            <div key={label} className="flex items-center gap-3">
              <div className="flex flex-col items-center gap-1">
                <StepDot n={i + 1} active={step === i + 1} done={step > i + 1} />
                <span className={`text-xs transition-colors ${step === i + 1 ? 'text-electric' : step > i + 1 ? 'text-electric/60' : 'text-slate-600'}`}>
                  {label}
                </span>
              </div>
              {i < 3 && <ChevronLeft className="w-3 h-3 text-slate-600 mb-4" />}
            </div>
          ))}
        </div>

        {/* Card container */}
        <div className="glass rounded-3xl border border-white/10 overflow-hidden">
          <AnimatePresence mode="wait">

            {/* Step 1 — Device */}
            {step === 1 && (
              <motion.div
                key="step1"
                initial={{ opacity: 0, x: 30 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -30 }}
                transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
                className="p-8"
              >
                <h3 className="text-xl font-bold mb-6 text-center">איזה מכשיר?</h3>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  {DEVICES.map((d) => (
                    <motion.button
                      key={d.id}
                      whileHover={{ scale: 1.04 }}
                      whileTap={{ scale: 0.97 }}
                      onClick={() => { setDevice(d.id); setStep(2) }}
                      className={`flex flex-col items-center gap-2 p-4 rounded-2xl border bg-gradient-to-br ${d.color} ${d.border} hover:border-electric/60 transition-all`}
                    >
                      <d.icon className="w-7 h-7 text-electric" />
                      <span className="text-sm font-semibold">{d.label}</span>
                    </motion.button>
                  ))}
                </div>
              </motion.div>
            )}

            {/* Step 2 — Model */}
            {step === 2 && selectedDevice && (
              <motion.div
                key="step2"
                initial={{ opacity: 0, x: 30 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -30 }}
                transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
                className="p-8"
              >
                <div className="flex items-center gap-3 mb-6">
                  <button onClick={() => setStep(1)} className="text-slate-400 hover:text-white transition-colors">
                    <ChevronLeft className="w-5 h-5 rotate-180" />
                  </button>
                  <selectedDevice.icon className="w-5 h-5 text-electric" />
                  <h3 className="text-xl font-bold">איזה דגם {selectedDevice.label}?</h3>
                </div>
                <div className="flex flex-col gap-2">
                  {selectedDevice.models.map((m) => (
                    <motion.button
                      key={m}
                      whileHover={{ x: -4 }}
                      whileTap={{ scale: 0.98 }}
                      onClick={() => { setModel(m); setStep(3) }}
                      className="text-right px-5 py-3.5 rounded-xl glass border border-white/10 hover:border-electric/50 hover:bg-electric/5 transition-all flex items-center justify-between"
                    >
                      <ChevronLeft className="w-4 h-4 text-slate-500" />
                      <span className="font-medium">{m}</span>
                    </motion.button>
                  ))}
                </div>
              </motion.div>
            )}

            {/* Step 3 — Issue */}
            {step === 3 && (
              <motion.div
                key="step3"
                initial={{ opacity: 0, x: 30 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -30 }}
                transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
                className="p-8"
              >
                <div className="flex items-center gap-3 mb-6">
                  <button onClick={() => setStep(2)} className="text-slate-400 hover:text-white transition-colors">
                    <ChevronLeft className="w-5 h-5 rotate-180" />
                  </button>
                  <h3 className="text-xl font-bold">מה התקלה?</h3>
                </div>
                <div className="grid sm:grid-cols-2 gap-2">
                  {ISSUES.map((iss) => {
                    const hasPrice = device ? getPrice(iss, device) !== null : true
                    return (
                      <motion.button
                        key={iss.id}
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.97 }}
                        disabled={!hasPrice}
                        onClick={() => { setIssue(iss.id); setStep(4) }}
                        className={`text-right px-4 py-3.5 rounded-xl border transition-all flex items-center gap-3 ${
                          hasPrice
                            ? 'glass border-white/10 hover:border-electric/50 hover:bg-electric/5'
                            : 'opacity-30 cursor-not-allowed bg-white/3 border-white/5'
                        } ${iss.premium ? 'border-electric/25 bg-electric/5' : ''}`}
                      >
                        <span className="text-xl flex-shrink-0">{iss.emoji}</span>
                        <div className="flex-1 min-w-0">
                          <p className="font-medium text-sm leading-tight">{iss.label}</p>
                          {iss.premium && (
                            <p className="text-xs text-electric mt-0.5">ציוד מתקדם נדרש</p>
                          )}
                        </div>
                        {!hasPrice && <span className="text-xs text-slate-600">לא רלוונטי</span>}
                      </motion.button>
                    )
                  })}
                </div>
              </motion.div>
            )}

            {/* Step 4 — Result */}
            {step === 4 && selectedDevice && selectedIssue && (
              <motion.div
                key="step4"
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.95 }}
                transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
                className="p-8"
              >
                {/* Summary */}
                <div className="flex flex-wrap gap-2 mb-6 justify-center">
                  {[selectedDevice.label, model, selectedIssue.emoji + ' ' + selectedIssue.label].map((t) => (
                    <span key={t} className="glass-blue border border-electric/30 text-electric text-sm px-3 py-1 rounded-full font-semibold">
                      {t}
                    </span>
                  ))}
                </div>

                {priceRange ? (
                  <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.15 }}
                    className="text-center mb-8"
                  >
                    <p className="text-slate-400 text-sm mb-2">הערכת מחיר</p>
                    <div className="flex items-end justify-center gap-2 mb-1">
                      <motion.span
                        initial={{ opacity: 0, scale: 0.7 }}
                        animate={{ opacity: 1, scale: 1 }}
                        transition={{ delay: 0.25, type: 'spring', stiffness: 180 }}
                        className="text-6xl font-black text-gradient"
                      >
                        ₪{priceRange[0]}–{priceRange[1]}
                      </motion.span>
                    </div>
                    <div className="flex items-center justify-center gap-1.5 text-sm text-slate-400 mt-2">
                      <Info className="w-3.5 h-3.5 text-electric" />
                      {selectedIssue.note}
                    </div>
                    {selectedIssue.premium && (
                      <div className="mt-3 inline-flex items-center gap-1.5 glass-blue border border-electric/30 rounded-full px-3 py-1 text-xs text-electric font-medium">
                        <Zap className="w-3 h-3" fill="currentColor" />
                        תיקון ברמת BGA — ציוד JBC / FLIR
                      </div>
                    )}
                  </motion.div>
                ) : (
                  <div className="text-center py-6 text-slate-400">
                    <p className="text-lg font-semibold mb-1">צור קשר לקבלת הצעה</p>
                    <p className="text-sm">התיקון הזה דורש אבחון אישי</p>
                  </div>
                )}

                {/* Disclaimer */}
                <p className="text-xs text-slate-500 text-center mb-6">
                  * המחיר הוא הערכה בלבד. המחיר הסופי ייקבע לאחר אבחון. כולל חלקים + עבודה.
                </p>

                {/* CTAs */}
                <div className="flex flex-col sm:flex-row gap-3">
                  <a
                    href={waUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-2 py-4 rounded-2xl bg-[#25D366] hover:bg-[#1fb957] text-white font-bold transition-all shadow-lg shadow-green-900/30"
                  >
                    <MessageCircle className="w-5 h-5" fill="white" />
                    קבע תיקון — {WA_DISPLAY}
                  </a>
                  <button
                    onClick={reset}
                    className="flex-1 py-4 rounded-2xl glass border border-white/15 hover:border-electric/40 text-slate-300 font-semibold transition-all"
                  >
                    חזור להתחלה
                  </button>
                </div>
              </motion.div>
            )}

          </AnimatePresence>
        </div>
      </div>
    </section>
  )
}
