import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { ChevronDown, MessageCircle } from 'lucide-react'

const WA_NUMBER = '972504951109'
const WA_DISPLAY = '050-495-1109'

const FAQS = [
  {
    q: 'כמה זמן לוקח התיקון?',
    a: 'רוב התיקונים מתבצעים תוך 30–90 דקות בשטח. תיקוני מיקרו-לחמה מורכבים (BGA, PMIC) עשויים לקחת 2–3 שעות. תמיד נאמר לך מראש.',
  },
  {
    q: 'האם אני צריך לעזוב את הבית?',
    a: 'לא! המעבדה מגיעה אליך — הביתה, למשרד, לחניה. כל ציוד המעבדה (כולל מיקרוסקופ, לחמה, FLIR) נמצא ברכב.',
  },
  {
    q: 'מה כלול באחריות?',
    a: '90 יום אחריות על כל רכיב שהוחלף ועל כל עבודת התיקון. אם אותה תקלה חוזרת — נתקן בחינם. האחריות לא מכסה נזקי נפילה/נוזלים חדשים לאחר התיקון.',
  },
  {
    q: 'האם אתם משתמשים בחלקים מקוריים?',
    a: 'כן — כברירת מחדל אנחנו עובדים עם חלקי OEM מקוריים. אם הלקוח מעוניין בחלופה זולה יותר, נציג את האפשרות בכתב לפני הזמנה.',
  },
  {
    q: 'מה קורה אם התיקון לא צולח?',
    a: 'לא תשלם על שעות עבודה. אם ניסינו לתקן ולא הצלחנו (תקלה בלתי-הפיכה), ייגבה אבחון בלבד (₪0–50 בהסכמה מראש). תקבל דוח מלא על מה שנמצא.',
  },
  {
    q: 'אני גר מחוץ למרכז — האם אתם מגיעים?',
    a: 'אנחנו מכסים את אזור המרכז, גוש דן, פתח תקווה, ראשון לציון ועוד. צור קשר ונבדוק יחד — לעיתים כדאי לתאם כמה לקוחות בסביבה ביום אחד.',
  },
  {
    q: 'האם ניתן לבטל/לשנות תור?',
    a: 'כן — ביטול עד שעה לפני ההגעה ב-WhatsApp. אנחנו גמישים, ללא קנסות ביטול.',
  },
  {
    q: 'מה שיטות התשלום?',
    a: 'מזומן, ביט, Paybox, או העברה בנקאית. תשלום לאחר סיום התיקון — לא לפני.',
  },
]

function FAQItem({ item, isOpen, onToggle }) {
  return (
    <div className="glass rounded-xl border border-white/10 overflow-hidden hover:border-electric/30 transition-all">
      <button
        className="w-full text-right px-5 py-4 flex items-center justify-between gap-3"
        onClick={onToggle}
        aria-expanded={isOpen}
      >
        <span className="font-semibold text-sm sm:text-base">{item.q}</span>
        <motion.div
          animate={{ rotate: isOpen ? 180 : 0 }}
          transition={{ duration: 0.25 }}
          className="flex-shrink-0"
        >
          <ChevronDown className="w-5 h-5 text-slate-400" />
        </motion.div>
      </button>

      <AnimatePresence initial={false}>
        {isOpen && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
            className="overflow-hidden"
          >
            <div className="px-5 pb-5 pt-0 text-slate-400 text-sm leading-relaxed border-t border-white/5">
              <div className="pt-3">{item.a}</div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

export default function FAQ() {
  const [openIdx, setOpenIdx] = useState(null)

  return (
    <div className="py-16 px-6 max-w-3xl mx-auto">
      <div className="text-center mb-12">
        <div className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-4 text-sm text-electric-light font-medium">
          שאלות נפוצות
        </div>
        <h2 className="text-4xl sm:text-5xl font-black mb-4 tracking-tight">
          יש לך <span className="text-gradient">שאלה?</span>
        </h2>
        <p className="text-slate-400 text-lg">כנראה כבר ענינו עליה 👇</p>
      </div>

      <div className="space-y-3 mb-12">
        {FAQS.map((item, i) => (
          <FAQItem
            key={i}
            item={item}
            isOpen={openIdx === i}
            onToggle={() => setOpenIdx(openIdx === i ? null : i)}
          />
        ))}
      </div>

      {/* Fallback CTA */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        className="glass-blue rounded-2xl p-6 border border-electric/30 text-center"
      >
        <p className="font-bold text-lg mb-1">לא מצאת תשובה?</p>
        <p className="text-slate-400 text-sm mb-4">שלח הודעה — נענה תוך דקות.</p>
        <a
          href={`https://wa.me/${WA_NUMBER}`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-2 px-6 py-3 rounded-full bg-[#25D366] hover:bg-[#1fb957] text-white font-bold transition-all"
        >
          <MessageCircle className="w-4 h-4" fill="white" />
          {WA_DISPLAY}
        </a>
      </motion.div>
    </div>
  )
}
