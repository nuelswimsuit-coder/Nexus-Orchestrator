import { motion } from 'framer-motion'
import { Zap, Cpu, Thermometer, Wrench, Star, CheckCircle2, MapPin, ExternalLink } from 'lucide-react'

const ADDRESS = 'רחוב סיני, פתח תקווה'
const MAPS_URL = 'https://www.google.com/maps/search/?api=1&query=רחוב+סיני+פתח+תקווה'

const fadeUp = {
  hidden: { opacity: 0, y: 30 },
  visible: (i = 0) => ({
    opacity: 1, y: 0,
    transition: { duration: 0.6, delay: i * 0.1, ease: [0.22, 1, 0.36, 1] },
  }),
}

const MILESTONES = [
  { year: '2019', title: 'ההתחלה', desc: 'תיקון הסמארטפון הראשון במוסך הביתי — iPhone 7 עם מסך שבור.' },
  { year: '2021', title: 'הצלחת ה-BGA הראשונה', desc: 'לחמת ראשון IC של MacBook M1 — הרגע שהכל השתנה.' },
  { year: '2022', title: 'המעבדה הניידת', desc: 'הבנתי שהלקוחות לא צריכים לנסוע. הציוד עבר לרכב.' },
  { year: '2024', title: 'Fix Express Labs', desc: 'מותג, אתר, ציוד מקצועי וצוות — המעבדה הניידת המתקדמת בישראל.' },
]

const VALUES = [
  { icon: Cpu, title: 'דיוק', desc: 'כל תיקון הוא נוירוכירורגיה. ±1°C בלחמה, מיקרוסקופ 40X, אפס פשרות.' },
  { icon: Thermometer, title: 'שקיפות', desc: 'מחיר מוסכם לפני כל תיקון. אם זה לא ניתן לתיקון — לא תשלם.' },
  { icon: Wrench, title: 'אחריות', desc: '90 יום אחריות על כל עבודה. אם משהו ישתבש — נתקן בלי שאלות.' },
  { icon: Star, title: 'חדשנות', desc: 'ציוד FLIR, JBC, לייזר — כשהתעשייה עומדת במקום, אנחנו זזים קדימה.' },
]

export default function About() {
  return (
    <div className="py-16 px-6 max-w-4xl mx-auto">
      {/* Hero */}
      <motion.div
        initial="hidden"
        animate="visible"
        variants={{ visible: { transition: { staggerChildren: 0.1 } } }}
        className="text-center mb-20"
      >
        <motion.div variants={fadeUp} className="inline-flex items-center gap-2 glass-blue rounded-full px-4 py-1.5 mb-5 text-sm text-electric-light font-medium">
          <Zap className="w-3.5 h-3.5" />
          הסיפור שלנו
        </motion.div>
        <motion.h2 variants={fadeUp} className="text-5xl sm:text-6xl font-black mb-6 tracking-tight leading-tight">
          מיקרוסקופ, לחמה,<br />
          <span className="text-gradient">ורצון לתקן.</span>
        </motion.h2>
        <motion.p variants={fadeUp} className="text-slate-400 text-xl max-w-2xl mx-auto leading-relaxed">
          Fix Express Labs לא התחיל כעסק — הוא התחיל מסקרנות. מה קורה בפנים כשמכשיר מת?
          אפשר להחיות אותו? התשובה, כמעט תמיד, היא כן.
        </motion.p>
      </motion.div>

      {/* Story block */}
      <motion.div
        initial={{ opacity: 0, y: 30 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.7 }}
        className="glass rounded-3xl border border-white/10 p-8 sm:p-10 mb-16 relative overflow-hidden"
      >
        <div className="absolute top-0 left-0 w-64 h-64 bg-electric/5 rounded-full blur-3xl -translate-x-1/2 -translate-y-1/2 pointer-events-none" />
        <div className="relative">
          <p className="text-lg text-slate-300 leading-relaxed mb-5">
            בשנת 2019, כשנגעתי לראשונה בלוח האם של iPhone שבור, הבנתי שיש עולם שלם
            שמרוב אנשים נסתר. מעגלים שניתן לתקן. רכיבים שאפשר להחליף.
            מכשירים שנגמר להם "הזמן" — אבל בפועל? הם רק מחכים לאדם הנכון.
          </p>
          <p className="text-lg text-slate-300 leading-relaxed mb-5">
            הבעיה עם מעבדות מסורתיות? הן גדולות, אנונימיות, ולעיתים קרובות
            מעדיפות להחליף מאשר לתקן. אצלנו — כל מכשיר מקבל אבחון מלא,
            עם ציוד תעשייתי, ואנחנו מגיעים אליך.
          </p>
          <p className="text-lg text-slate-300 leading-relaxed">
            היום, Fix Express Labs הוא מעבדה ניידת עם ציוד JBC, מצלמת FLIR תרמית,
            מיקרוסקופ 40X ויחידת לייזר — כל זה ברכב שמגיע אליך.
            כי הטכנולוגיה הכי טובה צריכה להגיע אלייך, לא להפך.
          </p>
        </div>
      </motion.div>

      {/* Timeline */}
      <div className="mb-16">
        <h3 className="text-3xl font-black text-center mb-10">
          ציר <span className="text-gradient">הזמן</span>
        </h3>
        <div className="relative">
          <div className="absolute right-6 top-0 bottom-0 w-0.5 bg-gradient-to-b from-electric via-electric/30 to-transparent" />
          <div className="space-y-8">
            {MILESTONES.map((m, i) => (
              <motion.div
                key={m.year}
                initial={{ opacity: 0, x: 20 }}
                whileInView={{ opacity: 1, x: 0 }}
                viewport={{ once: true }}
                transition={{ delay: i * 0.1, duration: 0.5 }}
                className="flex items-start gap-6 pr-14 relative"
              >
                <div className="absolute right-3 top-1 w-6 h-6 rounded-full bg-electric border-2 border-slate-950 flex items-center justify-center glow-blue">
                  <div className="w-2 h-2 rounded-full bg-white" />
                </div>
                <div className="glass rounded-xl p-4 border border-white/10 flex-1">
                  <div className="flex items-center gap-3 mb-1">
                    <span className="text-electric font-black text-lg">{m.year}</span>
                    <span className="font-bold">{m.title}</span>
                  </div>
                  <p className="text-slate-400 text-sm">{m.desc}</p>
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </div>

      {/* Values */}
      <div className="mb-16">
        <h3 className="text-3xl font-black text-center mb-10">
          הערכים <span className="text-gradient">שמנחים אותנו</span>
        </h3>
        <div className="grid sm:grid-cols-2 gap-5">
          {VALUES.map((v, i) => (
            <motion.div
              key={v.title}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.08 }}
              className="glass rounded-2xl p-6 border border-white/10 hover:border-electric/30 transition-all"
            >
              <div className="w-10 h-10 rounded-xl bg-electric/20 border border-electric/30 flex items-center justify-center mb-4">
                <v.icon className="w-5 h-5 text-electric" />
              </div>
              <h4 className="text-lg font-bold mb-2">{v.title}</h4>
              <p className="text-slate-400 text-sm leading-relaxed">{v.desc}</p>
            </motion.div>
          ))}
        </div>
      </div>

      {/* Certifications */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        className="glass-blue rounded-2xl p-8 border border-electric/30 text-center"
      >
        <h3 className="text-2xl font-bold mb-4">הציוד שמבדיל אותנו</h3>
        <div className="flex flex-wrap justify-center gap-3 mb-4">
          {['JBC T210-A Soldering', 'FLIR ONE Pro', 'Laser Repair Unit', 'DCPS Precision PSU', 'Ultrasonic Cleaner', 'Microscope 40X'].map((t) => (
            <span key={t} className="flex items-center gap-1.5 glass px-3 py-1.5 rounded-full text-sm border border-electric/20 text-electric-light">
              <CheckCircle2 className="w-3.5 h-3.5" />
              {t}
            </span>
          ))}
        </div>
        <p className="text-slate-400 text-sm">כולם נוסעים אליך.</p>
      </motion.div>

      {/* Central Hub Section */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ delay: 0.1 }}
        className="mt-8 glass rounded-2xl border border-white/10 overflow-hidden"
      >
        <div className="flex items-center gap-3 px-6 py-4 border-b border-white/10 bg-white/3">
          <div className="w-9 h-9 rounded-xl bg-electric/20 border border-electric/30 flex items-center justify-center flex-shrink-0">
            <MapPin className="w-4.5 h-4.5 text-electric" />
          </div>
          <h3 className="font-black text-lg">מרכז המעבדה הקבוע</h3>
        </div>
        <div className="px-6 py-5 grid sm:grid-cols-2 gap-6">
          <div>
            <a href={MAPS_URL} target="_blank" rel="noopener noreferrer"
              className="inline-flex items-center gap-2 text-electric hover:text-electric-light transition-colors font-bold mb-2">
              <MapPin className="w-4 h-4" />
              {ADDRESS}
              <ExternalLink className="w-3.5 h-3.5" />
            </a>
            <p className="text-slate-400 text-sm leading-relaxed">
              למרות שאנחנו מעבדה ניידת ומגיעים אליך, יש לנו מרכז מעבדה קבוע ברחוב סיני בפתח תקווה —
              המיועד לתיקוני לוח אם מורכבים שדורשים תנאי מעבדה סטטיים.
            </p>
          </div>
          <div className="space-y-2">
            <p className="text-sm font-semibold text-slate-300 mb-2">מתי מגיעים למרכז?</p>
            {[
              'תיקוני BGA מורכבים הדורשים שולחן יציב',
              'עבודת מיקרוסקופ ממושכת על לוח אם',
              'ניקוי אולטרה-סוני של מכשירים עם נזקי נוזלים',
              'שחזור נתונים מכוננים פגומים',
            ].map((item) => (
              <div key={item} className="flex items-start gap-2 text-sm text-slate-400">
                <CheckCircle2 className="w-3.5 h-3.5 text-electric mt-0.5 flex-shrink-0" />
                {item}
              </div>
            ))}
          </div>
        </div>
      </motion.div>
    </div>
  )
}
