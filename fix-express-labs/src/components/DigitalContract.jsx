import { useRef, useState } from 'react'
import SignatureCanvas from 'react-signature-canvas'
import { motion, AnimatePresence } from 'framer-motion'
import { FileText, Trash2, CheckCircle2, Lock, Download, X } from 'lucide-react'

const OWNER = 'ירין הלילי'
const ADDRESS = 'רחוב סיני, פתח תקווה'
const TODAY = new Date().toLocaleDateString('he-IL')

function Toast({ show, onClose }) {
  return (
    <AnimatePresence>
      {show && (
        <motion.div
          initial={{ opacity: 0, y: 30, scale: 0.9 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          exit={{ opacity: 0, y: 20, scale: 0.9 }}
          className="fixed bottom-8 right-1/2 translate-x-1/2 z-[9999] flex items-center gap-3 glass-blue border border-electric/40 rounded-2xl px-6 py-4 shadow-2xl shadow-electric/20"
        >
          <CheckCircle2 className="w-6 h-6 text-green-400" />
          <div>
            <p className="font-bold text-white">ההסכם נחתם בהצלחה!</p>
            <p className="text-slate-400 text-xs">החתימה נשמרה ונרשמה.</p>
          </div>
          <button onClick={onClose} className="text-slate-400 hover:text-white mr-2"><X className="w-4 h-4" /></button>
        </motion.div>
      )}
    </AnimatePresence>
  )
}

export default function DigitalContract({
  customerName = '',
  deviceType = '',
  model = '',
  issue = '',
  price = '',
  onSigned,
}) {
  const sigRef = useRef(null)
  const [signed, setSigned] = useState(false)
  const [agreed, setAgreed] = useState(false)
  const [showToast, setShowToast] = useState(false)
  const [sigData, setSigData] = useState(null)
  const [sigEmpty, setSigEmpty] = useState(true)

  function handleClear() {
    sigRef.current?.clear()
    setSigEmpty(true)
    setSigned(false)
  }

  function handleSave() {
    if (!agreed) return
    if (!sigRef.current || sigRef.current.isEmpty()) {
      alert('נא לחתום לפני שמירה.')
      return
    }
    const data = sigRef.current.toDataURL('image/png')
    setSigData(data)
    setSigned(true)
    setShowToast(true)
    setTimeout(() => setShowToast(false), 4000)
    onSigned?.({ signatureDataUrl: data, agreedAt: new Date().toISOString() })
  }

  function handleDownload() {
    if (!sigData) return
    const a = document.createElement('a')
    a.href = sigData
    a.download = `fix-express-labs-signature-${Date.now()}.png`
    a.click()
  }

  return (
    <div className="glass rounded-3xl border border-white/10 overflow-hidden">
      {/* Header */}
      <div className="px-6 py-5 border-b border-white/10 flex items-center gap-3">
        <div className="w-10 h-10 rounded-xl bg-electric/20 border border-electric/30 flex items-center justify-center">
          <FileText className="w-5 h-5 text-electric" />
        </div>
        <div>
          <h3 className="font-black text-lg">הסכם תיקון דיגיטלי</h3>
          <p className="text-slate-400 text-xs">Fix Express Labs | {OWNER}</p>
        </div>
      </div>

      {/* Contract body */}
      <div className="p-6 space-y-4 text-sm leading-relaxed text-slate-300 max-h-64 overflow-y-auto">
        <p className="font-semibold text-white">הסכם שירות תיקון — Fix Express Labs</p>
        <p><strong>בעלים:</strong> {OWNER} | <strong>כתובת:</strong> {ADDRESS}</p>
        {customerName && <p><strong>לקוח:</strong> {customerName}</p>}
        {deviceType && model && <p><strong>מכשיר:</strong> {deviceType} {model}{issue ? ` — ${issue}` : ''}</p>}
        {price && <p><strong>מחיר מוסכם:</strong> {price}</p>}
        <p><strong>תאריך:</strong> {TODAY}</p>

        <hr className="border-white/10" />

        <p>הלקוח מאשר כי:</p>
        <ol className="list-decimal list-inside space-y-2 text-slate-400">
          <li><strong className="text-white">נתונים ומידע:</strong> Fix Express Labs אינה אחראית לאובדן מידע שמור במכשיר. הלקוח אחראי לגיבוי נתוניו לפני מסירת המכשיר לתיקון.</li>
          <li><strong className="text-white">נזק קיים מראש:</strong> כל נזק שתועד בטופס זה היה קיים לפני הגעת הטכנאי.</li>
          <li><strong className="text-white">אחריות 90 יום:</strong> האחריות חלה על הרכיב שהוחלף ועל עבודת התיקון בלבד. אינה מכסה נפילות, נוזלים, או נזק חיצוני חדש.</li>
          <li><strong className="text-white">רכיבים מקוריים:</strong> Fix Express Labs משתמשת ברכיבי OEM מקוריים אלא אם הוסכם אחרת בכתב.</li>
          <li><strong className="text-white">תשלום:</strong> תשלום יתבצע לאחר השלמת התיקון ולא לפניו.</li>
          <li><strong className="text-white">הגבלת אחריות:</strong> אחריות Fix Express Labs לא תעלה על סכום התיקון ששולם בפועל.</li>
        </ol>
      </div>

      {/* Agree checkbox */}
      <div className="px-6 pb-4">
        <label className="flex items-center gap-3 cursor-pointer select-none">
          <div
            onClick={() => !signed && setAgreed((v) => !v)}
            className={`w-5 h-5 rounded border-2 flex items-center justify-center flex-shrink-0 transition-all ${
              agreed ? 'bg-electric border-electric' : 'border-white/30'
            } ${signed ? 'opacity-60' : 'cursor-pointer'}`}
          >
            {agreed && <CheckCircle2 className="w-3.5 h-3.5 text-white" />}
          </div>
          <span className="text-sm text-slate-300">קראתי ואני מסכים/ה לכל תנאי ההסכם לעיל</span>
        </label>
      </div>

      {/* Signature pad */}
      <div className="px-6 pb-6">
        <div className="flex items-center justify-between mb-2">
          <p className="text-xs text-slate-400 font-semibold">חתימה דיגיטלית</p>
          {!signed && (
            <button onClick={handleClear} className="flex items-center gap-1 text-xs text-slate-500 hover:text-red-400 transition-colors">
              <Trash2 className="w-3.5 h-3.5" />נקה
            </button>
          )}
        </div>

        <div className={`rounded-xl overflow-hidden border-2 transition-all ${
          signed ? 'border-green-500/50' : agreed ? 'border-electric/40' : 'border-white/10'
        }`}>
          {signed ? (
            <div className="relative bg-white/5 h-32 flex items-center justify-center">
              <img src={sigData} alt="חתימה" className="max-h-28 max-w-full object-contain" />
              <div className="absolute top-2 left-2 flex items-center gap-1 glass-blue border border-green-500/30 rounded-full px-2 py-0.5 text-xs text-green-400">
                <Lock className="w-3 h-3" />נעול
              </div>
            </div>
          ) : (
            <SignatureCanvas
              ref={sigRef}
              penColor="#0070f3"
              onEnd={() => setSigEmpty(false)}
              canvasProps={{
                className: 'w-full',
                height: 120,
                style: { background: 'rgba(255,255,255,0.03)' },
              }}
            />
          )}
        </div>

        {!signed && !sigEmpty && (
          <p className="text-xs text-electric mt-1 text-center">חתימה זוהתה ✓</p>
        )}
        {!signed && sigEmpty && (
          <p className="text-xs text-slate-600 mt-1 text-center">חתום כאן עם האצבע או העכבר</p>
        )}

        <div className="flex gap-3 mt-4">
          {signed ? (
            <button onClick={handleDownload}
              className="flex-1 flex items-center justify-center gap-2 py-3 rounded-xl glass border border-green-500/30 text-green-400 font-semibold text-sm hover:bg-green-500/10 transition-all">
              <Download className="w-4 h-4" />הורד חתימה
            </button>
          ) : (
            <button
              disabled={!agreed || sigEmpty}
              onClick={handleSave}
              className={`flex-1 flex items-center justify-center gap-2 py-3 rounded-xl font-bold text-sm transition-all ${
                agreed && !sigEmpty
                  ? 'bg-electric hover:bg-electric-light text-white glow-blue'
                  : 'bg-white/5 text-slate-600 cursor-not-allowed'
              }`}
            >
              <Lock className="w-4 h-4" />שמור ואשר הסכם
            </button>
          )}
        </div>
      </div>

      <Toast show={showToast} onClose={() => setShowToast(false)} />
    </div>
  )
}
