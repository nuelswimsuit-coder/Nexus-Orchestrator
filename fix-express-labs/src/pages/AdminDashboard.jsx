import { useState } from 'react'
import {
  Smartphone, Laptop, Gamepad2, Clock, CheckCircle2,
  AlertCircle, Wrench, ChevronDown, Search, Plus, X,
} from 'lucide-react'

const STATUS_CONFIG = {
  waiting:    { label: 'ממתין לאבחון', color: 'text-yellow-400', bg: 'bg-yellow-400/10 border-yellow-400/30', dot: 'bg-yellow-400' },
  diagnosis:  { label: 'באבחון',       color: 'text-blue-400',   bg: 'bg-blue-400/10 border-blue-400/30',   dot: 'bg-blue-400' },
  repair:     { label: 'בתיקון',       color: 'text-electric',   bg: 'bg-electric/10 border-electric/30',   dot: 'bg-electric animate-pulse' },
  quality:    { label: 'בקרת איכות',   color: 'text-purple-400', bg: 'bg-purple-400/10 border-purple-400/30', dot: 'bg-purple-400' },
  done:       { label: 'הושלם',        color: 'text-green-400',  bg: 'bg-green-400/10 border-green-400/30', dot: 'bg-green-400' },
  cancelled:  { label: 'בוטל',         color: 'text-red-400',    bg: 'bg-red-400/10 border-red-400/30',     dot: 'bg-red-400' },
}

const DEVICE_ICON = { phone: Smartphone, laptop: Laptop, console: Gamepad2 }

const INITIAL_JOBS = [
  { id: 'FEL-001', customer: 'מיכל לוי',    device: 'phone',   model: 'iPhone 14 Pro', issue: 'מסך שבור',              status: 'repair',    price: 420, created: '2026-04-14' },
  { id: 'FEL-002', customer: 'דוד כהן',     device: 'laptop',  model: 'MacBook Air M2', issue: 'לא נדלק',              status: 'diagnosis', price: 0,   created: '2026-04-14' },
  { id: 'FEL-003', customer: 'יוסי מזרחי',  device: 'console', model: 'PS5',            issue: 'בעיית HDMI',           status: 'done',      price: 350, created: '2026-04-13' },
  { id: 'FEL-004', customer: 'שרה אביב',    device: 'phone',   model: 'Samsung S24',    issue: 'טעינה לא עובדת',      status: 'waiting',   price: 0,   created: '2026-04-14' },
  { id: 'FEL-005', customer: 'אורי ברק',    device: 'laptop',  model: 'Dell XPS 15',    issue: 'התחממות יתר',          status: 'quality',   price: 280, created: '2026-04-13' },
  { id: 'FEL-006', customer: 'נועה שפירא',  device: 'phone',   model: 'Google Pixel 8', issue: 'מצלמה לא עובדת',      status: 'repair',    price: 390, created: '2026-04-12' },
]

function StatusBadge({ status }) {
  const cfg = STATUS_CONFIG[status]
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border ${cfg.bg} ${cfg.color}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${cfg.dot}`} />
      {cfg.label}
    </span>
  )
}

function StatCard({ label, value, icon: Icon, color }) {
  return (
    <div className="glass rounded-2xl p-5 border border-white/10 flex items-center gap-4">
      <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${color}`}>
        <Icon className="w-6 h-6" />
      </div>
      <div>
        <p className="text-2xl font-black">{value}</p>
        <p className="text-slate-400 text-sm">{label}</p>
      </div>
    </div>
  )
}

export default function AdminDashboard() {
  const [jobs, setJobs] = useState(INITIAL_JOBS)
  const [search, setSearch] = useState('')
  const [filterStatus, setFilterStatus] = useState('all')
  const [expandedId, setExpandedId] = useState(null)

  const filtered = jobs.filter((j) => {
    const matchSearch =
      j.customer.includes(search) ||
      j.model.toLowerCase().includes(search.toLowerCase()) ||
      j.id.includes(search)
    const matchStatus = filterStatus === 'all' || j.status === filterStatus
    return matchSearch && matchStatus
  })

  const stats = {
    active: jobs.filter((j) => !['done', 'cancelled'].includes(j.status)).length,
    done: jobs.filter((j) => j.status === 'done').length,
    revenue: jobs.filter((j) => j.status === 'done').reduce((s, j) => s + j.price, 0),
    total: jobs.length,
  }

  function cycleStatus(id) {
    const order = ['waiting', 'diagnosis', 'repair', 'quality', 'done']
    setJobs((prev) =>
      prev.map((j) => {
        if (j.id !== id) return j
        const idx = order.indexOf(j.status)
        return { ...j, status: order[(idx + 1) % order.length] }
      })
    )
  }

  return (
    <div className="min-h-screen bg-slate-950 text-white p-6 font-sans" dir="rtl">
      {/* Header */}
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-3xl font-black tracking-tight">
              לוח <span className="text-gradient">ניהול תיקונים</span>
            </h1>
            <p className="text-slate-400 text-sm mt-1">Fix Express Labs — מערכת מעקב עבודות</p>
          </div>
          <button className="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-electric hover:bg-electric-light transition-all text-white font-semibold text-sm glow-blue">
            <Plus className="w-4 h-4" />
            עבודה חדשה
          </button>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <StatCard label="עבודות פעילות"   value={stats.active}            icon={Wrench}       color="bg-electric/20 text-electric" />
          <StatCard label="הושלמו היום"      value={stats.done}              icon={CheckCircle2} color="bg-green-400/20 text-green-400" />
          <StatCard label="הכנסות (₪)"       value={`₪${stats.revenue}`}     icon={Clock}        color="bg-purple-400/20 text-purple-400" />
          <StatCard label="סה״כ עבודות"      value={stats.total}             icon={AlertCircle}  color="bg-yellow-400/20 text-yellow-400" />
        </div>

        {/* Filters */}
        <div className="flex flex-col sm:flex-row gap-3 mb-6">
          <div className="relative flex-1">
            <Search className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="חפש לקוח, מכשיר, מספר עבודה..."
              className="w-full pr-10 pl-4 py-2.5 rounded-xl glass border border-white/10 text-sm bg-transparent text-white placeholder-slate-500 focus:outline-none focus:border-electric/50"
            />
            {search && (
              <button onClick={() => setSearch('')} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-white">
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>
          <div className="flex gap-2 flex-wrap">
            {['all', ...Object.keys(STATUS_CONFIG)].map((s) => (
              <button
                key={s}
                onClick={() => setFilterStatus(s)}
                className={`px-3 py-2 rounded-xl text-xs font-semibold border transition-all ${
                  filterStatus === s
                    ? 'bg-electric text-white border-electric'
                    : 'glass border-white/10 text-slate-400 hover:border-electric/40'
                }`}
              >
                {s === 'all' ? 'הכל' : STATUS_CONFIG[s].label}
              </button>
            ))}
          </div>
        </div>

        {/* Table */}
        <div className="glass rounded-2xl border border-white/10 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-white/10 text-slate-400">
                  <th className="text-right p-4 font-semibold">מספר</th>
                  <th className="text-right p-4 font-semibold">לקוח</th>
                  <th className="text-right p-4 font-semibold">מכשיר</th>
                  <th className="text-right p-4 font-semibold">תקלה</th>
                  <th className="text-right p-4 font-semibold">סטטוס</th>
                  <th className="text-right p-4 font-semibold">מחיר</th>
                  <th className="text-right p-4 font-semibold">פעולות</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((job) => {
                  const DevIcon = DEVICE_ICON[job.device]
                  const isExpanded = expandedId === job.id
                  return (
                    <>
                      <tr
                        key={job.id}
                        className="border-b border-white/5 hover:bg-white/3 transition-colors cursor-pointer"
                        onClick={() => setExpandedId(isExpanded ? null : job.id)}
                      >
                        <td className="p-4 font-mono text-electric text-xs">{job.id}</td>
                        <td className="p-4 font-semibold">{job.customer}</td>
                        <td className="p-4">
                          <div className="flex items-center gap-2">
                            <DevIcon className="w-4 h-4 text-slate-400" />
                            <span className="text-slate-300">{job.model}</span>
                          </div>
                        </td>
                        <td className="p-4 text-slate-400">{job.issue}</td>
                        <td className="p-4"><StatusBadge status={job.status} /></td>
                        <td className="p-4 font-semibold">
                          {job.price ? `₪${job.price}` : <span className="text-slate-500">—</span>}
                        </td>
                        <td className="p-4">
                          <div className="flex items-center gap-2">
                            <button
                              onClick={(e) => { e.stopPropagation(); cycleStatus(job.id) }}
                              className="px-3 py-1.5 rounded-lg glass-blue border border-electric/30 text-electric text-xs font-semibold hover:bg-electric/20 transition-all"
                            >
                              עדכן סטטוס
                            </button>
                            <ChevronDown className={`w-4 h-4 text-slate-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                          </div>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr key={`${job.id}-expand`} className="bg-electric/5">
                          <td colSpan={7} className="px-6 py-4">
                            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
                              <div><span className="text-slate-500 block text-xs mb-0.5">תאריך פתיחה</span><span>{job.created}</span></div>
                              <div><span className="text-slate-500 block text-xs mb-0.5">סוג מכשיר</span><span className="capitalize">{job.device}</span></div>
                              <div><span className="text-slate-500 block text-xs mb-0.5">שלב נוכחי</span><StatusBadge status={job.status} /></div>
                              <div><span className="text-slate-500 block text-xs mb-0.5">מחיר סופי</span><span>{job.price ? `₪${job.price}` : 'טרם נקבע'}</span></div>
                            </div>
                          </td>
                        </tr>
                      )}
                    </>
                  )
                })}
              </tbody>
            </table>
          </div>
          {filtered.length === 0 && (
            <div className="text-center py-16 text-slate-500">
              <Search className="w-8 h-8 mx-auto mb-3 opacity-50" />
              <p>לא נמצאו עבודות תואמות</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
