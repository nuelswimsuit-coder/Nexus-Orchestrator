// ─── Custom high-tech SVG icons ───────────────────────────────────────────────

export function SmartphoneRepairIcon({ className = 'w-10 h-10' }) {
  return (
    <svg className={className} viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="13" y="4" width="22" height="36" rx="4" stroke="#0070f3" strokeWidth="2.2" />
      <rect x="17" y="8" width="14" height="22" rx="1.5" fill="#0070f310" stroke="#0070f3" strokeWidth="1.4" />
      <circle cx="24" cy="36" r="1.8" fill="#0070f3" />
      {/* Circuit lines */}
      <path d="M20 14h3M26 14h2" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      <path d="M20 17h8" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      <path d="M20 20h5" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      {/* Wrench overlay */}
      <path
        d="M33 6c1.5.8 2.5 2.4 2.5 4.2a4.7 4.7 0 01-4.7 4.7 4.6 4.6 0 01-1.6-.3l-5.8 5.8a.8.8 0 000 1.1l1.4 1.4a.8.8 0 001.1 0l5.8-5.8c.3.6.4 1 .4 1.6"
        stroke="#0070f3"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx="30.8" cy="10.2" r="1.4" fill="#0070f3" />
    </svg>
  )
}

export function LaptopRepairIcon({ className = 'w-10 h-10' }) {
  return (
    <svg className={className} viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="8" y="10" width="32" height="22" rx="3" stroke="#0070f3" strokeWidth="2.2" />
      <rect x="12" y="14" width="24" height="14" rx="1.5" fill="#0070f310" stroke="#0070f3" strokeWidth="1.4" />
      <path d="M4 34h40l-3 4H7L4 34z" stroke="#0070f3" strokeWidth="2" strokeLinejoin="round" fill="#0070f308" />
      {/* Circuit board lines */}
      <path d="M15 17h4M22 17h3M28 17h3" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      <path d="M15 20h6M24 20h6" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      <path d="M15 23h4M22 23h3" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      <circle cx="30" cy="20" r="1.2" fill="#0070f3" />
      <circle cx="19" cy="23" r="1" fill="#3291ff" />
      {/* Screwdriver */}
      <path d="M33 13l4-4 1.5 1.5-4 4" stroke="#0070f3" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="33.5" cy="13.5" r="1" fill="#0070f3" />
    </svg>
  )
}

export function ConsoleRepairIcon({ className = 'w-10 h-10' }) {
  return (
    <svg className={className} viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
      <rect x="6" y="16" width="36" height="20" rx="5" stroke="#0070f3" strokeWidth="2.2" fill="#0070f308" />
      {/* D-pad */}
      <rect x="13" y="22" width="3" height="8" rx="1" stroke="#3291ff" strokeWidth="1.4" />
      <rect x="10" y="25" width="9" height="3" rx="1" stroke="#3291ff" strokeWidth="1.4" />
      {/* Buttons */}
      <circle cx="33" cy="22.5" r="1.5" stroke="#3291ff" strokeWidth="1.4" />
      <circle cx="37" cy="26" r="1.5" stroke="#3291ff" strokeWidth="1.4" />
      <circle cx="33" cy="29.5" r="1.5" stroke="#3291ff" strokeWidth="1.4" />
      <circle cx="29" cy="26" r="1.5" stroke="#3291ff" strokeWidth="1.4" />
      {/* Analog sticks */}
      <circle cx="19" cy="30" r="2.5" stroke="#0070f3" strokeWidth="1.6" />
      <circle cx="27" cy="30" r="2.5" stroke="#0070f3" strokeWidth="1.6" />
      {/* Trigger */}
      <path d="M10 16c0-3 3-5 6-5h4" stroke="#0070f3" strokeWidth="2" strokeLinecap="round" />
      <path d="M38 16c0-3-3-5-6-5h-4" stroke="#0070f3" strokeWidth="2" strokeLinecap="round" />
      {/* Wrench bolt */}
      <path d="M22 8l1.5 1.5L22 11" stroke="#0070f3" strokeWidth="1.4" strokeLinecap="round" />
    </svg>
  )
}

export function MicrosolderingIcon({ className = 'w-10 h-10' }) {
  return (
    <svg className={className} viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
      {/* PCB base */}
      <rect x="6" y="28" width="36" height="14" rx="3" stroke="#0070f3" strokeWidth="2" fill="#0070f308" />
      {/* Board traces */}
      <path d="M10 33h5M18 33h4M25 33h5M33 33h5" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      <path d="M10 37h3M16 37h6M25 37h8" stroke="#3291ff" strokeWidth="1.2" strokeLinecap="round" />
      {/* Chip */}
      <rect x="18" y="30" width="12" height="8" rx="1.5" stroke="#0070f3" strokeWidth="1.6" fill="#0070f315" />
      <path d="M20 30v-2M22 30v-2M24 30v-2M26 30v-2M28 30v-2" stroke="#3291ff" strokeWidth="1" strokeLinecap="round" />
      <path d="M20 38v2M22 38v2M24 38v2M26 38v2M28 38v2" stroke="#3291ff" strokeWidth="1" strokeLinecap="round" />
      {/* Soldering iron */}
      <path d="M36 6L26 20" stroke="#0070f3" strokeWidth="2.2" strokeLinecap="round" />
      <path d="M26 20l-2 4 3-1 2-4-3 1z" fill="#0070f3" />
      {/* Heat waves */}
      <path d="M23 24c0-1 1-1.5 1-2.5" stroke="#3291ff" strokeWidth="1" strokeLinecap="round" />
      <path d="M25.5 24.5c0-1 1-1.5 1-2.5" stroke="#3291ff" strokeWidth="1" strokeLinecap="round" />
      {/* Magnifier */}
      <circle cx="38" cy="10" r="5" stroke="#0070f3" strokeWidth="1.8" />
      <path d="M34.5 13.5l-3 3" stroke="#0070f3" strokeWidth="2" strokeLinecap="round" />
      <line x1="36" y1="10" x2="40" y2="10" stroke="#3291ff" strokeWidth="1" strokeLinecap="round" />
      <line x1="38" y1="8" x2="38" y2="12" stroke="#3291ff" strokeWidth="1" strokeLinecap="round" />
    </svg>
  )
}
