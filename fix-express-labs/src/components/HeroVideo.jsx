import { useEffect, useRef, useState } from 'react'
import video1 from '../assets/videos/video1.mp4'
import video2 from '../assets/videos/video2.mp4'

/**
 * Cross-fades between two background videos on a configurable interval.
 * Both videos are always mounted (for seamless preload); opacity is toggled
 * with a CSS transition so the switch is imperceptible.
 */
export default function HeroVideo({ children, intervalMs = 7000, fadeDurationMs = 1200 }) {
  const [active, setActive] = useState(0)
  const v1Ref = useRef(null)
  const v2Ref = useRef(null)

  // Ensure both videos start playing as soon as they're ready
  useEffect(() => {
    [v1Ref, v2Ref].forEach((ref) => {
      if (ref.current) {
        ref.current.play().catch(() => {})
      }
    })
  }, [])

  // Alternate active video on interval
  useEffect(() => {
    const id = setInterval(() => setActive((a) => (a === 0 ? 1 : 0)), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])

  const transitionStyle = `opacity ${fadeDurationMs}ms ease-in-out`

  return (
    <div className="relative w-full h-full overflow-hidden">
      {/* Video 1 */}
      <video
        ref={v1Ref}
        src={video1}
        autoPlay
        muted
        loop
        playsInline
        className="absolute inset-0 w-full h-full object-cover"
        style={{ opacity: active === 0 ? 1 : 0, transition: transitionStyle, zIndex: 1 }}
      />
      {/* Video 2 */}
      <video
        ref={v2Ref}
        src={video2}
        autoPlay
        muted
        loop
        playsInline
        className="absolute inset-0 w-full h-full object-cover"
        style={{ opacity: active === 1 ? 1 : 0, transition: transitionStyle, zIndex: 1 }}
      />
      {/* Dark overlay */}
      <div
        className="absolute inset-0"
        style={{
          zIndex: 2,
          background:
            'linear-gradient(to bottom, rgba(2,6,23,0.72) 0%, rgba(2,6,23,0.55) 50%, rgba(2,6,23,0.85) 100%)',
        }}
      />
      {/* Content */}
      <div className="relative" style={{ zIndex: 3 }}>
        {children}
      </div>
    </div>
  )
}
