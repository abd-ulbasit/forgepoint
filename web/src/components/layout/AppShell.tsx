// ============================================================================
// AppShell — the persistent application chrome (sidebar + topbar).
// ============================================================================
// Rendered once around all authenticated routes via <Outlet/>. The sidebar is a
// fixed dark rail on desktop and a slide-over drawer on mobile; the topbar shows
// the current user and a logout control. NavLink handles the active-route
// styling, so the nav stays in sync with the URL automatically.

import { useState } from 'react'
import { NavLink, Outlet, useNavigate } from 'react-router-dom'
import { useAuth } from '@/auth/AuthContext'
import {
  BillingIcon,
  ChatIcon,
  DashboardIcon,
  EvalsIcon,
  ExperimentsIcon,
  LogoutIcon,
  MenuIcon,
  ModelsIcon,
  MonitoringIcon,
  NotificationsIcon,
  PipelinesIcon,
  PromptsIcon,
} from './icons'
import type { ComponentType } from 'react'

interface NavItem {
  to: string
  label: string
  Icon: ComponentType<{ className?: string }>
}

const NAV: NavItem[] = [
  { to: '/', label: 'Dashboard', Icon: DashboardIcon },
  { to: '/playground', label: 'Playground', Icon: ChatIcon },
  { to: '/prompts', label: 'Prompts', Icon: PromptsIcon },
  { to: '/evals', label: 'Evals', Icon: EvalsIcon },
  { to: '/models', label: 'Models', Icon: ModelsIcon },
  { to: '/pipelines', label: 'Pipelines', Icon: PipelinesIcon },
  { to: '/experiments', label: 'Experiments', Icon: ExperimentsIcon },
  { to: '/monitoring', label: 'Monitoring', Icon: MonitoringIcon },
  { to: '/billing', label: 'Billing', Icon: BillingIcon },
  { to: '/notifications', label: 'Notifications', Icon: NotificationsIcon },
]

function Brand() {
  return (
    <div className="flex items-center gap-2.5 px-2">
      <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-brand-600 font-bold text-white">
        F
      </div>
      <div className="leading-tight">
        <p className="text-sm font-bold text-white">Forgepoint</p>
        <p className="text-[10px] font-medium uppercase tracking-wider text-ink-400">ML Platform</p>
      </div>
    </div>
  )
}

function SidebarNav({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <nav className="flex flex-1 flex-col gap-1 px-3 py-4" aria-label="Primary">
      {NAV.map(({ to, label, Icon }) => (
        <NavLink
          key={to}
          to={to}
          end={to === '/'}
          onClick={onNavigate}
          className={({ isActive }) => `nav-link ${isActive ? 'nav-link-active' : ''}`}
        >
          <Icon className="h-5 w-5 flex-shrink-0" />
          <span>{label}</span>
        </NavLink>
      ))}
    </nav>
  )
}

function UserMenu() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()

  const initials = (user?.name || user?.email || '?')
    .split(/[\s@.]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((s) => s[0]?.toUpperCase())
    .join('')

  function handleLogout() {
    logout()
    navigate('/login', { replace: true })
  }

  return (
    <div className="flex items-center gap-3">
      <div className="hidden text-right sm:block">
        <p className="text-sm font-semibold text-ink-800">{user?.name || user?.email || 'User'}</p>
        <p className="text-xs text-ink-400">
          {user?.team ? `${user.team} · ` : ''}
          {user?.role || ''}
        </p>
      </div>
      <div
        className="flex h-9 w-9 items-center justify-center rounded-full bg-brand-100 text-sm font-semibold text-brand-700"
        title={user?.email}
        aria-hidden="true"
      >
        {initials || 'U'}
      </div>
      <button
        type="button"
        onClick={handleLogout}
        className="btn-ghost px-2.5 py-2"
        aria-label="Log out"
        title="Log out"
      >
        <LogoutIcon className="h-5 w-5" />
        <span className="hidden md:inline">Logout</span>
      </button>
    </div>
  )
}

export function AppShell() {
  const [mobileOpen, setMobileOpen] = useState(false)

  return (
    <div className="flex h-full bg-ink-100">
      {/* ---- Desktop sidebar (fixed dark rail) ---- */}
      <aside className="hidden w-64 flex-shrink-0 flex-col bg-ink-900 lg:flex">
        <div className="flex h-16 items-center border-b border-white/10 px-4">
          <Brand />
        </div>
        <SidebarNav />
        <div className="border-t border-white/10 px-4 py-3">
          <p className="text-[11px] text-ink-500">v0.1.0 · M4 console</p>
        </div>
      </aside>

      {/* ---- Mobile slide-over drawer ---- */}
      {mobileOpen && (
        <div className="fixed inset-0 z-40 lg:hidden">
          <div className="absolute inset-0 bg-ink-900/50" onClick={() => setMobileOpen(false)} aria-hidden="true" />
          <aside className="relative z-10 flex h-full w-64 animate-slide-in flex-col bg-ink-900">
            <div className="flex h-16 items-center border-b border-white/10 px-4">
              <Brand />
            </div>
            <SidebarNav onNavigate={() => setMobileOpen(false)} />
          </aside>
        </div>
      )}

      {/* ---- Main column ---- */}
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-16 flex-shrink-0 items-center justify-between gap-4 border-b border-ink-200 bg-white px-4 sm:px-6">
          <div className="flex items-center gap-3">
            <button
              type="button"
              className="btn-ghost p-2 lg:hidden"
              onClick={() => setMobileOpen(true)}
              aria-label="Open navigation menu"
            >
              <MenuIcon className="h-5 w-5" />
            </button>
          </div>
          <UserMenu />
        </header>

        <main className="flex-1 overflow-y-auto">
          <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
            <Outlet />
          </div>
        </main>
      </div>
    </div>
  )
}
