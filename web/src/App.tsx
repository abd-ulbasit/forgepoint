// ============================================================================
// App — the route table.
// ============================================================================
// /login is PUBLIC. Everything else lives under a ProtectedRoute-guarded
// AppShell layout route, so the sidebar/topbar render once and child routes swap
// via <Outlet/>. An unknown path redirects home.

import { Navigate, Route, Routes } from 'react-router-dom'
import { ProtectedRoute } from '@/auth/ProtectedRoute'
import { AppShell } from '@/components/layout/AppShell'
import { LoginPage } from '@/pages/LoginPage'
import { DashboardPage } from '@/pages/DashboardPage'
import { ModelsPage } from '@/pages/ModelsPage'
import { ModelDetailPage } from '@/pages/ModelDetailPage'
import { PipelinesPage } from '@/pages/PipelinesPage'
import { ExecutionDetailPage } from '@/pages/ExecutionDetailPage'
import { ExperimentsPage } from '@/pages/ExperimentsPage'
import { RunDetailPage } from '@/pages/RunDetailPage'
import { MonitoringPage } from '@/pages/MonitoringPage'
import { BillingPage } from '@/pages/BillingPage'
import { NotificationsPage } from '@/pages/NotificationsPage'

export default function App() {
  return (
    <Routes>
      {/* Public */}
      <Route path="/login" element={<LoginPage />} />

      {/* Protected app shell */}
      <Route
        element={
          <ProtectedRoute>
            <AppShell />
          </ProtectedRoute>
        }
      >
        <Route path="/" element={<DashboardPage />} />
        <Route path="/models" element={<ModelsPage />} />
        <Route path="/models/:id" element={<ModelDetailPage />} />
        <Route path="/pipelines" element={<PipelinesPage />} />
        <Route path="/executions/:id" element={<ExecutionDetailPage />} />
        <Route path="/experiments" element={<ExperimentsPage />} />
        <Route path="/runs/:id" element={<RunDetailPage />} />
        <Route path="/monitoring" element={<MonitoringPage />} />
        <Route path="/billing" element={<BillingPage />} />
        <Route path="/notifications" element={<NotificationsPage />} />
      </Route>

      {/* Fallback */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
