import type { ReportDetail, ReportSummary } from './types'

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? '/api/v1'

export class ApiClient {
  constructor(private readonly getToken: () => string) {}

  private async request<T>(url: string, init?: RequestInit): Promise<T> {
    const token = this.getToken().trim()
    const headers = new Headers(init?.headers ?? {})
    headers.set('Content-Type', 'application/json')
    if (token) {
      headers.set('Authorization', `Bearer ${token}`)
    }

    const resp = await fetch(`${API_BASE}${url}`, {
      ...init,
      headers,
    })

    const body = await resp.json().catch(() => ({}))
    if (!resp.ok) {
      const message = (body && (body.error as string)) || `${resp.status} ${resp.statusText}`
      throw new Error(message)
    }
    return body as T
  }

  verifyAuth() {
    return this.request<{ ok: boolean }>('/auth/verify')
  }

  listReports() {
    return this.request<{ items: ReportSummary[]; total: number }>('/reports')
  }

  getReport(runID: string) {
    return this.request<ReportDetail>(`/reports/${encodeURIComponent(runID)}`)
  }
}
