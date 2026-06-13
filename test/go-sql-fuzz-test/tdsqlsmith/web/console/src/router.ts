import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from './stores/auth'

const LoginView = () => import('./views/LoginView.vue')
const ReportsView = () => import('./views/ReportsView.vue')
const ReportDetailView = () => import('./views/ReportDetailView.vue')

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', name: 'login', component: LoginView, meta: { public: true } },
    { path: '/', redirect: '/reports' },
    { path: '/reports', name: 'reports', component: ReportsView },
    { path: '/reports/:runId', name: 'report-detail', component: ReportDetailView, props: true },
  ],
})

router.beforeEach((to) => {
  const auth = useAuthStore()
  const hasToken = !!auth.token.trim()
  if (!to.meta.public && !hasToken) {
    return '/login'
  }
  if (to.name === 'login' && hasToken) {
    return '/reports'
  }
  return true
})
