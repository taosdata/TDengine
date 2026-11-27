/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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
