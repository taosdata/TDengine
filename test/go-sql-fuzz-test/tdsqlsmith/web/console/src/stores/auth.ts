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

import { defineStore } from 'pinia'
import { ref } from 'vue'

const LS_TOKEN_KEY = 'tdsqlsmith_api_token'

export const useAuthStore = defineStore('auth', () => {
  const token = ref(localStorage.getItem(LS_TOKEN_KEY) ?? '')

  function setToken(next: string) {
    token.value = next.trim()
    if (token.value) {
      localStorage.setItem(LS_TOKEN_KEY, token.value)
    } else {
      localStorage.removeItem(LS_TOKEN_KEY)
    }
  }

  function clearToken() {
    setToken('')
  }

  return { token, setToken, clearToken }
})
