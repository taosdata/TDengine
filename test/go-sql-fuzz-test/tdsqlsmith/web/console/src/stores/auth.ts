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
