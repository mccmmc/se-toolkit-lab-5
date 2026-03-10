import { useState, useEffect, useReducer, FormEvent, ChangeEvent } from 'react'
import './App.css'
import Dashboard from './Dashboard'

const STORAGE_KEY = 'api_key'

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: Item[] }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: Item[] }
  | { type: 'fetch_error'; message: string }

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', items: action.data }
    case 'fetch_error':
      return { status: 'error', message: action.message }
  }
}

function App() {
  const [token, setToken] = useState(() => localStorage.getItem(STORAGE_KEY) ?? '')
  const [draft, setDraft] = useState('')
  const [fetchState, dispatch] = useReducer(fetchReducer, { status: 'idle' })
  const [currentPage, setCurrentPage] = useState<'items' | 'dashboard'>('items')

  useEffect(() => {
    if (!token) return
    dispatch({ type: 'fetch_start' })

    fetch('/items/', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: Item[]) => dispatch({ type: 'fetch_success', data }))
      .catch((err: Error) => dispatch({ type: 'fetch_error', message: err.message }))
  }, [token])

  function handleConnect(e: FormEvent) {
    e.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return
    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
  }

  function handlePageChange(page: 'items' | 'dashboard') {
    setCurrentPage(page)
  }

  if (!token) {
    return (
      <form className="token-form" onSubmit={handleConnect}>
        <h1>Learning Management</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={draft}
          onChange={(e: ChangeEvent<HTMLInputElement>) => setDraft(e.target.value)}
        />
        <button type="submit">Connect</button>
      </form>
    )
  }

  return (
    <div>
      <div className="app-header">
        <h1>Learning Management</h1>
        <div>
          <button
            className="btn-disconnect"
            onClick={() => handlePageChange('items')}
            style={{
              marginRight: '0.5rem',
              backgroundColor: currentPage === 'items' ? '#007bff' : '#f5f5f5',
              color: currentPage === 'items' ? '#fff' : '#000',
            }}
          >
            Items
          </button>
          <button
            className="btn-disconnect"
            onClick={() => handlePageChange('dashboard')}
            style={{
              marginRight: '0.5rem',
              backgroundColor: currentPage === 'dashboard' ? '#007bff' : '#f5f5f5',
              color: currentPage === 'dashboard' ? '#fff' : '#000',
            }}
          >
            Dashboard
          </button>
          <button className="btn-disconnect" onClick={handleDisconnect}>
            Disconnect
          </button>
        </div>
      </div>

      {currentPage === 'dashboard' ? (
        <Dashboard />
      ) : (
        <>
          {fetchState.status === 'loading' && <p>Loading...</p>}
          {fetchState.status === 'error' && <p>Error: {fetchState.message}</p>}
          {fetchState.status === 'success' && (
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Title</th>
                  <th>Created at</th>
                </tr>
              </thead>
              <tbody>
                {fetchState.items.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.type}</td>
                    <td>{item.title}</td>
                    <td>{item.created_at}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}
    </div>
  )
}

export default App