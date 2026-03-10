import { useState, useEffect, useReducer, ChangeEvent } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  PointElement,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  PointElement
)

interface ScoreDistribution {
  bucket: string
  count: number
}

interface ScoresResponse {
  lab_id: string
  scores: ScoreDistribution[]
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface TimelineResponse {
  lab_id: string
  timeline: TimelineEntry[]
}

interface PassRateEntry {
  task_id: string
  task_name: string
  pass_rate: number
  passed: number
  total: number
}

interface PassRatesResponse {
  lab_id: string
  pass_rates: PassRateEntry[]
}

type DashboardState =
  | { status: 'idle' }
  | { status: 'loading' }
  | {
      status: 'success'
      scores: ScoresResponse | null
      timeline: TimelineResponse | null
      passRates: PassRatesResponse | null
    }
  | { status: 'error'; message: string }

type DashboardAction =
  | { type: 'fetch_start' }
  | {
      type: 'fetch_success'
      scores: ScoresResponse | null
      timeline: TimelineResponse | null
      passRates: PassRatesResponse | null
    }
  | { type: 'fetch_error'; message: string }

function dashboardReducer(state: DashboardState, action: DashboardAction): DashboardState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return {
        status: 'success',
        scores: action.scores,
        timeline: action.timeline,
        passRates: action.passRates,
      }
    case 'fetch_error':
      return { status: 'error', message: action.message }
    default:
      return state
  }
}

const LABS = [
  { id: 'lab-01', name: 'Lab 01' },
  { id: 'lab-02', name: 'Lab 02' },
  { id: 'lab-03', name: 'Lab 03' },
  { id: 'lab-04', name: 'Lab 04' },
]

function Dashboard() {
  const [token] = useState(() => localStorage.getItem('api_key') ?? '')
  const [labId, setLabId] = useState<string>('lab-04')
  const [state, dispatch] = useReducer(dashboardReducer, { status: 'idle' })

  useEffect(() => {
    if (!token) return

    dispatch({ type: 'fetch_start' })

    const controller = new AbortController()

    const fetchAll = async () => {
      try {
        const headers = {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        }

        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${labId}`, { headers, signal: controller.signal }),
          fetch(`/analytics/timeline?lab=${labId}`, { headers, signal: controller.signal }),
          fetch(`/analytics/pass-rates?lab=${labId}`, { headers, signal: controller.signal }),
        ])

        const parse = async (res: Response) => (res.ok ? (await res.json()) : null)

        dispatch({
          type: 'fetch_success',
          scores: await parse(scoresRes),
          timeline: await parse(timelineRes),
          passRates: await parse(passRatesRes),
        })
      } catch (err) {
        if (err instanceof Error) {
          dispatch({ type: 'fetch_error', message: err.message })
        }
      }
    }

    fetchAll()

    return () => controller.abort()
  }, [token, labId])

  if (!token) {
    return <div style={styles.container}>Please connect with your API key first.</div>
  }

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h1 style={styles.title}>Dashboard</h1>
        <select
          value={labId}
          onChange={(e: ChangeEvent<HTMLSelectElement>) => setLabId(e.target.value)}
          style={styles.select}
        >
          {LABS.map((lab) => (
            <option key={lab.id} value={lab.id}>
              {lab.name}
            </option>
          ))}
        </select>
      </div>

      {state.status === 'loading' && <p>Loading analytics...</p>}

      {state.status === 'error' && <p style={styles.error}>Error: {state.message}</p>}

      {state.status === 'success' && (
        <>
          <div style={styles.card}>
            <h2 style={styles.cardTitle}>Score Distribution</h2>
            {state.scores?.scores && state.scores.scores.length > 0 ? (
              <Bar
                data={{
                  labels: state.scores.scores.map((s) => s.bucket),
                  datasets: [
                    {
                      label: 'Students',
                      data: state.scores.scores.map((s) => s.count),
                      backgroundColor: 'rgba(54, 162, 235, 0.6)',
                      borderColor: 'rgba(54, 162, 235, 1)',
                      borderWidth: 1,
                    },
                  ],
                }}
                options={{ responsive: true, maintainAspectRatio: false }}
              />
            ) : (
              <p>No score data</p>
            )}
          </div>

          <div style={styles.card}>
            <h2 style={styles.cardTitle}>Submissions Over Time</h2>
            {state.timeline?.timeline && state.timeline.timeline.length > 0 ? (
              <Line
                data={{
                  labels: state.timeline.timeline.map((t) => t.date),
                  datasets: [
                    {
                      label: 'Submissions',
                      data: state.timeline.timeline.map((t) => t.submissions),
                      borderColor: 'rgba(75, 192, 192, 1)',
                      backgroundColor: 'rgba(75, 192, 192, 0.2)',
                      tension: 0.3,
                    },
                  ],
                }}
                options={{ responsive: true, maintainAspectRatio: false }}
              />
            ) : (
              <p>No timeline data</p>
            )}
          </div>

          <div style={styles.card}>
            <h2 style={styles.cardTitle}>Pass Rates</h2>
            {state.passRates?.pass_rates && state.passRates.pass_rates.length > 0 ? (
              <table>
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Passed</th>
                    <th>Total</th>
                    <th>Rate</th>
                  </tr>
                </thead>
                <tbody>
                  {state.passRates.pass_rates.map((row) => (
                    <tr key={row.task_id}>
                      <td>{row.task_name}</td>
                      <td>{row.passed}</td>
                      <td>{row.total}</td>
                      <td>{(row.pass_rate * 100).toFixed(1)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p>No pass rate data</p>
            )}
          </div>
        </>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    maxWidth: '1000px',
    margin: '0 auto',
    fontFamily: 'sans-serif',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '1.5rem',
  },
  title: {
    margin: 0,
    fontSize: '1.5rem',
  },
  select: {
    padding: '0.5rem',
    fontSize: '1rem',
    borderRadius: '4px',
    border: '1px solid #ddd',
  },
  card: {
    border: '1px solid #ddd',
    borderRadius: '8px',
    padding: '1rem',
    marginBottom: '1.5rem',
    backgroundColor: '#fff',
  },
  cardTitle: {
    margin: '0 0 1rem 0',
    fontSize: '1.25rem',
  },
  error: {
    color: '#d32f2f',
    backgroundColor: '#ffebee',
    padding: '1rem',
    borderRadius: '4px',
  },
}

export default Dashboard