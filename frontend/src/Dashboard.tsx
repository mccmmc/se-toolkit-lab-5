import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// --- Types ---

interface ScoreBucket {
  bucket: string
  count: number
}

interface PassRateItem {
  task: string
  avg_score: number
  attempts: number
}

interface TimelineItem {
  date: string
  submissions: number
}

interface LabOption {
  id: string
  title: string
}

// --- Helpers ---

function getApiToken(): string {
  return localStorage.getItem('api_key') ?? ''
}

async function fetchApi<T>(url: string): Promise<T> {
  const token = getApiToken()
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  })
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`)
  }
  return res.json() as Promise<T>
}

const LAB_OPTIONS: LabOption[] = [
  { id: 'lab-01', title: 'Lab 01' },
  { id: 'lab-02', title: 'Lab 02' },
  { id: 'lab-03', title: 'Lab 03' },
  { id: 'lab-04', title: 'Lab 04' },
  { id: 'lab-05', title: 'Lab 05' },
]

// --- Component ---

export default function Dashboard() {
  const [lab, setLab] = useState<string>(LAB_OPTIONS[4].id)
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [passRates, setPassRates] = useState<PassRateItem[]>([])
  const [timeline, setTimeline] = useState<TimelineItem[]>([])
  const [loading, setLoading] = useState<boolean>(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)

    Promise.all([
      fetchApi<ScoreBucket[]>(`/analytics/scores?lab=${lab}`),
      fetchApi<PassRateItem[]>(`/analytics/pass-rates?lab=${lab}`),
      fetchApi<TimelineItem[]>(`/analytics/timeline?lab=${lab}`),
    ])
      .then(([s, p, t]) => {
        setScores(s)
        setPassRates(p)
        setTimeline(t)
        setLoading(false)
      })
      .catch((err: Error) => {
        setError(err.message)
        setLoading(false)
      })
  }, [lab])

  // Bar chart data for score distribution
  const barData = {
    labels: scores.map((b) => b.bucket),
    datasets: [
      {
        label: 'Number of submissions',
        data: scores.map((b) => b.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  const barOptions = {
    responsive: true,
    plugins: {
      title: {
        display: true,
        text: 'Score Distribution',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: { stepSize: 1 },
      },
    },
  }

  // Line chart data for timeline
  const lineData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions per day',
        data: timeline.map((t) => t.submissions),
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.1,
        fill: true,
      },
    ],
  }

  const lineOptions = {
    responsive: true,
    plugins: {
      title: {
        display: true,
        text: 'Submission Timeline',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: { stepSize: 1 },
      },
    },
  }

  if (loading) {
    return <p>Loading dashboard...</p>
  }

  if (error) {
    return <p>Error loading dashboard: {error}</p>
  }

  return (
    <div className="dashboard">
      <div className="dashboard-controls">
        <label htmlFor="lab-select">Select lab: </label>
        <select
          id="lab-select"
          value={lab}
          onChange={(e) => setLab(e.target.value)}
        >
          {LAB_OPTIONS.map((opt) => (
            <option key={opt.id} value={opt.id}>
              {opt.title}
            </option>
          ))}
        </select>
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <canvas id="scores-bar-chart" />
          <Bar data={barData} options={barOptions} />
        </div>

        <div className="chart-card">
          <canvas id="timeline-line-chart" />
          <Line data={lineData} options={lineOptions} />
        </div>
      </div>

      <div className="pass-rates-table">
        <h2>Pass Rates</h2>
        {passRates.length === 0 ? (
          <p>No data available.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Avg Score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRates.map((pr) => (
                <tr key={pr.task}>
                  <td>{pr.task}</td>
                  <td>{pr.avg_score.toFixed(1)}%</td>
                  <td>{pr.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
