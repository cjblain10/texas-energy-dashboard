import { useState, useEffect } from 'react'
import {
  PieChart, Pie, Cell,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend
} from 'recharts'
import { format, parseISO } from 'date-fns'

// Color palette - clean, professional
const COLORS = {
  solar: '#f59e0b',
  battery: '#8b5cf6',
  wind: '#3b82f6',
  gas: '#ef4444',
  other: '#6b7280',
  primary: '#0f172a',
  secondary: '#64748b',
  accent: '#2563eb',
  background: '#f8fafc',
  card: '#ffffff',
  border: '#e2e8f0'
}

const FUEL_COLORS = {
  'Solar': COLORS.solar,
  'Battery Storage': COLORS.battery,
  'Battery': COLORS.battery,
  'Wind': COLORS.wind,
  'Gas': COLORS.gas,
  'Natural Gas': COLORS.gas,
  'Other': COLORS.other
}

const BASIN_COLORS = {
  'Permian': '#0ea5e9',
  'Eagle Ford': '#22c55e',
  'Haynesville': '#f97316',
  'Anadarko': '#a855f7',
  'Barnett': '#ec4899',
  'Granite Wash': '#14b8a6',
  'Other': '#6b7280'
}

function App() {
  const [ercotData, setErcotData] = useState(null)
  const [permitsData, setPermitsData] = useState(null)
  const [enforcementData, setEnforcementData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    async function fetchData() {
      try {
        const [ercotRes, permitsRes, enforcementRes] = await Promise.all([
          fetch('/data/ercot_queue.json'),
          fetch('/data/rrc_permits.json'),
          fetch('/data/rrc_enforcement.json')
        ])

        if (!ercotRes.ok || !permitsRes.ok || !enforcementRes.ok) {
          throw new Error('Failed to fetch data')
        }

        const [ercot, permits, enforcement] = await Promise.all([
          ercotRes.json(),
          permitsRes.json(),
          enforcementRes.json()
        ])

        setErcotData(ercot)
        setPermitsData(permits)
        setEnforcementData(enforcement)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="text-slate-500">Loading dashboard...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="text-red-500">Error: {error}</div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Header */}
      <header className="bg-white border-b border-slate-200 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-2xl font-semibold text-slate-900">
                Texas Energy Regulatory Pulse
              </h1>
              <p className="text-sm text-slate-500 mt-1">
                Tracking drilling permits, grid interconnections, and enforcement activity
              </p>
            </div>
            <div className="text-right text-xs text-slate-400">
              <div>Last updated</div>
              <div className="font-medium">
                {ercotData?.updated_at
                  ? format(parseISO(ercotData.updated_at), 'MMM d, yyyy h:mm a')
                  : 'Unknown'}
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Key Metrics Row */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          <MetricCard
            label="ERCOT Queue"
            value={`${ercotData?.total_capacity_gw || 0} GW`}
            subtext={`${ercotData?.total_projects || 0} projects`}
          />
          <MetricCard
            label="Permits (30d)"
            value={permitsData?.total_permits_30d || 0}
            subtext="drilling permits filed"
          />
          <MetricCard
            label="Active Basins"
            value={Object.keys(permitsData?.by_basin || {}).length}
            subtext="with permit activity"
          />
          <MetricCard
            label="Enforcement"
            value={enforcementData?.total_recent || 0}
            subtext="recent actions"
          />
        </div>

        {/* Charts Grid */}
        <div className="grid md:grid-cols-2 gap-6 mb-8">
          {/* ERCOT Queue by Fuel Type */}
          <div className="bg-white rounded-lg border border-slate-200 p-6">
            <h2 className="text-lg font-medium text-slate-900 mb-4">
              ERCOT Interconnection Queue by Fuel Type
            </h2>
            <div className="h-72">
              <ERCOTPieChart data={ercotData?.by_fuel_type} />
            </div>
          </div>

          {/* Permit Activity by Basin */}
          <div className="bg-white rounded-lg border border-slate-200 p-6">
            <h2 className="text-lg font-medium text-slate-900 mb-4">
              Drilling Permits by Basin (30 Days)
            </h2>
            <div className="h-72">
              <PermitBarChart data={permitsData?.by_basin} />
            </div>
          </div>
        </div>

        {/* Bottom Row */}
        <div className="grid md:grid-cols-2 gap-6">
          {/* Enforcement Headlines */}
          <div className="bg-white rounded-lg border border-slate-200 p-6">
            <h2 className="text-lg font-medium text-slate-900 mb-4">
              Recent RRC Enforcement Actions
            </h2>
            <EnforcementList items={enforcementData?.items} />
          </div>

          {/* County Permit Velocity */}
          <div className="bg-white rounded-lg border border-slate-200 p-6">
            <h2 className="text-lg font-medium text-slate-900 mb-4">
              Top Counties by Permit Activity
            </h2>
            <CountyVelocityTable data={permitsData?.by_county} />
          </div>
        </div>

        {/* Footer */}
        <footer className="mt-12 pt-8 border-t border-slate-200 text-center text-xs text-slate-400">
          <p>
            Data sources: Texas Railroad Commission, ERCOT
          </p>
          <p className="mt-1">
            © {new Date().getFullYear()} Local Insights AI • Proof of Concept
          </p>
        </footer>
      </main>
    </div>
  )
}

// Metric Card Component
function MetricCard({ label, value, subtext }) {
  return (
    <div className="bg-white rounded-lg border border-slate-200 p-4">
      <div className="text-xs font-medium text-slate-500 uppercase tracking-wide">
        {label}
      </div>
      <div className="mt-2 text-2xl font-semibold text-slate-900">
        {value}
      </div>
      <div className="text-xs text-slate-400 mt-1">
        {subtext}
      </div>
    </div>
  )
}

// ERCOT Pie Chart Component
function ERCOTPieChart({ data }) {
  if (!data || Object.keys(data).length === 0) {
    return <div className="flex items-center justify-center h-full text-slate-400">No data</div>
  }

  const chartData = Object.entries(data).map(([name, info]) => ({
    name,
    value: info.capacity_gw || info.capacity_mw / 1000 || info.count || 0,
    count: info.count || 0,
    raw: info.capacity_mw || 0
  })).sort((a, b) => b.value - a.value)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <PieChart>
        <Pie
          data={chartData}
          cx="50%"
          cy="50%"
          innerRadius={50}
          outerRadius={90}
          paddingAngle={2}
          dataKey="value"
          label={({ name, value }) => `${name}: ${value.toFixed(1)} GW`}
          labelLine={false}
        >
          {chartData.map((entry, index) => (
            <Cell
              key={`cell-${index}`}
              fill={FUEL_COLORS[entry.name] || COLORS.other}
            />
          ))}
        </Pie>
        <Tooltip
          formatter={(value, name, props) => [
            `${value.toFixed(1)} GW (${props.payload.count} projects)`,
            name
          ]}
        />
        <Legend />
      </PieChart>
    </ResponsiveContainer>
  )
}

// Permit Bar Chart Component
function PermitBarChart({ data }) {
  if (!data || Object.keys(data).length === 0) {
    return <div className="flex items-center justify-center h-full text-slate-400">No data</div>
  }

  const chartData = Object.entries(data)
    .map(([name, count]) => ({ name, count: typeof count === 'number' ? count : 0 }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={chartData} layout="vertical" margin={{ left: 10, right: 20 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis type="number" tick={{ fontSize: 12 }} />
        <YAxis
          type="category"
          dataKey="name"
          tick={{ fontSize: 12 }}
          width={80}
        />
        <Tooltip />
        <Bar
          dataKey="count"
          fill={COLORS.accent}
          radius={[0, 4, 4, 0]}
        >
          {chartData.map((entry, index) => (
            <Cell
              key={`cell-${index}`}
              fill={BASIN_COLORS[entry.name] || COLORS.accent}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

// Enforcement List Component
function EnforcementList({ items }) {
  if (!items || items.length === 0) {
    return <div className="text-slate-400 text-sm">No recent enforcement actions</div>
  }

  return (
    <ul className="space-y-3">
      {items.slice(0, 5).map((item, index) => (
        <li key={index} className="border-b border-slate-100 pb-3 last:border-0">
          <div className="flex justify-between items-start">
            <div className="flex-1">
              <p className="text-sm text-slate-700 leading-snug">
                {item.headline}
              </p>
              {item.docket_number && (
                <p className="text-xs text-slate-400 mt-1">
                  Docket: {item.docket_number}
                </p>
              )}
            </div>
            <div className="ml-4 flex-shrink-0 text-right">
              <span className={`inline-block px-2 py-0.5 text-xs rounded-full ${
                item.status === 'Pending' ? 'bg-yellow-100 text-yellow-700' :
                item.status === 'Resolved' ? 'bg-green-100 text-green-700' :
                'bg-slate-100 text-slate-600'
              }`}>
                {item.status || 'Unknown'}
              </span>
              <p className="text-xs text-slate-400 mt-1">
                {item.date ? format(parseISO(item.date), 'MMM d') : ''}
              </p>
            </div>
          </div>
        </li>
      ))}
    </ul>
  )
}

// County Velocity Table Component
function CountyVelocityTable({ data }) {
  if (!data || Object.keys(data).length === 0) {
    return <div className="text-slate-400 text-sm">No data</div>
  }

  const sortedData = Object.entries(data)
    .map(([county, count]) => ({ county, count: typeof count === 'number' ? count : 0 }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10)

  const maxCount = sortedData[0]?.count || 1

  return (
    <div className="space-y-2">
      {sortedData.map((item, index) => (
        <div key={index} className="flex items-center">
          <div className="w-24 text-sm text-slate-600 truncate">
            {item.county}
          </div>
          <div className="flex-1 mx-3">
            <div
              className="h-5 bg-blue-500 rounded-sm transition-all"
              style={{
                width: `${(item.count / maxCount) * 100}%`,
                backgroundColor: `rgba(37, 99, 235, ${0.4 + (item.count / maxCount) * 0.6})`
              }}
            />
          </div>
          <div className="w-8 text-sm text-slate-500 text-right">
            {item.count}
          </div>
        </div>
      ))}
    </div>
  )
}

export default App
