import { useEffect, useRef, useState } from 'react'
import { createChart, IChartApi, ISeriesApi, LineData, LineSeries, UTCTimestamp } from 'lightweight-charts'

interface Tick {
  symbol: string
  event_ts: string
  trade_id: string
  price: string
  size: string
  side: string
}

export default function App() {
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const lineSeriesRef = useRef<ISeriesApi<'Line'> | null>(null)

  const [ticks, setTicks] = useState<Tick[]>([])
  const [latestPrice, setLatestPrice] = useState<string>('---')
  const [lastUpdated, setLastUpdated] = useState<string>('---')
  const [status, setStatus] = useState<string>('Connecting...')

  useEffect(() => {
    if (!chartContainerRef.current) return

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth || 800,
      height: 400,
      layout: {
        background: { color: '#131722' },
        textColor: '#d1d4dc',
      },
      grid: {
        vertLines: { color: '#1e222d' },
        horzLines: { color: '#1e222d' },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: true,
      },
    })

    const lineSeries = chart.addSeries(LineSeries, {
      color: '#2962FF',
      lineWidth: 2,
    })

    chartRef.current = chart
    lineSeriesRef.current = lineSeries

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({ width: chartContainerRef.current.clientWidth })
      }
    }
    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
    }
  }, [])

  // Poll query-api /v1/ticks every 1000ms
  useEffect(() => {
    let isMounted = true

    const fetchTicks = async () => {
      try {
        const res = await fetch('/v1/ticks?symbol=BTC-USD&limit=200')
        if (!res.ok) {
          if (isMounted) setStatus(`Query error: HTTP ${res.status}`)
          return
        }
        const data = await res.json()
        if (!isMounted) return

        setStatus('Live (Connected to Query-API)')
        if (data.items && data.items.length > 0) {
          // Sort ascending for chart
          const sorted = [...data.items].reverse()
          setTicks(data.items.slice(0, 10))
          setLatestPrice(sorted[sorted.length - 1].price)
          setLastUpdated(sorted[sorted.length - 1].event_ts)

          if (lineSeriesRef.current) {
            const chartData: LineData[] = sorted.map((t: Tick) => {
              const epoch = Math.floor(new Date(t.event_ts).getTime() / 1000) as UTCTimestamp
              return {
                time: epoch,
                value: parseFloat(t.price),
              }
            })

            // Filter strictly ascending timestamps for lightweight-charts
            const uniqueAscData: LineData[] = []
            let lastT = 0
            for (const item of chartData) {
              const itemTimeNum = Number(item.time)
              if (itemTimeNum > lastT) {
                uniqueAscData.push(item)
                lastT = itemTimeNum
              }
            }

            if (uniqueAscData.length > 0) {
              lineSeriesRef.current.setData(uniqueAscData)
            }
          }
        }
      } catch (err) {
        if (isMounted) setStatus('Reconnecting to Query-API...')
      }
    }

    fetchTicks()
    const timer = setInterval(fetchTicks, 1000)
    return () => {
      isMounted = false
      clearInterval(timer)
    }
  }, [])

  return (
    <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <div>
          <h1 style={{ margin: 0, fontSize: '24px', color: '#f0f3f6' }}>Crypto Intelligence Platform (CIP)</h1>
          <p style={{ margin: '4px 0 0 0', color: '#787b86', fontSize: '14px' }}>Milestone M0: Walking Skeleton Dashboard</p>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ fontSize: '12px', color: status.startsWith('Live') ? '#089981' : '#f23645' }}>
            ● {status}
          </div>
          <div style={{ fontSize: '11px', color: '#787b86', marginTop: '2px' }}>Symbol: BTC-USD</div>
        </div>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '16px', marginBottom: '20px' }}>
        <div style={{ background: '#131722', padding: '16px', borderRadius: '8px', border: '1px solid #1e222d' }}>
          <div style={{ fontSize: '12px', color: '#787b86' }}>BTC-USD Latest Price</div>
          <div style={{ fontSize: '28px', fontWeight: 'bold', color: '#f0f3f6', marginTop: '4px' }}>
            ${latestPrice !== '---' ? parseFloat(latestPrice).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '---'}
          </div>
        </div>
        <div style={{ background: '#131722', padding: '16px', borderRadius: '8px', border: '1px solid #1e222d' }}>
          <div style={{ fontSize: '12px', color: '#787b86' }}>Last Tick Timestamp</div>
          <div style={{ fontSize: '14px', color: '#d1d4dc', marginTop: '12px' }}>{lastUpdated}</div>
        </div>
        <div style={{ background: '#131722', padding: '16px', borderRadius: '8px', border: '1px solid #1e222d' }}>
          <div style={{ fontSize: '12px', color: '#787b86' }}>Pipeline SLO Target</div>
          <div style={{ fontSize: '14px', color: '#089981', marginTop: '12px' }}>p95 &le; 2.0s (Tick &rarr; UI)</div>
        </div>
      </div>

      <div style={{ background: '#131722', padding: '16px', borderRadius: '8px', border: '1px solid #1e222d' }}>
        <h2 style={{ fontSize: '16px', margin: '0 0 16px 0', color: '#d1d4dc' }}>BTC-USD Real-Time Price Line Chart</h2>
        <div ref={chartContainerRef} style={{ width: '100%', height: '400px' }} />
      </div>

      <div style={{ marginTop: '24px', background: '#131722', padding: '16px', borderRadius: '8px', border: '1px solid #1e222d' }}>
        <h3 style={{ fontSize: '14px', margin: '0 0 12px 0', color: '#d1d4dc' }}>Recent Executed Matches</h3>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'left' }}>
          <thead>
            <tr style={{ color: '#787b86', borderBottom: '1px solid #1e222d' }}>
              <th style={{ padding: '8px' }}>Trade ID</th>
              <th style={{ padding: '8px' }}>Side</th>
              <th style={{ padding: '8px' }}>Price</th>
              <th style={{ padding: '8px' }}>Size</th>
              <th style={{ padding: '8px' }}>Event Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {ticks.map((t) => (
              <tr key={t.trade_id} style={{ borderBottom: '1px solid #1e222d' }}>
                <td style={{ padding: '8px', color: '#d1d4dc' }}>{t.trade_id}</td>
                <td style={{ padding: '8px', color: t.side === 'BUY' ? '#089981' : '#f23645' }}>{t.side}</td>
                <td style={{ padding: '8px', color: '#d1d4dc' }}>${parseFloat(t.price).toFixed(2)}</td>
                <td style={{ padding: '8px', color: '#d1d4dc' }}>{parseFloat(t.size).toFixed(4)}</td>
                <td style={{ padding: '8px', color: '#787b86' }}>{t.event_ts}</td>
              </tr>
            ))}
            {ticks.length === 0 && (
              <tr>
                <td colSpan={5} style={{ padding: '16px', textAlign: 'center', color: '#787b86' }}>
                  No ticks received yet. Awaiting market trades...
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
