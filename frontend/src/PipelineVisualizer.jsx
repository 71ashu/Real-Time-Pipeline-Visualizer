import { useState, useEffect, useRef, useCallback } from "react";
import * as d3 from "d3";

let packetCounter = 0;
const NODES_DEF = [
  { id: "source-1", name: "Kafka Stream", type: "source", x: 80, y: 70 },
  { id: "source-2", name: "Event Bus", type: "source", x: 80, y: 270 },
  { id: "transform-1", name: "Parser", type: "transform", x: 300, y: 20 },
  { id: "transform-2", name: "Enricher", type: "transform", x: 300, y: 170 },
  { id: "transform-3", name: "Validator", type: "transform", x: 300, y: 320 },
  { id: "aggregate-1", name: "Aggregator", type: "aggregate", x: 520, y: 90 },
  { id: "aggregate-2", name: "Joiner", type: "aggregate", x: 520, y: 250 },
  { id: "sink-1", name: "PostgreSQL", type: "sink", x: 740, y: 20 },
  { id: "sink-2", name: "Redis Cache", type: "sink", x: 740, y: 170 },
  { id: "sink-3", name: "S3 Archive", type: "sink", x: 740, y: 320 },
];

const EDGES_DEF = [
  { id: "e1", source: "source-1", target: "transform-1" },
  { id: "e2", source: "source-1", target: "transform-2" },
  { id: "e3", source: "source-2", target: "transform-2" },
  { id: "e4", source: "source-2", target: "transform-3" },
  { id: "e5", source: "transform-1", target: "aggregate-1" },
  { id: "e6", source: "transform-2", target: "aggregate-1" },
  { id: "e7", source: "transform-2", target: "aggregate-2" },
  { id: "e8", source: "transform-3", target: "aggregate-2" },
  { id: "e9", source: "aggregate-1", target: "sink-1" },
  { id: "e10", source: "aggregate-1", target: "sink-2" },
  { id: "e11", source: "aggregate-2", target: "sink-2" },
  { id: "e12", source: "aggregate-2", target: "sink-3" },
];

let mockPackets = [];
let totalErrors = 0;
const startTime = Date.now();

function simulatePipeline() {
  const t = Date.now() / 1000;
  const nodes = NODES_DEF.map((n, i) => {
    const throughput = 800 + 400 * Math.sin(t * 0.3 + i * 0.7) + Math.random() * 200;
    const latency = 5 + 15 * Math.abs(Math.sin(t * 0.2 + i * 0.5)) + Math.random() * 5;
    const errorRate = Math.max(0, 0.5 * Math.sin(t * 0.1 + i * 1.2) + 0.3 + Math.random() * 0.3);
    const queueDepth = Math.floor(50 + 30 * Math.sin(t * 0.4 + i * 0.3) + Math.random() * 20);
    let status = "active";
    if (errorRate > 1.2) {
      status = "warning";
      totalErrors++;
    }
    if (errorRate > 1.8) {
      status = "error";
    }
    return { ...n, throughput, latency, errorRate, queueDepth, status };
  });

  const edges = EDGES_DEF.map((e, i) => ({
    ...e,
    flowRate: 500 + 300 * Math.sin(t * 0.25 + i * 0.6) + Math.random() * 150,
    active: true,
  }));

  if (Math.random() < 0.4) {
    const edgeIdx = Math.floor(Math.random() * EDGES_DEF.length);
    const ptype = Math.random() < 0.1 ? "error" : Math.random() < 0.2 ? "control" : "data";
    mockPackets.push({
      id: `pkt-${++packetCounter}`,
      edgeId: EDGES_DEF[edgeIdx].id,
      progress: 0,
      size: 0.3 + Math.random() * 0.7,
      packetType: ptype,
    });
  }

  mockPackets = mockPackets
    .map((p) => ({ ...p, progress: p.progress + 0.03 + Math.random() * 0.02 }))
    .filter((p) => p.progress < 1.0)
    .slice(-60);

  const totalFlow = edges.reduce((s, e) => s + e.flowRate, 0);
  return {
    nodes,
    edges,
    packets: mockPackets,
    timestamp: Date.now(),
    totalFlow,
    throughput: totalFlow / edges.length,
    errors: totalErrors,
    uptime: (Date.now() - startTime) / 1000,
  };
}

const NODE_W = 120;
const NODE_H = 44;
const TYPE_COLORS = {
  source: { base: "#00d4aa", glow: "#00d4aa44", border: "#00ffc8", darkText: "#007a61", darkBorder: "#00a882" },
  transform: { base: "#f59e0b", glow: "#f59e0b44", border: "#fbbf24", darkText: "#92400e", darkBorder: "#b45309" },
  aggregate: { base: "#8b5cf6", glow: "#8b5cf644", border: "#a78bfa", darkText: "#4c1d95", darkBorder: "#6d28d9" },
  sink: { base: "#06b6d4", glow: "#06b6d444", border: "#67e8f9", darkText: "#164e63", darkBorder: "#0891b2" },
};
const STATUS_BADGE = { active: "#00d4aa", warning: "#f59e0b", error: "#ef4444" };
const PACKET_COLORS = { data: "#00d4aa", error: "#ef4444", control: "#8b5cf6" };

const THEMES = {
  dark: {
    bg: "#060810",
    headerBg: "rgba(6,8,16,0.9)",
    headerBorder: "#ffffff0a",
    metricsBorder: "#ffffff05",
    cardBg: "rgba(10,12,20,0.85)",
    svgBg: "rgba(4,6,12,0.7)",
    svgBorder: "#ffffff08",
    text: "#ccd",
    labelColor: "#556",
    titleColor: "#00d4aa",
    toggleBg: "#1a1f2e",
    toggleBorder: "#ffffff1a",
    toggleColor: "#ccd",
  },
  light: {
    bg: "#ffffff",
    headerBg: "rgba(255,255,255,0.95)",
    headerBorder: "#00000010",
    metricsBorder: "#00000008",
    cardBg: "rgba(255,255,255,0.9)",
    svgBg: "rgba(250,250,252,0.9)",
    svgBorder: "#00000010",
    text: "#1a1d2e",
    labelColor: "#888",
    titleColor: "#008f72",
    toggleBg: "#f0f0f0",
    toggleBorder: "#00000018",
    toggleColor: "#1a1d2e",
  },
};

function getEdgePoints(src, tgt) {
  const sx = src.x + NODE_W / 2;
  const sy = src.y + NODE_H / 2;
  const tx = tgt.x;
  const ty = tgt.y + NODE_H / 2;
  const mx = (sx + tx) / 2;
  return { sx, sy, tx, ty, mx };
}

function edgePath(src, tgt) {
  const { sx, sy, tx, ty, mx } = getEdgePoints(src, tgt);
  return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
}

function packetPos(src, tgt, progress) {
  const { sx, sy, tx, ty, mx } = getEdgePoints(src, tgt);
  const t = progress;
  const t1 = 1 - t;
  const x = t1 * t1 * t1 * sx + 3 * t1 * t1 * t * mx + 3 * t1 * t * t * mx + t * t * t * tx;
  const y = t1 * t1 * t1 * sy + 3 * t1 * t1 * t * sy + 3 * t1 * t * t * ty + t * t * t * ty;
  return { x, y };
}

function Sparkline({ data, color, width = 80, height = 24 }) {
  const ref = useRef();
  useEffect(() => {
    if (!ref.current || data.length < 2) return;
    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    const x = d3.scaleLinear().domain([0, data.length - 1]).range([0, width]);
    const y = d3.scaleLinear().domain([d3.min(data), d3.max(data)]).range([height - 2, 2]);
    const line = d3.line().x((_, i) => x(i)).y((d) => y(d)).curve(d3.curveCatmullRom);
    svg
      .append("path")
      .datum(data)
      .attr("fill", "none")
      .attr("stroke", color)
      .attr("stroke-width", 1.5)
      .attr("d", line)
      .attr("opacity", 0.9);
  }, [data, color, width, height]);
  return <svg ref={ref} width={width} height={height} style={{ display: "block" }} />;
}

function MetricCard({ label, value, unit, color, sparkData, theme }) {
  return (
    <div
      style={{
        background: theme.cardBg,
        border: `1px solid ${color}33`,
        borderRadius: 6,
        padding: "10px 14px",
        minWidth: 130,
        boxShadow: `0 0 12px ${color}22`,
      }}
    >
      <div style={{ color: theme.labelColor, fontSize: 10, letterSpacing: 2, textTransform: "uppercase", fontFamily: "'Space Mono', monospace", marginBottom: 4 }}>{label}</div>
      <div style={{ color, fontSize: 22, fontFamily: "'Space Mono', monospace", fontWeight: 700, lineHeight: 1 }}>
        {typeof value === "number" ? value.toFixed(0) : value}
        <span style={{ fontSize: 11, color: theme.labelColor, marginLeft: 4 }}>{unit}</span>
      </div>
      {sparkData && <Sparkline data={sparkData} color={color} />}
    </div>
  );
}

function wsTarget() {
  if (import.meta.env.VITE_WS_URL) return import.meta.env.VITE_WS_URL;
  if (window.location.port === "5173") return "ws://localhost:8080/ws";
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${window.location.host}/ws`;
}

function apiTarget(path) {
  if (import.meta.env.VITE_API_BASE_URL) return `${import.meta.env.VITE_API_BASE_URL}${path}`;
  if (window.location.port === "5173") return `http://localhost:8080${path}`;
  return path;
}

export default function PipelineVisualizer() {
  const [state, setState] = useState(null);
  const [history, setHistory] = useState({ throughput: [], errors: [] });
  const [connected, setConnected] = useState(false);
  const [useWS, setUseWS] = useState(false);
  const [darkMode, setDarkMode] = useState(true);
  const animRef = useRef(null);
  const theme = THEMES[darkMode ? "dark" : "light"];

  useEffect(() => {
    fetch(apiTarget("/api/state"))
      .then((r) => r.json())
      .then((data) => {
        if (data?.nodes?.length) setState(data);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    let ws;
    try {
      ws = new WebSocket(wsTarget());
      ws.onopen = () => {
        setConnected(true);
        setUseWS(true);
      };
      ws.onmessage = (e) => {
        const data = JSON.parse(e.data);
        setState(data);
        setHistory((h) => ({
          throughput: [...h.throughput.slice(-50), data.throughput],
          errors: [...h.errors.slice(-50), data.errors],
        }));
      };
      ws.onerror = () => ws.close();
      ws.onclose = () => {
        setConnected(false);
        setUseWS(false);
      };
    } catch {
      setUseWS(false);
    }
    return () => ws?.close();
  }, []);

  useEffect(() => {
    if (useWS) return;
    const id = setInterval(() => {
      const data = simulatePipeline();
      setState(data);
      setHistory((h) => ({
        throughput: [...h.throughput.slice(-50), data.throughput],
        errors: [...h.errors.slice(-50), data.errors],
      }));
    }, 100);
    return () => clearInterval(id);
  }, [useWS]);

  useEffect(() => {
    const loop = () => {
      animRef.current = requestAnimationFrame(loop);
    };
    animRef.current = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(animRef.current);
  }, []);

  const nodeMap = useCallback(() => {
    if (!state) return {};
    return Object.fromEntries(state.nodes.map((n) => [n.id, n]));
  }, [state]);

  if (!state) return null;

  const nMap = nodeMap();
  const SVG_W = 920;
  const SVG_H = 500;

  return (
    <div style={{ background: theme.bg, minHeight: "100vh", width: "100%", fontFamily: "'Space Mono', monospace", color: theme.text, overflowX: "hidden", position: "relative", transition: "background 0.3s, color 0.3s" }}>
      <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&display=swap" rel="stylesheet" />
      <div style={{ position: "relative", zIndex: 10, display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 24px", borderBottom: `1px solid ${theme.headerBorder}`, background: theme.headerBg }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: theme.titleColor, letterSpacing: 3, textTransform: "uppercase" }}>Pipeline Visualizer</div>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div style={{ fontSize: 10, color: connected ? "#00d4aa" : "#f59e0b", letterSpacing: 2 }}>{connected ? "WS LIVE" : "SIMULATED"}</div>
          <button
            onClick={() => setDarkMode((d) => !d)}
            style={{
              background: theme.toggleBg,
              border: `1px solid ${theme.toggleBorder}`,
              borderRadius: 20,
              padding: "4px 12px",
              cursor: "pointer",
              fontSize: 11,
              color: theme.toggleColor,
              fontFamily: "'Space Mono', monospace",
              letterSpacing: 1,
              transition: "background 0.2s, color 0.2s",
            }}
          >
            {darkMode ? "☀ LIGHT" : "☾ DARK"}
          </button>
        </div>
      </div>

      <div style={{ position: "relative", zIndex: 10, display: "flex", gap: 12, padding: "12px 24px", overflowX: "auto", borderBottom: `1px solid ${theme.metricsBorder}` }}>
        <MetricCard label="Total Flow" value={state.totalFlow} unit="msg/s" color="#00d4aa" sparkData={history.throughput} theme={theme} />
        <MetricCard label="Avg Throughput" value={state.throughput} unit="msg/s" color="#f59e0b" sparkData={history.throughput} theme={theme} />
        <MetricCard label="Total Errors" value={state.errors} unit="" color="#ef4444" sparkData={history.errors} theme={theme} />
        <MetricCard label="Active Nodes" value={state.nodes.filter((n) => n.status === "active").length} unit={`/ ${state.nodes.length}`} color="#8b5cf6" theme={theme} />
      </div>

      <div style={{ position: "relative", zIndex: 10, padding: "16px 24px", width: "100%", boxSizing: "border-box", display: "flex", justifyContent: "center", alignItems: "flex-start" }}>
        <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} style={{ display: "block", width: "min(calc(100vw - 48px), 1300px)", height: "auto", borderRadius: 8, border: `1px solid ${theme.svgBorder}`, background: theme.svgBg, transition: "background 0.3s" }}>
          {state.edges.map((edge) => {
            const src = nMap[edge.source];
            const tgt = nMap[edge.target];
            if (!src || !tgt) return null;
            const c = TYPE_COLORS[src.type] ?? TYPE_COLORS.transform;
            const path = edgePath(src, tgt);
            return <path key={edge.id} d={path} fill="none" stroke={c.base} strokeWidth={1.5} opacity={0.4} />;
          })}

          {state.packets.map((pkt) => {
            const edge = state.edges.find((e) => e.id === pkt.edgeId);
            if (!edge) return null;
            const src = nMap[edge.source];
            const tgt = nMap[edge.target];
            if (!src || !tgt) return null;
            const pos = packetPos(src, tgt, pkt.progress);
            const color = PACKET_COLORS[pkt.packetType] ?? "#00d4aa";
            return <circle key={pkt.id} cx={pos.x} cy={pos.y} r={3 + pkt.size * 3} fill={color} opacity={0.85} />;
          })}

          {state.nodes.map((node) => {
            const c = TYPE_COLORS[node.type] ?? TYPE_COLORS.transform;
            const statusColor = STATUS_BADGE[node.status] ?? "#00d4aa";
            return (
              <g key={node.id}>
                <rect x={node.x} y={node.y} width={NODE_W} height={NODE_H} rx={5} fill={c.base} fillOpacity={darkMode ? 0.1 : 0.22} stroke={darkMode ? c.border : c.darkBorder} strokeWidth={1} strokeOpacity={darkMode ? 0.5 : 0.9} />
                <circle cx={node.x + NODE_W - 10} cy={node.y + 10} r={4} fill={statusColor} />
                <text x={node.x + 10} y={node.y + 17} style={{ fill: darkMode ? c.border : c.darkText, fontSize: 11, fontWeight: 700, fontFamily: "'Space Mono', monospace" }}>
                  {node.name}
                </text>
                <text x={node.x + 10} y={node.y + 31} style={{ fill: darkMode ? c.base : c.darkBorder, fontSize: 9, fontFamily: "'Space Mono', monospace", opacity: 0.8 }}>
                  {(node.throughput ?? 0).toFixed(0)} msg/s
                </text>
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}
