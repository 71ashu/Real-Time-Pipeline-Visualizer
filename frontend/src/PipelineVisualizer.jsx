import { useState, useEffect, useRef, useCallback } from "react";
import * as d3 from "d3";

let packetCounter = 0;
const NODES_DEF = [
  { id: "source-1", name: "Kafka Stream", type: "source", x: 100, y: 120 },
  { id: "source-2", name: "Event Bus", type: "source", x: 100, y: 320 },
  { id: "transform-1", name: "Parser", type: "transform", x: 330, y: 50 },
  { id: "transform-2", name: "Enricher", type: "transform", x: 330, y: 220 },
  { id: "transform-3", name: "Validator", type: "transform", x: 330, y: 380 },
  { id: "aggregate-1", name: "Aggregator", type: "aggregate", x: 560, y: 130 },
  { id: "aggregate-2", name: "Joiner", type: "aggregate", x: 560, y: 300 },
  { id: "sink-1", name: "PostgreSQL", type: "sink", x: 790, y: 60 },
  { id: "sink-2", name: "Redis Cache", type: "sink", x: 790, y: 210 },
  { id: "sink-3", name: "S3 Archive", type: "sink", x: 790, y: 360 },
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
  source: { base: "#00d4aa", glow: "#00d4aa44", border: "#00ffc8" },
  transform: { base: "#f59e0b", glow: "#f59e0b44", border: "#fbbf24" },
  aggregate: { base: "#8b5cf6", glow: "#8b5cf644", border: "#a78bfa" },
  sink: { base: "#06b6d4", glow: "#06b6d444", border: "#67e8f9" },
};
const STATUS_BADGE = { active: "#00d4aa", warning: "#f59e0b", error: "#ef4444" };
const PACKET_COLORS = { data: "#00d4aa", error: "#ef4444", control: "#8b5cf6" };

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

function MetricCard({ label, value, unit, color, sparkData }) {
  return (
    <div
      style={{
        background: "rgba(10,12,20,0.85)",
        border: `1px solid ${color}33`,
        borderRadius: 6,
        padding: "10px 14px",
        minWidth: 130,
        boxShadow: `0 0 12px ${color}22`,
      }}
    >
      <div style={{ color: "#556", fontSize: 10, letterSpacing: 2, textTransform: "uppercase", fontFamily: "'Space Mono', monospace", marginBottom: 4 }}>{label}</div>
      <div style={{ color, fontSize: 22, fontFamily: "'Space Mono', monospace", fontWeight: 700, lineHeight: 1 }}>
        {typeof value === "number" ? value.toFixed(0) : value}
        <span style={{ fontSize: 11, color: "#556", marginLeft: 4 }}>{unit}</span>
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
  const animRef = useRef(null);

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
  const SVG_H = 470;

  return (
    <div style={{ background: "#060810", minHeight: "100vh", width: "100%", fontFamily: "'Space Mono', monospace", color: "#ccd", overflowX: "hidden", position: "relative" }}>
      <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&display=swap" rel="stylesheet" />
      <div style={{ position: "relative", zIndex: 10, display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 24px", borderBottom: "1px solid #ffffff0a", background: "rgba(6,8,16,0.9)" }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: "#00d4aa", letterSpacing: 3, textTransform: "uppercase" }}>Pipeline Visualizer</div>
        <div style={{ fontSize: 10, color: connected ? "#00d4aa" : "#f59e0b", letterSpacing: 2 }}>{connected ? "WS LIVE" : "SIMULATED"}</div>
      </div>

      <div style={{ position: "relative", zIndex: 10, display: "flex", gap: 12, padding: "12px 24px", overflowX: "auto", borderBottom: "1px solid #ffffff05" }}>
        <MetricCard label="Total Flow" value={state.totalFlow} unit="msg/s" color="#00d4aa" sparkData={history.throughput} />
        <MetricCard label="Avg Throughput" value={state.throughput} unit="msg/s" color="#f59e0b" sparkData={history.throughput} />
        <MetricCard label="Total Errors" value={state.errors} unit="" color="#ef4444" sparkData={history.errors} />
        <MetricCard label="Active Nodes" value={state.nodes.filter((n) => n.status === "active").length} unit={`/ ${state.nodes.length}`} color="#8b5cf6" />
      </div>

      <div style={{ position: "relative", zIndex: 10, padding: "16px 24px", width: "100%", boxSizing: "border-box", display: "flex", justifyContent: "center", alignItems: "flex-start" }}>
        <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} style={{ display: "block", width: "min(calc(100vw - 48px), 1300px)", height: "auto", borderRadius: 8, border: "1px solid #ffffff08", background: "rgba(4,6,12,0.7)" }}>
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
                <rect x={node.x} y={node.y} width={NODE_W} height={NODE_H} rx={5} fill={c.base} fillOpacity={0.1} stroke={c.border} strokeWidth={1} strokeOpacity={0.5} />
                <circle cx={node.x + NODE_W - 10} cy={node.y + 10} r={4} fill={statusColor} />
                <text x={node.x + 10} y={node.y + 17} style={{ fill: c.border, fontSize: 11, fontWeight: 700, fontFamily: "'Space Mono', monospace" }}>
                  {node.name}
                </text>
                <text x={node.x + 10} y={node.y + 31} style={{ fill: c.base, fontSize: 9, fontFamily: "'Space Mono', monospace", opacity: 0.8 }}>
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
