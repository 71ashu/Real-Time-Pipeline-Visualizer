# Pipeline Visualizer (Full Stack)

A complete full-stack project for visualizing real-time pipeline activity:
- `backend`: Go API + WebSocket stream
- `frontend`: React + Vite + D3 UI
- `docker-compose.yml`: run both services together

## Project Structure

```txt
pipeline-visualizer/
  backend/
    main.go
    go.mod
    Dockerfile
  frontend/
    src/
    package.json
    vite.config.js
    Dockerfile
  docker-compose.yml
```

## Features

- Real-time pipeline graph with nodes, edges, and packet movement
- Metrics cards for flow, throughput, errors, and node health
- WebSocket streaming from backend at 100ms intervals
- REST API endpoint for current state snapshot (`/api/state`)
- Frontend fallback simulator if backend is unavailable
- Dockerized local deployment for frontend + backend

## Local Development

### 1) Run backend

```bash
cd backend
go mod tidy
go run .
```

Backend runs at `http://localhost:8080`.

### 2) Run frontend

```bash
cd frontend
npm install
npm run dev
```

Frontend runs at `http://localhost:5173`.

## Docker (Both Services)

```bash
docker compose up --build
```

- Frontend: `http://localhost:4173`
- Backend: `http://localhost:8080`

## API Endpoints

- `GET /api/health` - health check
- `GET /api/state` - latest pipeline state snapshot
- `GET /ws` - WebSocket stream of live updates

## Notes

- Legacy files from the prototype (`pipeline-visualizer.html`, root `main.go`, root `PipelineVisualizer.jsx`) are still present for reference.
- The new full-stack app uses the `backend` and `frontend` folders.
