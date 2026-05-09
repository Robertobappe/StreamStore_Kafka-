---
name: testing-dashboard
description: How to test the StreamStore Kafka real-time dashboard end-to-end.
---

## Prerequisites

- Docker must be running
- Python dependencies installed: `pip install -r requirements.txt`

## Start Services

1. Start Kafka:
   ```bash
   docker compose up -d
   ```
   Wait for healthy status: `docker inspect --format='{{.State.Health.Status}}' kafka`

2. Start the FastAPI backend:
   ```bash
   uvicorn backend.app.main:app --port 8000
   ```

3. Start the order simulator (separate terminal):
   ```bash
   python simulator.py
   ```

4. Open `http://localhost:8000` in browser.

## What to Verify

- **Dashboard layout**: Header with "StreamStore", 4 stat cards, timeline chart, products doughnut, revenue bar chart, live orders feed
- **WebSocket status**: Green dot + "Connected" in top-right header
- **Real-time orders**: Orders appear in Live Orders feed with product, user, time, price
- **Stats update**: Total Orders, Items Sold, Revenue, Avg/Order increase as simulator runs
- **Charts update**: Timeline, Top Products, and Revenue by Product charts populate with data
- **REST API**: `curl http://localhost:8000/api/stats` and `curl http://localhost:8000/api/orders` return valid JSON

## Notes

- All data is in-memory; restarting backend resets stats
- Kafka shows `UNKNOWN_TOPIC_OR_PART` warnings briefly before the first message (expected)
- CLI tools also available: `python producer.py` (single order), `python tracker.py` (terminal consumer)
