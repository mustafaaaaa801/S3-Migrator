# S3 Migrator

S3 Migrator is a project for migrating files from an AWS S3 bucket (or S3-compatible service) to another destination efficiently. The project includes a **Backend API** (FastAPI), a **Frontend UI**, and a **Worker** system for asynchronous tasks using **Celery** and **RabbitMQ**.

---

## Project Description

S3 Migrator is designed for seamless migration of large volumes of S3 objects while tracking status and maintaining logs. It supports:

- AWS S3 and S3-compatible storage.
- Background task processing with Celery.
- Monitoring and controlling migration through a simple web frontend.
- Status tracking and logs stored in a local SQLite database.
- Configurable number of workers and multipart upload settings.
- Secure storage of configuration files (permissions 600).

This project is ideal for developers and teams who need a lightweight, internal tool for S3 data migration with both programmatic and visual monitoring capabilities.

---

## Features

- **Asynchronous Migration:** Handles large file transfers without blocking the main application.
- **Frontend Dashboard:** Provides real-time status and logs.
- **Backend API:** Easy integration with scripts or other systems.
- **Worker System:** Celery manages task distribution across multiple workers.
- **Logging & Monitoring:** All actions logged to files and summarized in the database.
- **Configurable:** Max workers, multipart upload threshold, and dry-run mode.

---

## Requirements

- Docker & Docker Compose
- Python 3.11 (for backend and worker)
- Node.js (for frontend build if needed)
- RabbitMQ

---

## Project Structure

 migrator/
│
├─ backend/
│ ├─ app/
│ │ ├─ main.py # FastAPI backend entry point
│ │ └─ migrator.py # Migration logic
│ ├─ Dockerfile
│ └─ requirements.txt
│
├─ frontend/
│ ├─ index.html # Frontend entry point
│ └─ src/
│ └─ main.jsx
│ └─ Dockerfile
│
├─ worker/
│ ├─ worker.py # Celery worker entry
│ └─ Dockerfile
│
├─ docker-compose.yml
└─ README.md


---

## Environment Variables

- **Backend**
  - `AWS_ACCESS_KEY` : Your AWS access key
  - `AWS_SECRET_KEY` : Your AWS secret key
- **Worker**
  - `CELERY_BROKER_URL` : RabbitMQ URL (e.g., `amqp://guest:guest@rabbitmq:5672//`)
  - `CELERY_RESULT_BACKEND` : Optional (default: `rpc://`)

---

## Installation & Usage

1. Clone the repository:
```bash
git clone <your-repo-url>
cd migrator


Build and run using Docker Compose:

docker-compose up --build


Access services:

Backend API: http://localhost:5000

Frontend UI: http://localhost

RabbitMQ Management UI: http://localhost:15672
 (user: guest, pass: guest)

Configure migration:

Use the API endpoint /api/config to save AWS/S3 configuration.

Start migration using /api/start.

Monitor progress via /api/status and /api/logs.

Notes

Make sure RabbitMQ is running before starting the worker.

All data and logs are saved in ./data and ./logs.

The project is designed to be run as an internal tool on a local or internal server.
gmail:mustafazamzamkazak@gmail.com
