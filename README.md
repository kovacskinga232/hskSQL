# hskSQL

A full-stack, visual database management system built on top of MongoDB, implementing a relational-style key-value storage engine with a fully graphical interface — no SQL typing required. Built as a university Database Management Systems course project.

---

## Overview

hskSQL simulates a relational database engine using MongoDB collections as the underlying storage. Relational tables are stored as MongoDB collections, where each document represents a row: the **primary key** is stored as the `key` field, and the remaining attributes are concatenated and stored in the `value` field.

All operations — from inserting rows to running multi-table joins — are performed through a point-and-click web UI, with no manual query writing needed.

---

## Features

### Data Definition & Storage
- Relational tables stored in MongoDB collections using a key-value structure
- Catalog stored as a JSON file describing table schemas, column types, and constraints
- Support for primary keys, unique keys, and foreign keys

### Index Management
- Automatic index file creation and maintenance for unique and non-unique keys
- Index files stored as separate MongoDB collections
- Indexes used automatically during query execution to optimize performance
- Range-based index lookups supported (e.g. `age > 18 AND age < 30`)

### INSERT & DELETE
- Visual Query Designer for inserting and deleting records
- Primary Key, Unique Key, and Foreign Key constraint validation on every write
- Cascading index file updates on insert and delete
- Deletion blocked with an error message if a foreign key reference exists

### SELECT — Projection & Selection
- Visual column picker (individual columns or `SELECT *`)
- WHERE clause builder supporting multiple AND conditions
- Comparison operators for numeric fields: `=`, `>`, `>=`, `<`, `<=`
- Equality operator for string fields
- Automatic index usage: if indexed columns are present in WHERE or projection, indexes are used; remaining columns are scanned sequentially
- Duplicate elimination in results

### JOIN
- Indexed Nested Loop Join algorithm
- INNER JOIN support across 2 or more tables
- Multi-table join chaining
- Tested on tables with 100,000+ rows

### Aggregation
- `GROUP BY` with aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `ORDER BY` support
- `HAVING` clause support (bonus feature)
- Index-aware aggregation

---

## Tech Stack

| Layer    | Technology              |
|----------|-------------------------|
| Backend  | Python, FastAPI          |
| Frontend | JavaScript, HTML, CSS   |
| Database | MongoDB                 |

---

## Project Structure

```
hskSQL/
├── backend/
│   ├── server.py              # FastAPI entry point
│   ├── database/
│   │   └── databases.json     # (you must create this — see setup below)
│   └── .env                   # (you must create this — see setup below)
├── frontend/                  # Web UI
└── .gitignore
```

---

## Getting Started

### Prerequisites

- Python 3.8+
- pip
- A running MongoDB instance (see options below)

### 1. Start MongoDB

**Option A — Docker (recommended):**
```bash
docker run -d -p 27017:27017 mongo
```

**Option B — Local install:**  
Download and install from https://www.mongodb.com/try/download/community, then run:
```bash
mongod
```

**Option C — MongoDB Atlas (cloud, free tier):**  
Create a free cluster at https://www.mongodb.com/pricing and copy your connection string.

---

### 2. Configure the backend

Inside the `backend/` folder, create two files:

**`.env`** — MongoDB connection string:
```
MONGO_URI=mongodb://localhost:27017
```
> If using MongoDB Atlas, replace the value with your Atlas connection string.

**`backend/databases/databases.json`** — starts as an empty catalog:
```json
{}
```

---

### 3. Install Python dependencies and run the backend

```bash
cd backend
pip install fastapi uvicorn pymongo python-dotenv
python server.py
```

The API will be available at `http://localhost:8000`  
Interactive API docs at `http://localhost:8000/docs`

---

### 4. Open the frontend

Simply open `frontend/index.html` directly in your browser — no build step or server needed.

> **Windows users:** If you want to serve it via a local server and get a PowerShell script execution error with `npx`, run this first in PowerShell as Administrator:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
> ```
> Then `npx serve frontend` will work. Alternatively, just open the HTML file directly.

---

## Example Queries (via UI)

The following SQL-equivalent operations can all be performed through the visual interface:

```sql
SELECT * FROM customers

SELECT Name, Age, EmailAddress FROM customers

SELECT Name FROM customers WHERE CategoryID = 15 AND Age >= 18

SELECT p.product_name, c.category_name, p.price
FROM products p
         INNER JOIN categories c ON c.category_id = p.category_id
WHERE p.price > 100

SELECT cs.GroupID, COUNT(d.StudentID)
FROM Groups g JOIN Students s ON s.GroupID = g.GroupID
WHERE s.Age > 18
GROUP BY g.GroupID
HAVING COUNT(d.StudentID) >= 10
```

---

## Authors

Built by [kovacskinga232](https://github.com/kovacskinga232) and a fellow colleague as part of a university Database Management Systems course project.