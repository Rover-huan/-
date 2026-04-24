# AutoReport AI Frontend

This frontend connects directly to the local `SmartAnalyst` backend.

## Prerequisites

- Node.js
- SmartAnalyst API available at `http://127.0.0.1:8000`
- SmartAnalyst worker / Redis already running

## Local Run

1. Install dependencies:
   `npm install`
2. Create `.env.local` if you need to override the backend address:
   `VITE_API_BASE_URL=http://127.0.0.1:8000`
3. Start the frontend:
   `npm run dev`
4. Open:
   `http://127.0.0.1:3000`

## Supported Workflow

- Register
- Login
- Upload one or more `csv/xls/xlsx` files
- Wait for analysis polling
- Select candidate charts
- Generate the final report
- Download `zip/docx/pdf/ipynb/txt` artifacts
