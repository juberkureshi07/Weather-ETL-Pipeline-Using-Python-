# 🌦 Data Engineering Project  
## ETL Pipeline using Python (API → Clean → CSV / SQLite)

This project implements a complete **Data Engineering ETL Pipeline** using Python.  
It extracts real-time weather data from an open API, transforms/cleans the data using pandas, and loads it into both **CSV** and **SQLite database** formats.

---

## 📌 Objective
Build an ETL system using:
- Python programming  
- OpenWeatherMap API (Public Open API)  
- pandas (Data Cleaning & Transformation)  
- sqlite3 (Database Storage)  
- CSV file handling  

---

## 🚀 Features

- Extracts real-time weather data from an open API  
- Cleans & transforms raw JSON data  
- Loads structured data into:
  - CSV file  
  - SQLite database  
- Covers all **Maharashtra districts** + **Nashik talukas**  
- Includes retry mechanism for failed API calls  

---

## 🛠 Technologies Used

| Component | Tool |
|----------|------|
| Programming | Python |
| API | OpenWeatherMap |
| Data Processing | pandas |
| Database | sqlite3 |
| File Format | CSV |

---

## 📂 Project Structure

```
Weather-ETL-Pipeline-Using-Python-/
│
├── Weather_ETL.py
├── README.md
├── requirements.txt
├── .gitignore
│
└── output/
      ├── weather_YYYYMMDD_HHMM.csv
      └── weather.db
```

---

## 🔧 Setup Instructions

### 1️⃣ Install Dependencies

```
pip install -r requirements.txt
```

### 2️⃣ Create `.env` File

```
OPENWEATHER_KEY=your_api_key_here
```

### 3️⃣ Run the ETL Pipeline

```
python Weather_ETL.py
```

---

## 🗄 Output Files

### 📌 CSV File  
Saved in: `output/weather_timestamp.csv`

### 📌 SQLite Database  
Saved in: `output/weather.db`  
Table name: `weather_data`

---

## 📜 License  
MIT License – Free to use and modify.

---

## 👨‍💻 Author
**Juber J. Kureshi**  
SND College of Engineering & Research Centre, Yeola  
Project: Data Engineering ETL Pipeline
