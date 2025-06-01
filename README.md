# 📉 Layoff Data Analysis with SQL

This project is part of the **Data Analyst Bootcamp by [Alex The Analyst](https://www.youtube.com/playlist?list=PLUaB-1hjhk8FE_XZ87vPPSfHqb6OcM0cF)** on YouTube.

I used raw SQL to clean, transform, and analyze a dataset on tech layoffs from 2020 to 2023.

## 📊 Dataset
- Source: [Layoffs Dataset CSV](https://github.com/AlexTheAnalyst/MySQL-YouTube-Series/blob/main/layoffs.csv)
- Duration: March 2020 to March 2023
- Fields include company, location, industry, total_laid_off, percentage_laid_off, date, stage, and funding info.

## 🧹 What I Did

- Created staging tables to preserve raw data
- Removed duplicates using `ROW_NUMBER()`
- Standardized text data (e.g., trimming whitespace, fixing country/industry names)
- Filled missing `industry` values via self-join
- Removed unusable rows (where both layoff fields were null)
- Performed **exploratory data analysis** using:
  - Aggregations (company, industry, country, stage)
  - Monthly and yearly trends
  - Rolling total of layoffs
  - Top 5 companies with highest layoffs per year

## 📂 Files

- `DataAnalysis.sql` – full SQL cleaning + EDA queries with detailed comments
- `layoffs.csv` – the dataset used

## 🧠 Key Insights

- 📆 **2022 had the most layoffs**, but 2023 was second highest by March
- 🏢 **Amazon, Google, Meta** led layoffs, with over 10K each
- 🌍 The **United States** saw over 250K job losses, far ahead of other countries
- 📉 Even **well-funded and public companies** were not safe (Post-IPO layoffs were highest)
