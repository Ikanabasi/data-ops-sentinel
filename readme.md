
# DataOps Sentinel

### AWS Banking Data Engineering & Financial Monitoring Platform

DataOps Sentinel is an end-to-end data engineering project that
collects real banking financial data from the FDIC API, processes
it through a Bronze–Silver–Gold architecture, and provides
interactive financial monitoring dashboards.

## Project Results

- 102,983 bank-quarter financial records processed
- 22 reporting quarters (Q1 2021 – Q2 2026)
- 4,313 institutions in the latest validated quarter
- Eight SQL views for financial monitoring and analysis
- Automated testing and GitHub Actions CI/CD
- Interactive Streamlit dashboard with historical trends

## Technology Stack

Python, Pandas, PostgreSQL-independent SQL analytics,
AWS S3, AWS Glue, Amazon Athena, Parquet, Plotly,
Streamlit, pytest, Git and GitHub Actions.

## Dashboard Features

- Executive financial monitoring overview
- Institution-level investigation
- Deposit, asset and loan trends
- Profitability and credit-quality monitoring
- Historical Explorer
- Downloadable reports

## Run Locally

Install dependencies:

    pip install -r requirements.txt

Start the dashboard:

    python -m streamlit run dashboard/app.py

The AWS-backed version requires authorized read access
to the private S3 dashboard snapshots.

## Data Source

FDIC Bank Financial Data API

## Important Note

Monitoring alerts are exploratory indicators, not
official regulatory distress ratings or predictions
of bank failure.

## Future Improvements

An AI-powered banking investigation assistant will be
developed as a separate project.
