# Install required libraries
!pip install requests pandas pandera tenacity --quiet faker

import requests
import pandas as pd
import logging
from datetime import datetime
import os
from tenacity import retry, stop_after_attempt, wait_exponential
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from concurrent.futures import ThreadPoolExecutor
from google.colab import files
import random
from faker import Faker

# Initialize Faker for realistic data generation
faker = Faker()

# Configure logging for observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration dictionary for maintainability
CONFIG = {
    'API_URL': 'https://fakestoreapi.com/products',
    'OUTPUT_PATH': 'business_data_for_powerbi.csv',
    'USER_AGENT': 'BusinessPipeline/1.0 (recruiter@example.com)'
}

# Data validation schemas for quality assurance
product_schema = DataFrameSchema({
    'product_id': Column(int, Check.ge(1), nullable=False),
    'product_name': Column(str, Check.str_length(min_value=1), nullable=False),
    'unit_price': Column(float, Check.gt(0), nullable=False),
    'product_category': Column(str, Check.str_length(min_value=1), nullable=False),
    'price_with_tax': Column(float, Check.gt(0), nullable=False)
})

order_schema = DataFrameSchema({
    'order_id': Column(int, Check.ge(100), nullable=False),
    'customer_name': Column(str, Check.str_length(min_value=1), nullable=False),
    'order_amount': Column(float, Check.gt(0), nullable=False),
    'order_date': Column(pa.DateTime, Check(lambda x: x <= pd.Timestamp.now()), nullable=False),
    'total_order_value': Column(float, Check.gt(0), nullable=False)
})

# Step 1: Extract Data
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def extract_from_api():
    """Extract e-commerce product data from FakeStoreAPI."""
    logger.info("Initiating API data extraction")
    try:
        headers = {'User-Agent': CONFIG['USER_AGENT']}
        response = requests.get(CONFIG['API_URL'], headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logger.info(f"Successfully extracted {len(df)} product records from API")
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"API extraction failed: {e}")
        raise

def extract_from_csv():
    """Extract simulated customer order data from a CSV file."""
    logger.info("Initiating CSV data extraction")
    try:
        # Generate 50 orders with realistic data
        sample_data = {
            'order_id': list(range(101, 151)),
            'customer_name': [faker.name() for _ in range(50)],
            'order_amount': [round(random.uniform(50.0, 500.0), 2) for _ in range(50)],
            'order_date': [faker.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') for _ in range(50)]
        }
        sample_df = pd.DataFrame(sample_data)
        sample_df.to_csv('sample_orders.csv', index=False)
        df = pd.read_csv('sample_orders.csv')
        logger.info(f"Successfully extracted {len(df)} order records from CSV")
        return df
    except Exception as e:
        logger.error(f"CSV extraction failed: {e}")
        raise

# Step 2: Transform Data
def transform_data(api_df, csv_df):
    """Transform and integrate product and order data for reporting."""
    logger.info("Initiating data transformation")
    try:
        # Transform API data (products)
        if not api_df.empty:
            api_df = api_df[['id', 'title', 'price', 'category']].copy()
            api_df.rename(columns={
                'id': 'product_id',
                'title': 'product_name',
                'price': 'unit_price',
                'category': 'product_category'
            }, inplace=True)
            api_df['source'] = 'API'
            api_df['load_timestamp'] = datetime.now()
            api_df['price_with_tax'] = api_df['unit_price'] * 1.10
            product_schema.validate(api_df, lazy=True)
            logger.info("Product data validated successfully")
        else:
            logger.warning("No API data available for transformation")

        # Transform CSV data (orders)
        if not csv_df.empty:
            csv_df = csv_df[['order_id', 'customer_name', 'order_amount', 'order_date']].copy()
            csv_df['source'] = 'CSV'
            csv_df['load_timestamp'] = datetime.now()
            csv_df['order_date'] = pd.to_datetime(csv_df['order_date'])
            csv_df['order_year'] = csv_df['order_date'].dt.year
            logger.info("Order data prepared")
        else:
            logger.warning("No CSV data available for transformation")

        # Simulate product-order relationship (for demo purposes)
        if not csv_df.empty and not api_df.empty:
            csv_df['product_id'] = api_df['product_id'].iloc[0]  # Assign first product
            combined_df = pd.merge(
                csv_df,
                api_df[['product_id', 'product_name', 'unit_price', 'product_category', 'price_with_tax']],
                on='product_id',
                how='left'
            )
            combined_df['total_order_value'] = combined_df['order_amount'] + combined_df['price_with_tax']
            combined_df.fillna({
                'product_name': 'Unknown',
                'unit_price': 0,
                'product_category': 'N/A',
                'price_with_tax': 0
            }, inplace=True)
            order_schema.validate(combined_df, lazy=True)
            logger.info("Combined data validated successfully")
        else:
            combined_df = pd.DataFrame()
            logger.warning("No data combined due to empty input")

        return combined_df
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise

# Step 3: Load Data
def load_to_csv(df):
    """Save transformed data to a CSV file for Power BI reporting."""
    logger.info("Initiating data loading")
    try:
        output_path = CONFIG['OUTPUT_PATH']
        df.to_csv(output_path, index=False)
        logger.info(f"Data successfully saved to {output_path}")
        files.download(output_path)
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise

# Main Pipeline
def run_pipeline():
    """Execute the end-to-end data pipeline with parallel extraction."""
    logger.info("Starting data pipeline execution")
    try:
        # Parallel extraction
        with ThreadPoolExecutor() as executor:
            api_future = executor.submit(extract_from_api)
            csv_future = executor.submit(extract_from_csv)
            api_data = api_future.result()
            csv_data = csv_future.result()

        # Transform
        if not api_data.empty or not csv_data.empty:
            transformed_data = transform_data(api_data, csv_data)
        else:
            logger.error("No data extracted; pipeline aborted")
            return

        # Load
        if not transformed_data.empty:
            load_to_csv(transformed_data)
        else:
            logger.error("No data to load; pipeline aborted")
            return

        logger.info("Data pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

# Execute pipeline
if __name__ == "__main__":
    run_pipeline()

# README
"""
# Business Data Pipeline

## Overview
This Python script implements a robust end-to-end data pipeline for extracting, transforming, and loading (ETL) business data to support reporting. It integrates e-commerce product data from a public API with a simulated dataset of 50 customer orders, processes the data with rigorous validation, and produces a CSV output optimized for Power BI business intelligence reporting.

## Features
- **Extraction**: Retrieves product data from FakeStoreAPI and 50 customer orders from a generated CSV, using parallel processing for efficiency.
- **Transformation**: Cleans, enriches, and merges datasets, incorporating derived metrics such as price with tax and total order value.
- **Validation**: Applies schema-based data quality checks to ensure data integrity.
- **Loading**: Generates a Power BI-compatible CSV file, downloadable directly from Google Colab.
- **Observability**: Implements structured logging for execution tracking and diagnostics.
- **Reliability**: Features API retry logic and comprehensive error handling for robust operation.

## Prerequisites
- **Environment**: Google Colab (cloud-based Python notebook).
- **Dependencies**: Automatically installed (`requests`, `pandas`, `pandera`, `tenacity`, `faker`).
- **Internet**: Required for API connectivity.

## Usage
1. Copy and execute the script in a Google Colab notebook.
2. The pipeline will:
   - Extract product data from FakeStoreAPI and 50 simulated orders from a CSV.
   - Transform and validate the combined dataset.
   - Save the output as `business_data_for_powerbi.csv` and download it automatically.
3. Import the CSV into Power BI to create visualizations, such as sales by product category or order trends.
4. Download `pipeline.log` from Colab for execution details or troubleshooting.

## Data Sources
- **FakeStoreAPI**: A public API (https://fakestoreapi.com/products) providing realistic e-commerce product data, including product ID, name, price, and category.
- **Sample CSV**: A programmatically generated dataset containing 50 customer orders with fields for order ID, customer name, order amount, and order date.

## Output
The pipeline produces `business_data_for_powerbi.csv` with the following columns:
- Order details: `order_id`, `customer_name`, `order_amount`, `order_date`, `order_year`.
- Product details: `product_id`, `product_name`, `unit_price`, `product_category`, `price_with_tax`.
- Derived metrics: `total_order_value`.
- Metadata: `source`, `load_timestamp`.

## Technical Details
- **Error Handling**: Robust try-except blocks and logging ensure operational stability.
- **Retries**: API requests include up to three retries with exponential backoff for reliability.
- **Validation**: Pandera schemas enforce data quality, checking for valid IDs, positive values, and date ranges.
- **Performance**: Parallel extraction via ThreadPoolExecutor optimizes runtime.
- **Configuration**: Centralized settings enhance maintainability.

## Customization
To extend for production use:
- **API**: Update `CONFIG['API_URL']` to a proprietary endpoint with appropriate authentication (e.g., API keys).
- **Data Source**: Modify `extract_from_csv` to read from Google Drive, a database, or an uploaded file.
- **Validation**: Adjust schemas to align with specific data requirements.
- **Output**: Extend `load_to_csv` to support cloud databases (e.g., Google BigQuery) or storage services.
- **Scheduling**: Deploy to a cloud platform (e.g., Google Cloud Functions) with automated triggers.

## Limitations
- **Colab Environment**: Outputs (`business_data_for_powerbi.csv`, `pipeline.log`) are stored temporarily and must be downloaded.
- **Demo Data**: The CSV is simulated, and the product-order relationship is simplified for demonstration purposes.
- **Alerting**: Email notifications are not included due to Google Colab’s SMTP constraints.

## Future Enhancements
- Integration with cloud databases like Google BigQuery for scalable storage.
- Implementation of real-time alerting using cloud monitoring tools.
- Workflow orchestration with tools like Apache Airflow for scheduled execution.
- Support for incremental data loading to manage large datasets efficiently.

## Notes
This pipeline demonstrates proficiency in data engineering, including ETL processes, data validation, and integration with business intelligence tools. It is designed for clarity, scalability, and robustness, making it suitable for professional evaluation.

For inquiries, please contact me on linkedin https://www.linkedin.com/in/edward-antwi-8a01a1196/
"""