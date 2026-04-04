"""
Books User ETL Pipeline DAG
Orchestrates data generation, ETL transformation, and analytics loading
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.goodreads_plugin import DataQualityOperator
from airflow.operators.goodreads_plugin import LoadAnalyticsOperator
from airflow.plugins.helpers.analytics_queries import AnalyticsQueries

# Set up logging
logger = logging.getLogger(__name__)

# Default arguments for all tasks in the DAG
default_args = {
    "owner": "books_etl",
    "depends_on_past": False,  # Don't require previous DAG run to succeed
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email": ["your-email@example.com"],  # Update with your email
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),  # Allow up to 3 hours for DAG completion
    "catchup": False,  # Don't backfill old dates
}

# DAG definition
dag_name = "books_user_etl_pipeline"
dag = DAG(
    dag_name,
    default_args=default_args,
    description="Books User ETL Pipeline: Generate data, transform, and load to warehouse",
    schedule_interval="0 2 * * *",  # Run daily at 2 AM UTC
    max_active_runs=1,  # Only one run at a time
    tags=["books", "etl", "data-pipeline"],
    catchup=False,
)

# ============================================================================
# Task Definitions
# ============================================================================

# Start task
start_operator = DummyOperator(
    task_id="Begin_execution",
    dag=dag,
    doc_md="Start of the Books ETL pipeline"
)

# SSH Hook for EMR connection
emr_ssh_hook = SSHHook(ssh_conn_id="emr_ssh_connection")

# Task 1: Generate fake data
# This task generates synthetic Books/Goodreads data locally and uploads to S3
generate_data_task = BashOperator(
    task_id="Generate_Books_Data",
    bash_command="""
    set -e
    echo "Starting fake data generation..."
    cd /home/hadoop/books_user_etl_pipeline/goodreadsfaker
    
    # Generate 1000 records (10 iterations x 100 records each)
    python3 generate_fake_data.py -n 1000
    
    # Upload generated data to S3 landing zone
    echo "Uploading generated data to S3..."
    S3_BUCKET="{{ var.value.s3_bucket_name }}"
    aws s3 cp ~/GoodReadsData/fake s3://${S3_BUCKET}/books-landing-zone/ --recursive --region us-east-1
    
    echo "Data generation and upload completed successfully"
    """,
    dag=dag,
    pool="default_pool",
    queue="default",
    doc_md="Generates 50,000 fake book review records and uploads to S3 landing zone"
)

# Task 2: Run ETL Job on EMR
# This task orchestrates: S3 movement, Spark transformations, and warehouse operations
etl_job_operator = SSHOperator(
    task_id="BooksRead_ETL_Job",
    command="""
    cd /home/hadoop/books_user_etl_pipeline/src && \
    export PYSPARK_DRIVER_PYTHON=python3 && \
    export PYSPARK_PYTHON=python3 && \
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 4g \
        --executor-memory 4g \
        --executor-cores 2 \
        --num-executors 5 \
        booksread_driver.py 2>&1 | tee /tmp/etl_job.log
    """,
    ssh_hook=emr_ssh_hook,
    dag=dag,
    pool="emr_pool",
    doc_md="Executes main ETL job: moves data from landing → working → processed zones, applies transformations, loads to warehouse"
)

# Task 3: Warehouse data quality checks
# Validates that data successfully loaded to warehouse tables
warehouse_data_quality_checks = DataQualityOperator(
    task_id="Warehouse_Data_Quality_Checks",
    dag=dag,
    redshift_conn_id="redshift_connection",
    tables=[
        "bookreads_warehouse.authors",
        "bookreads_warehouse.books",
        "bookreads_warehouse.reviews",
        "bookreads_warehouse.users"
    ],
    doc_md="Validates row counts and data integrity in warehouse tables"
)

# Task 4: Create analytics schema
create_analytics_schema = LoadAnalyticsOperator(
    task_id="Create_Analytics_Schema",
    dag=dag,
    redshift_conn_id="redshift_connection",
    sql_query=[AnalyticsQueries.create_schema],
    doc_md="Creates analytical schema for books and authors analytics"
)

# Task 5: Create author analytics tables
create_author_analytics_table = LoadAnalyticsOperator(
    task_id="Create_Author_Analytics_Tables",
    redshift_conn_id="redshift_connection",
    sql_query=[
        AnalyticsQueries.create_author_reviews,
        AnalyticsQueries.create_author_rating,
        AnalyticsQueries.create_best_authors
    ],
    dag=dag,
    doc_md="Creates author-related analytics tables"
)

# Task 6: Create book analytics tables
create_book_analytics_table = LoadAnalyticsOperator(
    task_id="Create_Book_Analytics_Tables",
    redshift_conn_id="redshift_connection",
    sql_query=[
        AnalyticsQueries.create_book_reviews,
        AnalyticsQueries.create_book_rating,
        AnalyticsQueries.create_best_books
    ],
    dag=dag,
    doc_md="Creates book-related analytics tables"
)

# ============================================================================
# Author Analytics Tasks
# ============================================================================

load_author_table_reviews = LoadAnalyticsOperator(
    task_id="Load_Author_Table_Reviews",
    redshift_conn_id="redshift_connection",
    sql_query=[
        AnalyticsQueries.populate_author_reviews.format(
            '{{ ds }} 00:00:00.000000',
            '{{ ds }} 23:59:59.999999'
        )
    ],
    dag=dag,
    doc_md="Populates author reviews analytics table for current day"
)

load_author_table_ratings = LoadAnalyticsOperator(
    task_id="Load_Author_Table_Ratings",
    redshift_conn_id="redshift_connection",
    sql_query=[
        AnalyticsQueries.populate_author_rating.format(
            '{{ ds }} 00:00:00.000000',
            '{{ ds }} 23:59:59.999999'
        )
    ],
    dag=dag,
    doc_md="Populates author ratings analytics table for current day"
)

load_best_author = LoadAnalyticsOperator(
    task_id="Load_Best_Author",
    redshift_conn_id="redshift_connection",
    sql_query=[AnalyticsQueries.populate_best_authors],
    dag=dag,
    doc_md="Populates best authors analytics table"
)

# ============================================================================
# Book Analytics Tasks
# ============================================================================

load_book_table_reviews = LoadAnalyticsOperator(
    task_id="Load_Book_Table_Reviews",
    redshift_conn_id="redshift_connection",
    sql_query=[
        AnalyticsQueries.populate_book_reviews.format(
            '{{ ds }} 00:00:00.000000',
            '{{ ds }} 23:59:59.999999'
        )
    ],
    dag=dag,
    doc_md="Populates book reviews analytics table for current day"
)

load_book_table_ratings = LoadAnalyticsOperator(
    task_id="Load_Book_Table_Ratings",
    redshift_conn_id="redshift_connection",
    sql_query=[
        AnalyticsQueries.populate_book_rating.format(
            '{{ ds }} 00:00:00.000000',
            '{{ ds }} 23:59:59.999999'
        )
    ],
    dag=dag,
    doc_md="Populates book ratings analytics table for current day"
)

load_best_book = LoadAnalyticsOperator(
    task_id="Load_Best_Books",
    redshift_conn_id="redshift_connection",
    sql_query=[AnalyticsQueries.populate_best_books],
    dag=dag,
    doc_md="Populates best books analytics table"
)

# ============================================================================
# Data Quality Checks for Analytics
# ============================================================================

authors_data_quality_checks = DataQualityOperator(
    task_id="Authors_Analytics_Data_Quality_Checks",
    dag=dag,
    redshift_conn_id="redshift_connection",
    tables=[
        "bookreads_analytics.author_reviews",
        "bookreads_analytics.author_average_rating",
        "bookreads_analytics.popular_authors_average_rating"
    ],
    doc_md="Validates author analytics tables have expected data"
)

books_data_quality_checks = DataQualityOperator(
    task_id="Books_Analytics_Data_Quality_Checks",
    dag=dag,
    redshift_conn_id="redshift_connection",
    tables=[
        "bookreads_analytics.popular_books_average_rating",
        "bookreads_analytics.popular_books_review_count"
    ],
    doc_md="Validates book analytics tables have expected data"
)

# End task
end_operator = DummyOperator(
    task_id="Stop_Execution",
    dag=dag,
    doc_md="End of the Books ETL pipeline"
)

# ============================================================================
# Task Dependencies / DAG Execution Flow
# ============================================================================

# Main execution flow
start_operator >> generate_data_task >> etl_job_operator >> warehouse_data_quality_checks >> create_analytics_schema

# Analytics schema must be created before populating analytics tables
create_analytics_schema >> [create_author_analytics_table, create_book_analytics_table]

# Author analytics: Create tables → Load data → Data quality
create_author_analytics_table >> [load_author_table_reviews, load_author_table_ratings, load_best_author] >> authors_data_quality_checks

# Book analytics: Create tables → Load data → Data quality
create_book_analytics_table >> [load_book_table_reviews, load_book_table_ratings, load_best_book] >> books_data_quality_checks

# End after both analytics data quality checks complete
[authors_data_quality_checks, books_data_quality_checks] >> end_operator
