"""
Books User ETL Driver
Main orchestration script for Books ETL pipeline
Manages data movement, transformation, and warehouse operations
"""

import sys
import logging
import logging.config
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession

from booksread_transform import GoodreadsTransform
from s3_module import GoodReadsS3Module
from warehouse.booksread_warehouse_driver import GoodreadsWarehouseDriver

import configparser
import time

# ============================================================================
# Configuration Setup
# ============================================================================

# Read configuration file
config = configparser.ConfigParser()
config_path = Path(__file__).parent / "config.cfg"

try:
    config.read_file(open(config_path))
except FileNotFoundError:
    print(f"ERROR: Configuration file not found at {config_path}")
    sys.exit(1)

# Set up logging
try:
    logging_config_path = Path(__file__).parent / "logging.ini"
    if logging_config_path.exists():
        logging.config.fileConfig(logging_config_path)
    else:
        # Basic logging configuration if logging.ini doesn't exist
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
except Exception as e:
    logging.basicConfig(level=logging.INFO)
    logging.warning(f"Could not load logging configuration: {e}")

logger = logging.getLogger(__name__)


# ============================================================================
# Spark Session Creation
# ============================================================================

def create_sparksession():
    """
    Initialize and configure a Spark session for Books ETL
    
    Returns:
        SparkSession: Configured Spark session with necessary packages
    """
    try:
        logger.info("Creating Spark session...")
        
        spark = SparkSession.builder \
            .master('yarn') \
            .appName("books_user_etl") \
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.adaptive.enabled", "true") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logger.info(f"Spark session created. Spark version: {spark.version}")
        return spark
        
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}", exc_info=True)
        raise


# ============================================================================
# Main ETL Process
# ============================================================================

def main():
    """
    Main ETL orchestration function
    
    Process Flow:
    1. Check Landing Zone for new data files
    2. Move files from Landing Zone to Working Zone
    3. Apply Spark transformations on data
    4. Save transformed data to Processed Zone
    5. Set up Redshift staging tables
    6. Load data from Processed Zone to staging tables
    7. Set up warehouse tables
    8. Perform UPSERT operations to populate warehouse
    
    Raises:
        Exception: If any critical step fails
    """
    
    spark = None
    
    try:
        start_time = datetime.now()
        logger.info("=" * 80)
        logger.info(f"Starting Books User ETL Pipeline at {start_time}")
        logger.info("=" * 80)
        
        # ====================================================================
        # Phase 1: Initialize Spark and Transformation Classes
        # ====================================================================
        
        logger.info("Phase 1: Initializing Spark session and transformation engine...")
        spark = create_sparksession()
        
        try:
            grt = GoodreadsTransform(spark)
            logger.info("Transformation engine initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize transformation engine: {str(e)}", exc_info=True)
            raise
        
        # Define transformation modules
        modules = {
            "author.csv": ("Author", grt.transform_author_dataset),
            "book.csv": ("Book", grt.transform_books_dataset),
            "reviews.csv": ("Reviews", grt.transform_reviews_dataset),
            "user.csv": ("User", grt.tranform_users_dataset)
        }
        
        # ====================================================================
        # Phase 2: Data Movement (Landing Zone → Working Zone)
        # ====================================================================
        
        logger.info("Phase 2: Moving data from Landing Zone to Working Zone...")
        
        try:
            gds3 = GoodReadsS3Module()
            landing_zone = config.get('BUCKET', 'LANDING_ZONE')
            working_zone = config.get('BUCKET', 'WORKING_ZONE')
            processed_zone = config.get('BUCKET', 'PROCESSED_ZONE')
            
            logger.info(f"Landing Zone: {landing_zone}")
            logger.info(f"Working Zone: {working_zone}")
            logger.info(f"Processed Zone: {processed_zone}")
            
            gds3.s3_move_data(source_bucket=landing_zone, target_bucket=working_zone)
            logger.info("Data moved to Working Zone successfully")
            
        except Exception as e:
            logger.error(f"Failed to move data from Landing Zone: {str(e)}", exc_info=True)
            raise
        
        # ====================================================================
        # Phase 3: Get files from Working Zone and validate
        # ====================================================================
        
        logger.info("Phase 3: Retrieving files from Working Zone...")
        
        try:
            files_in_working_zone = gds3.get_files(working_zone)
            logger.info(f"Found {len(files_in_working_zone)} files in Working Zone: {files_in_working_zone}")
            
            if not files_in_working_zone:
                logger.warning("No files found in Working Zone. Pipeline will skip transformations.")
            
        except Exception as e:
            logger.error(f"Failed to retrieve files from Working Zone: {str(e)}", exc_info=True)
            raise
        
        # ====================================================================
        # Phase 4: Cleanup Processed Zone and Transform Data
        # ====================================================================
        
        logger.info("Phase 4: Cleaning Processed Zone and transforming data...")
        
        # Check if any expected files exist in working zone
        expected_files = set(modules.keys())
        available_files = set(files_in_working_zone)
        matching_files = expected_files & available_files
        
        if matching_files:
            try:
                logger.info(f"Cleaning Processed Zone to prepare for new data...")
                gds3.clean_bucket(processed_zone)
                logger.info("Processed Zone cleaned successfully")
            except Exception as e:
                logger.error(f"Failed to clean Processed Zone: {str(e)}", exc_info=True)
                raise
            
            # Transform each file
            for file in files_in_working_zone:
                if file in modules.keys():
                    file_type, transform_func = modules[file]
                    
                    try:
                        logger.info(f"Starting transformation for {file_type} dataset...")
                        start_transform = datetime.now()
                        
                        transform_func()
                        
                        duration = (datetime.now() - start_transform).total_seconds()
                        logger.info(f"{file_type} dataset transformed successfully in {duration:.2f} seconds")
                        
                    except Exception as e:
                        logger.error(f"Failed to transform {file_type} dataset ({file}): {str(e)}", exc_info=True)
                        raise
                else:
                    logger.warning(f"File {file} found in Working Zone but no transformation defined")
        else:
            logger.warning("No matching data files found in Working Zone for transformation")
        
        # ====================================================================
        # Phase 5: Warehouse Operations
        # ====================================================================
        
        logger.info("Phase 5: Setting up and loading Redshift warehouse...")
        
        try:
            # Small delay to ensure Spark jobs are completed
            time.sleep(5)
            
            grwarehouse = GoodreadsWarehouseDriver()
            
            # Setup staging tables
            logger.info("Setting up staging tables...")
            grwarehouse.setup_staging_tables()
            logger.info("Staging tables created successfully")
            
            # Load data to staging tables
            logger.info("Loading data to staging tables...")
            grwarehouse.load_staging_tables()
            logger.info("Staging tables loaded successfully")
            
            # Setup warehouse tables
            logger.info("Setting up warehouse tables...")
            grwarehouse.setup_warehouse_tables()
            logger.info("Warehouse tables created successfully")
            
            # Perform UPSERT operation
            logger.info("Performing UPSERT operation on warehouse tables...")
            grwarehouse.upsert_warehouse_tables()
            logger.info("UPSERT operation completed successfully")
            
        except Exception as e:
            logger.error(f"Warehouse operations failed: {str(e)}", exc_info=True)
            raise
        
        # ====================================================================
        # Pipeline Complete
        # ====================================================================
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60  # Convert to minutes
        
        logger.info("=" * 80)
        logger.info(f"Books User ETL Pipeline completed successfully!")
        logger.info(f"Total execution time: {duration:.2f} minutes")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"Books User ETL Pipeline FAILED with error: {str(e)}", exc_info=True)
        logger.error("=" * 80)
        return 1
        
    finally:
        # Clean up Spark session
        if spark:
            try:
                logger.info("Stopping Spark session...")
                spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.warning(f"Error while stopping Spark session: {str(e)}")


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    """
    Entry point for the Books ETL driver script
    """
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {str(e)}", exc_info=True)
        sys.exit(1)
