import configparser
import logging
import psycopg2
from src.warehouse.booksread_staging_queries import *
from src.warehouse.booksread_warehouse_queries import *
from src.warehouse.booksread_upsert import *
from pathlib import Path


logger = logging.getLogger(__name__)
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

class GoodReadsWarehouseDriver:

    def __init__(self):
        self._conn = psycopg2.connect("host={} dbname={} user={} password={}".format(*config['CLUSTER'].values()))
        self._cur = self._conn.cursor()
    
    def setup_staging_tables(self):
        logging.debug("Setting up staging tables")
        self.execute_query([create_staging_schema])

        logging.debug("Dropping staging tables if they exist")
        self.execute_query([drop_staging_tables])

        logging.debug("Creating staging tables")
        self.execute_query([create_staging_tables])
    
    def load_staging_tables(self):
        logging.debug("Loading staging tables")
        self.execute_query(copy_staging_tables)

    def setup_warehouse_tables(self):
        logging.debug("Creating schema for warehouse tables")
        self.execute_query([create_warehouse_schema])

        logging.debug("Creating Warehouse tables")
        self.execute_query([create_warehouse_tables])
    
    def perform_upsert(self):
        logging.debug("Performing upsert to warehouse tables")
        self.execute_query(upsert_queries)
    
    def execute_query(self, query_list):
        for query in query_list:
            print(query)
            logging.debug(f"Executing query: {query}")
            self._cur.execute(query)
            self._conn.commit()
    