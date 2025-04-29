
import yaml
import logging
import os
from data_producer import produce_data
from spark_job import run_spark_job

def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def main():
    with open('configs/config.yaml') as f:
        config = yaml.safe_load(f)
    
    setup_logging()
    logging.info("Starting data pipeline")

    logging.info("Producing data to Kafka topic")
    produce_data(config)

    logging.info("Running Spark processing job")
    run_spark_job(config)

    logging.info("Data pipeline execution completed")

if __name__ == "__main__":
    main()
