import logging
import logging.config

# Load the Logging Configuration File.
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def extract_files(df, format, filePath, split_no, headerReq, compressionType):
    try:
        logger.info(" Ectraction - extract_files() is started...")
        if format == "parquet":
            df.write \
                .format("parquet") \
                .partitionBy("partition_column") \
                .save("output_path")
        elif format == "csv":
            df.coalesce(split_no) \
                .write \
                .format(format) \
                .save(filePath, header=headerReq, compression=compressionType)
    except Exception as exp:
        logger.error("Error in method - extract_files(). Please check stack trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Extraction - extract_files() is completed")