import logging
import codegen.utils as utl
import os.path
from datetime import datetime


class Benchmark:
    def __init__(self, name):
        self.name = name
        self.logger = setup_logger()
        self.total_time = -1
        self.renoir_compile_time = -1
        self.renoir_execute_time = -1
        self.ibis_time = -1
        self.run_count = -1
        self.backend_name = "renoir"

    def log(self):
        message = f"{self.name},{self.backend_name},{self.run_count},{self.total_time:.10f}s,{self.renoir_compile_time:.10f}s,{self.renoir_execute_time:.10f}s,{self.ibis_time:.10f}s"
        self.logger.info(message)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("codegen_log")
    file = utl.ROOT_DIR + "/log/codegen_log.csv"
    if not os.path.isfile(file):
        with open(file, "w") as f:
            f.write(
                "level,timestamp,test_name,backend_name,run_count,renoir_compile_time,renoir_execution_time,ibis_total_time\n")
    if not logger.hasHandlers():
        handler = logging.FileHandler(file, mode='a')
        handler.setFormatter(CustomFormatter(
            "%(levelname)s,%(asctime)s,%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


class CustomFormatter(logging.Formatter):
    converter = datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
            return s
        else:
            return ct.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
