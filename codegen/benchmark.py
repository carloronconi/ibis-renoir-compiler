import logging
import codegen.utils as utl
from datetime import datetime


class Benchmark:
    def __init__(self, name):
        self.name = name
        self.logger = setup_logger()

    def set_renoir_compile(self, time: float):
        self.renoir_compile_time = time

    def set_renoir_execute(self, time: float):
        self.renoir_execute_time = time

    def set_ibis(self, time: float):
        self.ibis_time = time

    def log(self):
        message = f"{self.name},{self.renoir_compile_time:.10f}s,{self.renoir_execute_time:.10f}s,{self.ibis_time:.10f}s"
        self.logger.info(message)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("codegen_log")
    if not logger.hasHandlers():
        handler = logging.FileHandler(
            utl.ROOT_DIR + "/log/codegen_log.csv", mode='a')
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
