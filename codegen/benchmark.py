import logging
import codegen.utils as utl
import os.path
from datetime import datetime


class Benchmark:
    def __init__(self, name, dir=None):
        self.test_name = name
        self.backend_name = "renoir"
        self.run_count = -1
        self.total_time_s = -1
        self.renoir_compile_time_s = -1
        self.renoir_execute_time_s = -1
        self.ibis_time_s = -1
        self.max_memory_MiB = -1
        self.table_origin = "None"
        self.exception = "None"
        self.logger = setup_logger(dir, self)

    def log(self):
        message = ""
        for attr, val in self.__dict__.items():
            if attr == "logger":
                continue
            message += f"{val},"
        message = message[:-1]
        self.logger.info(message)


def setup_logger(dir, bench: Benchmark) -> logging.Logger:
    name = f"{dir}/codegen_log" if dir else "codegen_log"
    logger = logging.getLogger(name)
    file = utl.ROOT_DIR + f"/log/{name}.csv"
    if not os.path.isfile(file):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        header = "level,timestamp,"
        for attr in bench.__dict__.keys():
            if attr == "logger":
                continue
            header += f"{attr},"
        header = header[:-1]
        with open(file, "w") as f:
            f.write(header + "\n")
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
