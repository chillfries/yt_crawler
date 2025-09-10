import logging
import os
from datetime import datetime

class Logger:
    def __init__(self, log_file_name, log_level="INFO"):
        self.logger = logging.getLogger(log_file_name)
        self.logger.setLevel(log_level)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Console handler
        if not any(isinstance(h, logging.StreamHandler) for h in self.logger.handlers):
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)
        
        # File handler
        if not any(isinstance(h, logging.FileHandler) for h in self.logger.handlers):
            logs_dir = "logs"
            os.makedirs(logs_dir, exist_ok=True)
            log_path = os.path.join(logs_dir, log_file_name)
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger