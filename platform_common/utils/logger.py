import os
import logging
import sys
def setup_logger(log_level=logging.INFO):
    dir_path = "logs"
    os.makedirs(dir_path, exist_ok=True)
    filename = f"{dir_path}/app.log"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(filename),
            logging.StreamHandler(sys.stdout),
        ],
    )
def debug(message: str, job_id: str = None) -> None:
    if job_id:
        logging.debug(f"[{job_id}] - {message}")
    else:
        logging.debug(message)
def info(message: str, job_id: str = None) -> None:
    if job_id:
        logging.info(f"[{job_id}] - {message}")
    else:
        logging.info(message)
def warning(message: str, job_id: str = None) -> None:
    if job_id:
        logging.warning(f"[{job_id}] - {message}")
    else:
        logging.warning(message)
def error(message: str, job_id: str = None) -> None:
    if job_id:
        logging.error(f"[{job_id}] - {message}")
    else:
        logging.error(message)
def critical(message: str, job_id: str = None) -> None:
    if job_id:
        logging.critical(f"[{job_id}] - {message}")
    else:
        logging.critical(message)
def exception(message: str, job_id: str = None) -> None:
    if job_id:
        logging.exception(f"[{job_id}] - {message}")
    else:
        logging.exception(message)
