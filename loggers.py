import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import datetime
import os
import yaml

def configure_logger(log_level, log_file):
    logger = logging.getLogger(log_file)
    logger.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(module)s:%(funcName)s:%(lineno)d:%(message)s')
    file_handler = logging.FileHandler(f'{log_file}_NPF_bifercation_logs.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger