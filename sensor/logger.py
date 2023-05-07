import logging
import os,sys
from datetime import datetime

#create log file name
log_filename = f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"

#create log dir
log_dir = os.path.join(os.getcwd(),"logs")
os.makedirs(log_dir,exist_ok=True)

#create log file path
log_filepath = os.path.join(log_dir,log_filename)

#log basic config
logging.basicConfig(
    filename = log_filepath,
    format = "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level = logging.INFO
)

