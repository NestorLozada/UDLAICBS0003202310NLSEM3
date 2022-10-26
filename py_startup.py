from config import DbConfig
import extract
import traceback

from util import db_connection

try:
    extract.extract ( )
except:
    traceback.print_exc()