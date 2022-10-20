import extract
import traceback

try:
    extract.extract()
except:
    traceback.print_exc()