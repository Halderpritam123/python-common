import datetime
from json import JSONEncoder
class DateTimeEncoder(JSONEncoder):
    # Override the default method
Uncovered code
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()