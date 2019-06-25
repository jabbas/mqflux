from datetime import date, datetime
import json


def dt(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s is not serializable".format(type(obj)))


def pprint(obj):
    print(json.dumps(obj, indent=4, sort_keys=True, default=dt))
