import datetime

class Utils:

    @staticmethod
    def local_now():
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)