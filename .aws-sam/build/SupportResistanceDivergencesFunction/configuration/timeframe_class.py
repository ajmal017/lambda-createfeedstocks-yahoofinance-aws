from enum import Enum
class IBTraderTimeFrame(Enum):
    DAY  = "DAY"
    WEEK  = "WEEK"
    MONTH = "MONTH"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, IBTraderTimeFrame))