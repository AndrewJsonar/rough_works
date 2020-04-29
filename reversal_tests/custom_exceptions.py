class ReversalExceptions(Exception):
    pass


class InvalidTime(ReversalExceptions):
    def __init__(self):
        super(InvalidTime, self).__init__(
            "No data for entire month."
        )
