class DataProcessError(Exception):
    pass

class NameMismatchError(DataProcessError):
    def __init__(self, expected_name, actual_name):
        self.message = f"Expected: {expected_name}; Actual: {actual_name}"
        super().__init__(self.message)

class DateTypeError(Exception):
    pass
    # TODO check runtime_status

class DateRangeError(Exception):
    pass
