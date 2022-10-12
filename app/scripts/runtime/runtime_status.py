# print(f"Data source {self.data_src} verified.")
class RuntimeStatus:
    def __init__(self, message="This is a base class."):
        self.message = message

    def __str__(self):
        return f"{__class__.__name__}: {self.message}"

class PathExistStatus(RuntimeStatus):
    def __init__(self, path, exist):
        if exist:
            super().__init__(message=f"Verified >> {path}")
        else:
            super().__init(message=f"Not found >> {path}")

class PathCreated(RuntimeStatus):
    def __init__(self, path):
        super().__init__(message=path)

# class DataDateRangeError(Exception):
#     def __init__(self, start, end):
#         self.start = start
#         self.end = end
#         self.pass_all_checks = True
#         self._check()
#
#         super().__init__(self.message)
#
#     def _check(self):
#         self._check_type()
#         self._check_date_range()
#         if self.pass_all_checks:
#             self.message = ""
#
#     def _check_type(self):
#         try:
#             assert isinstance(self.start, date)
#         except AssertionError:
#             self.message = self.message + "\nStart " + self.raise_datetype_error()
#
#         try:
#             assert isinstance(self.end, date)
#         except AssertionError:
#             self.message = self.message + "\nEnd " + self.raise_datetype_error()
#
#     def _check_date_range(self):
#         try:
#             assert self.start <= self.end
#         except AssertionError:
#             self.pass_all_checks = False
#             self.message = self.message + "\nStart date must be <= end date"
#
#     def raise_datetype_error(self):
#         self.pass_all_checks = False
#         return "date must be a datetime.date object"
# TODO Add to data_process.py
class DataRetreived(RuntimeStatus):
    pass

class DataProcessed(RuntimeStatus):
    pass

class DataStored(RuntimeStatus):
    pass
