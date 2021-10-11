from log import Logger

class BaseException(Exception):
     def __init__(self, message):
        self.logger = Logger.getInstance()
        self.logger.fatal(message)


class ConfigurationException(BaseException):
    def __init__(self, message="Incorrect inputs in config file"):
        super().__init__(message)

class IncorrectStatusException(BaseException):
    def __init__(self, message="Incorrect processing status"):
        super().__init__(message)


class IncorrectInputsException(BaseException):
    def __init__(self, message="Incorrect arguments provided"):
        super().__init__(message)