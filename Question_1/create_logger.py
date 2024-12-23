import logging
import os


    # This class is responsible for logging activities into a log file.
    # It supports multiple logging levels and handles both the creation and management 
    # of the logger and log file.
class LogActivities:
    

# Initializes the LogActivities object with a logger name and log level.

    def __init__(self, logger_name: str, log_level: str):
        
        
        self.logger_name = logger_name
        self.log_level = log_level
        
        # Mapping of log level strings to corresponding logging module constants
        self._log_levels = {"critical": logging.CRITICAL,
                      "debug": logging.DEBUG,
                      "error": logging.ERROR,
                      "fatal": logging.FATAL,
                      "warning": logging.WARNING,
                      "info": logging.INFO}
        
        # Create the logger instance
        self._logger = self._create_logger(self.logger_name, self.log_level)

    
    def get_logger(self)-> logging.RootLogger:
        

        
        return self._logger
        
# Creates and configures the logger, setting the appropriate logging level 
# and adding a file handler to log messages to a file. 
    def _create_logger(self, logger_name: str, log_level: str)-> logging.RootLogger:
        
      
        
        log_filename = f"{logger_name}_log.log"  # Log file is named based on the logger name
        
        # If the log file exists, print a message indicating that new logs will be appended.
        if os.path.exists(log_filename):
            print(f"{log_filename} already exists. Appending new logs")
        else:
            print(f"Creating new log file, {log_filename}")
        
        # Set up the logger
        logger = logging.getLogger(logger_name)
        
        # Set up the file handler and formatter for logging
        log_handler = logging.FileHandler(log_filename)
        log_handler.setFormatter(logging.Formatter('%(asctime)s - Function: %(funcName)s - %(message)s'))
        
        # Add the handler to the logger
        logger.addHandler(log_handler)
        
        # Set the log level based on the provided log level string
        logger.setLevel(self._log_levels[log_level])
        
        return logger
    
# Determines which log levels are allowed to be logged based on the given log level.

    def _get_allowed_logs(self):
        

        
        log_levels = ["fatal", "critical", "error", "warning", "info", "debug"]
        log_level_index = log_levels.index(self.log_level)
        return log_levels[:log_level_index + 1]
    
    
    # Log functions for each log level
    def _log_info(self, message: str):
        logger = self.get_logger()
        logger.info(message)
        
    def _log_debug(self, message: str):
        logger = self.get_logger()
        logger.debug(message)
        
    def _log_warning(self, message: str):
        logger = self.get_logger()
        logger.warning(message)
        
    def _log_error(self, message: str):
        logger = self.get_logger()
        logger.error(message)
        
    def _log_critical(self, message: str):
        logger = self.get_logger()
        logger.critical(message)
        
    def _log_fatal(self, message: str):
        logger = self.get_logger()
        logger.fatal(message)


# Selects the appropriate log function based on the current log level.
      
    def _log_selector(self):
        

        
        return {"critical": self._log_critical,
                "fatal": self._log_fatal,
                "info": self._log_info,
                "debug": self._log_debug,
                "warning": self._log_warning,
                "error": self._log_error}
        
    def log_message(self, message: str):
        

        
        logger = self.get_logger()
        allowed_logs = self._get_allowed_logs()
            
        min_importance_log = allowed_logs[-1]
        
        self._log_selector()[min_importance_log](message)
        
        
    