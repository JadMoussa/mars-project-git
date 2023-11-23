import logging
from lookups import LogLevels


class Logger:
    def _init_(self, log_file=None):
        self.log_file = log_file  
        self._log_level = logging.DEBUG 
        self._configure_logger()

    def _configure_logger(self):
        
        mode = 'w' if self.log_file else 'a'
        logging.basicConfig(
            format="%(asctime)s;%(levelname)s;%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=self._log_level,
            handlers=[
                logging.FileHandler(
                    self.log_file, mode=mode) if self.log_file else logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger()

    def log_message(self, inputLogLevel, log_mess):
        pass
        if inputLogLevel == LogLevels.DEBUG:
            self.logger.debug(log_mess)
        elif inputLogLevel == LogLevels.INFO:
            self.logger.info(log_mess)
        elif inputLogLevel == LogLevels.ERROR:
            self.logger.error(log_mess)
        elif inputLogLevel == LogLevels.WARNING:
            self.logger.warning(log_mess)