import logging, os

class Logger:
    project = os.path.basename(os.getcwd())
    module = os.path.splitext(os.path.basename(__name__))[0]
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_level = logging.DEBUG
    
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(f'{self.__class__.project}.{self.__class__.module}.{self.__class__.__name__}')
        self.logger.info(f'Instantiate {self.__class__.__name__}')
    
    def track(func):
        def tmp(self, *args, **kwargs):
            self.logger.info(f'Start {self.__class__.__name__}.{func.__name__} execution')
            r = func(self, *args, **kwargs)
            self.logger.info(f'Finish {self.__class__.__name__}.{func.__name__} execution')
            return r
        return tmp
    
    @classmethod
    def get_project_logger(cls, log_level = logging.DEBUG):
        project_logger = logging.getLogger(cls.project)
        project_logger.setLevel(log_level)
        return project_logger
    
    @classmethod
    def show_stream_log(cls, logger = None, log_level = None, formatter = None):
        
        log_level = log_level or cls.log_level
        logger = logger or cls.get_project_logger()
        formatter = formatter or cls.formatter

        stream_log = logging.StreamHandler()
        stream_log.setLevel(log_level)
        stream_log.setFormatter(formatter)
        logger.addHandler(stream_log)

    @classmethod
    def save_log(cls, filename = None, logger = None, log_level = None, formatter = None):
        
        filename = filename or f'{cls.module}.log'
        log_level = log_level or cls.log_level
        logger = logger or cls.get_project_logger()
        formatter = formatter or cls.formatter
        
        fh = logging.FileHandler(filename)
        fh.setLevel(log_level)
        fh.setFormatter(cls.formatter)
        logger.addHandler(fh)
   
    @classmethod
    def get_module_logger(cls):
        return logging.getLogger(f'{cls.project}.{cls.module}')

if __name__ == "__main__":
    pass