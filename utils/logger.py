import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional
from .config_manager import ConfigManager

def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Set up logger with file and console handlers.
    
    Args:
        name (Optional[str]): Logger name
        
    Returns:
        logging.Logger: Configured logger instance
    """
    config = ConfigManager()
    
    # Create logs directory if it doesn't exist
    log_file = config.get('logging.file', 'logs/pipeline.log')
    log_dir = os.path.dirname(log_file)
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Get logger
    logger = logging.getLogger(name or __name__)
    logger.setLevel(config.get('logging.level', 'INFO'))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=config.get('logging.max_size_mb', 10) * 1024 * 1024,
        backupCount=config.get('logging.backup_count', 5)
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name (Optional[str]): Logger name
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return setup_logger(name) 