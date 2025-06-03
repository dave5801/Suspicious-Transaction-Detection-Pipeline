import yaml
import os
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self.load_config()

    def load_config(self, config_path: str = "config.yaml") -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_path (str): Path to the configuration file
        """
        try:
            with open(config_path, 'r') as file:
                self._config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key (str): Configuration key (e.g., 'database.name')
            default (Any): Default value if key not found
            
        Returns:
            Any: Configuration value
        """
        try:
            value = self._config
            for k in key.split('.'):
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def get_all(self) -> Dict:
        """
        Get all configuration values.
        
        Returns:
            Dict: Complete configuration dictionary
        """
        return self._config.copy()

    def validate_config(self) -> bool:
        """
        Validate configuration values.
        
        Returns:
            bool: True if configuration is valid
        """
        required_sections = ['database', 'analysis', 'logging', 'io']
        for section in required_sections:
            if section not in self._config:
                logger.error(f"Missing required configuration section: {section}")
                return False
        return True 