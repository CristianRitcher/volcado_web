"""
Flask Configuration File
Database Consolidation System Configuration
"""

import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent

class Config:
    """Base configuration class"""
    
    # Flask settings
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    JSON_AS_ASCII = False  # Support for UTF-8 characters
    
    # Application settings
    APP_NAME = "Volcado Web - Database Consolidation System"
    APP_VERSION = "2.0.0-flask"
    
    # File paths
    ASSETS_DIR = BASE_DIR / 'assets'
    STATIC_DIR = BASE_DIR / 'static'
    TEMPLATES_DIR = BASE_DIR / 'templates'
    
    # Database consolidation settings
    ALIAS_FILE = ASSETS_DIR / 'alias.json'
    LOG_FILE = BASE_DIR / 'db_consolidation.log'
    SNAPSHOT_FILE = BASE_DIR / 'consolidation_snapshot.json'
    FAILURES_FILE = BASE_DIR / 'consolidation_failures.json'
    
    # Sync script settings
    SYNC_SCRIPT = BASE_DIR / 'sync.py'
    SYNC_TIMEOUT = 300  # 5 minutes
    
    # Logging configuration
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    FLASK_ENV = 'development'

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    FLASK_ENV = 'production'
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'change-this-in-production'

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
