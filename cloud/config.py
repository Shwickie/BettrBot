# config.py - Environment configuration for cloud deployment
"""
Centralized configuration for local development and cloud deployment
"""

import os
from urllib.parse import quote_plus

class Config:
    """Base configuration"""
    
    # Environment detection
    IS_PRODUCTION = os.environ.get('FLASK_ENV') == 'production'
    IS_CLOUD = os.environ.get('DATABASE_URL') is not None
    
    # Database Configuration
    if IS_CLOUD or IS_PRODUCTION:
        # Production/Cloud database (PostgreSQL)
        DATABASE_URL = os.environ.get('DATABASE_URL') or "postgresql://postgres:ApeNuts123!@db.bmfwrdsastxbsbubuuhs.supabase.co:5432/postgres"
        
        # Handle both formats
        if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
            DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
            
    else:
        # Local development (SQLite)
        DATABASE_URL = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
    
    # API Keys (set as environment variables)
    ODDS_API_KEY = os.environ.get('ODDS_API_KEY', 'your-odds-api-key')
    ESPN_API_KEY = os.environ.get('ESPN_API_KEY', 'your-espn-api-key')
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    FLASK_PORT = int(os.environ.get('PORT', 5000))
    FLASK_HOST = '0.0.0.0' if IS_PRODUCTION else '127.0.0.1'
    
    # Cache and Performance
    CACHE_TIMEOUT = 300  # 5 minutes
    MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 4))
    
    # Data Pipeline Settings
    PIPELINE_TIMEOUT = int(os.environ.get('PIPELINE_TIMEOUT', 1800))  # 30 minutes
    UPDATE_FREQUENCY = os.environ.get('UPDATE_FREQUENCY', 'hourly')  # hourly, daily, weekly
    
    # Logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    
    @classmethod
    def get_database_url(cls):
        """Get the appropriate database URL"""
        return cls.DATABASE_URL
    
    @classmethod
    def is_cloud_environment(cls):
        """Check if running in cloud"""
        return cls.IS_CLOUD or cls.IS_PRODUCTION

# Environment-specific configurations
class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    TESTING = False
    
    # Security settings for production
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True
    DATABASE_URL = "sqlite:///:memory:"

# Configuration selector
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

def get_config():
    """Get the appropriate configuration"""
    env = os.environ.get('FLASK_ENV', 'development')
    return config.get(env, config['default'])