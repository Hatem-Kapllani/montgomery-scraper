"""
Logger module for the application.

Features:
- Supports multiple log levels: error, warn, info, debug
- Uses daily rotate file for log rotation
- Has separate error and combined logs
- Supports structured logging with metadata
- Has error formatting capabilities
"""

import os
import logging
import json
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from typing import Dict, Any, Optional, Union

# Create logs directory if it doesn't exist
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

class Logger:
    """Custom logger with Winston-like features"""
    
    def __init__(self, name: str = "app"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.setup_handlers()
    
    def setup_handlers(self):
        """Set up console and file handlers"""
        # Clear any existing handlers
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        
        # Combined log file with daily rotation
        combined_handler = TimedRotatingFileHandler(
            filename=os.path.join("logs", "combined.log"),
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8"
        )
        combined_handler.setLevel(logging.DEBUG)
        combined_handler.setFormatter(console_format)
        
        # Error log file with daily rotation
        error_handler = TimedRotatingFileHandler(
            filename=os.path.join("logs", "error.log"),
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(console_format)
        
        # Add handlers to logger
        self.logger.addHandler(console_handler)
        self.logger.addHandler(combined_handler)
        self.logger.addHandler(error_handler)
    
    def _format_message(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Format message with metadata if provided"""
        if metadata:
            try:
                metadata_str = json.dumps(metadata)
                return f"{message} {metadata_str}"
            except Exception:
                return f"{message} [Error serializing metadata]"
        return message
    
    def _format_error(self, error: Exception) -> Dict[str, Any]:
        """Format error details into a structured object"""
        error_details = {
            "type": error.__class__.__name__,
            "message": str(error),
            "timestamp": datetime.now().isoformat(),
        }
        
        if hasattr(error, "__traceback__"):
            import traceback
            error_details["stacktrace"] = traceback.format_exc()
        
        return error_details
    
    def debug(self, message: str, metadata: Optional[Dict[str, Any]] = None):
        """Log debug message with optional metadata"""
        self.logger.debug(self._format_message(message, metadata))
    
    def info(self, message: str, metadata: Optional[Dict[str, Any]] = None):
        """Log info message with optional metadata"""
        self.logger.info(self._format_message(message, metadata))
    
    def warn(self, message: str, metadata: Optional[Dict[str, Any]] = None):
        """Log warning message with optional metadata"""
        self.logger.warning(self._format_message(message, metadata))
    
    def error(self, message: str, error: Optional[Exception] = None, metadata: Optional[Dict[str, Any]] = None):
        """Log error message with optional error details and metadata"""
        if error:
            error_details = self._format_error(error)
            if metadata:
                metadata.update({"error": error_details})
            else:
                metadata = {"error": error_details}
        
        self.logger.error(self._format_message(message, metadata))

# Create a default logger instance
logger = Logger()

def get_logger(name: str = "app") -> Logger:
    """Get a logger instance with the specified name"""
    return Logger(name) 