"""
Flask REST API Application Factory.

This module creates and configures the Flask application.
"""
from flask import Flask
from flask_restful import Api

from api.config import Config


def create_app(config_class=Config):
    """
    Application factory pattern for Flask.
    
    Args:
        config_class: Configuration class to use
        
    Returns:
        Flask application instance
    """
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Initialize Flask-RESTful
    api = Api(app, prefix='/api')
    
    # Register routes
    from api.routes import register_routes
    register_routes(api)
    
    # Health check endpoint (without /api prefix)
    @app.route('/health')
    def health():
        return {'status': 'healthy', 'service': 'curriculum-parser-api'}
    
    return app
