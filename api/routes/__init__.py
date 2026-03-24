"""
API Routes Registration.

Registers all API resources (endpoints).
"""
from flask_restful import Api


def register_routes(api: Api):
    """
    Register all API routes.
    
    Args:
        api: Flask-RESTful Api instance
    """
    # Import resources
    from api.routes.health import HealthResource
    from api.routes.disciplines import DisciplineListResource, DisciplineResource
    from api.routes.sections import SectionListResource, SectionResource
    from api.routes.auth import AuthRegister, AuthLogin, AuthMe
    
    # Health check
    api.add_resource(HealthResource, '/health')
    
    # Authentication
    api.add_resource(AuthRegister, '/auth/register')
    api.add_resource(AuthLogin, '/auth/login')
    api.add_resource(AuthMe, '/auth/me')
    
    # Disciplines
    api.add_resource(DisciplineListResource, '/disciplines')
    api.add_resource(DisciplineResource, '/disciplines/<int:id>')
    
    # Sections
    api.add_resource(SectionListResource, '/sections')
    api.add_resource(SectionResource, '/sections/<int:id>')


def register_etl_routes(app):
    """
    Register ETL Blueprint routes.
    
    Args:
        app: Flask application instance
    """
    from api.routes.etl import etl_bp
    app.register_blueprint(etl_bp, url_prefix='/api/etl')


def register_upload_routes(app):
    """
    Register Upload Blueprint routes.
    
    Args:
        app: Flask application instance
    """
    from api.routes.upload import upload_bp
    app.register_blueprint(upload_bp, url_prefix='/api/upload')


def register_topics_routes(app):
    """
    Register Topics Blueprint routes.
    
    Args:
        app: Flask application instance
    """
    from api.routes.topics import topics_bp
    app.register_blueprint(topics_bp, url_prefix='/api/topics')


def register_schedule_routes(app):
    """
    Register Schedule Blueprint routes.
    
    Args:
        app: Flask application instance
    """
    from api.routes.schedule import schedule_bp
    app.register_blueprint(schedule_bp, url_prefix='/api/schedule')
