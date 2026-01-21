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
    from api.routes.themes import ThemeListResource, ThemeResource
    from api.routes.activities import ActivityListResource, ActivityResource
    
    # Health check
    api.add_resource(HealthResource, '/health')
    
    # Disciplines
    api.add_resource(DisciplineListResource, '/disciplines')
    api.add_resource(DisciplineResource, '/disciplines/<int:id>')
    
    # Sections
    api.add_resource(SectionListResource, '/sections')
    api.add_resource(SectionResource, '/sections/<int:id>')
    
    # Themes
    api.add_resource(ThemeListResource, '/themes')
    api.add_resource(ThemeResource, '/themes/<int:id>')
    
    # Activities
    api.add_resource(ActivityListResource, '/activities')
    api.add_resource(ActivityResource, '/activities/<int:id>')
