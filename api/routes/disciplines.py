"""Disciplines API endpoints."""
from flask_restful import Resource


class DisciplineListResource(Resource):
    """Handle /api/disciplines endpoint."""
    
    def get(self):
        """Get all disciplines."""
        # TODO: Implement in controller
        return {'message': 'Get all disciplines - not implemented yet'}, 501
    
    def post(self):
        """Create new discipline."""
        # TODO: Implement in controller
        return {'message': 'Create discipline - not implemented yet'}, 501


class DisciplineResource(Resource):
    """Handle /api/disciplines/<id> endpoint."""
    
    def get(self, id):
        """Get discipline by ID."""
        # TODO: Implement in controller
        return {'message': f'Get discipline {id} - not implemented yet'}, 501
    
    def put(self, id):
        """Update discipline by ID."""
        # TODO: Implement in controller
        return {'message': f'Update discipline {id} - not implemented yet'}, 501
    
    def delete(self, id):
        """Delete discipline by ID."""
        # TODO: Implement in controller
        return {'message': f'Delete discipline {id} - not implemented yet'}, 501
