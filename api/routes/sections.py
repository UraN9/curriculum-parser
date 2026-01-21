"""Sections API endpoints."""
from flask_restful import Resource


class SectionListResource(Resource):
    """Handle /api/sections endpoint."""
    
    def get(self):
        """Get all sections."""
        # TODO: Implement in controller
        return {'message': 'Get all sections - not implemented yet'}, 501
    
    def post(self):
        """Create new section."""
        # TODO: Implement in controller
        return {'message': 'Create section - not implemented yet'}, 501


class SectionResource(Resource):
    """Handle /api/sections/<id> endpoint."""
    
    def get(self, id):
        """Get section by ID."""
        # TODO: Implement in controller
        return {'message': f'Get section {id} - not implemented yet'}, 501
    
    def put(self, id):
        """Update section by ID."""
        # TODO: Implement in controller
        return {'message': f'Update section {id} - not implemented yet'}, 501
    
    def delete(self, id):
        """Delete section by ID."""
        # TODO: Implement in controller
        return {'message': f'Delete section {id} - not implemented yet'}, 501
