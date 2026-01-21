"""Activities API endpoints."""
from flask_restful import Resource


class ActivityListResource(Resource):
    """Handle /api/activities endpoint."""
    
    def get(self):
        """Get all activities."""
        # TODO: Implement in controller
        return {'message': 'Get all activities - not implemented yet'}, 501
    
    def post(self):
        """Create new activity."""
        # TODO: Implement in controller
        return {'message': 'Create activity - not implemented yet'}, 501


class ActivityResource(Resource):
    """Handle /api/activities/<id> endpoint."""
    
    def get(self, id):
        """Get activity by ID."""
        # TODO: Implement in controller
        return {'message': f'Get activity {id} - not implemented yet'}, 501
    
    def put(self, id):
        """Update activity by ID."""
        # TODO: Implement in controller
        return {'message': f'Update activity {id} - not implemented yet'}, 501
    
    def delete(self, id):
        """Delete activity by ID."""
        # TODO: Implement in controller
        return {'message': f'Delete activity {id} - not implemented yet'}, 501
