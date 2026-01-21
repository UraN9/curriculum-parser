"""Health check endpoint."""
from flask_restful import Resource


class HealthResource(Resource):
    """API Health check."""
    
    def get(self):
        """
        Check API health status.
        
        Returns:
            dict: Health status
        """
        return {
            'status': 'healthy',
            'service': 'curriculum-parser-api',
            'version': '1.0.0'
        }
