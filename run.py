"""
Flask Application Entry Point.

Run this file to start the REST API server.

Usage:
    python run.py
    
Or with Flask CLI:
    flask --app run:app run --debug
"""
from api import create_app

app = create_app()

if __name__ == '__main__':
    print("🚀 Starting Curriculum Parser REST API...")
    print("📍 API endpoints available at: http://localhost:5000/api/")
    print("❤️  Health check: http://localhost:5000/health")
    print("-" * 50)
    app.run(host='0.0.0.0', port=5000, debug=True)
