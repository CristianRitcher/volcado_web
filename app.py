from flask import Flask, request, jsonify, render_template, send_from_directory
import subprocess
import json
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # Support for UTF-8 characters

# Configuration
BASE_DIR = Path(__file__).parent
ASSETS_DIR = BASE_DIR / 'assets'
STATIC_DIR = BASE_DIR / 'static'
TEMPLATES_DIR = BASE_DIR / 'templates'

@app.route('/')
def index():
    """Serve the main page"""
    return render_template('index.html')

@app.route('/assets/<path:filename>')
def serve_assets(filename):
    """Serve static assets (CSS, JS, JSON)"""
    return send_from_directory('assets', filename)

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory('static', filename)

@app.route('/api/sync_db', methods=['POST'])
def sync_database():
    """
    Main API endpoint for database synchronization
    Replaces the PHP api.php functionality
    """
    try:
        # Get JSON data from request
        data = request.get_json()
        
        if not data:
            return jsonify({
                "status": "error",
                "message": "No JSON data received"
            }), 400

        logger.info(f"Received sync request: {data}")

        # Validate required fields
        if 'modo' not in data:
            return jsonify({
                "status": "error",
                "message": 'El campo "modo" es requerido. Use "apertura" o "cierre"'
            }), 400

        # Extract and validate data
        modo = data['modo']
        if modo not in ['apertura', 'cierre']:
            return jsonify({
                "status": "error",
                "message": 'El modo debe ser "apertura" o "cierre"'
            }), 400

        # Extract database configurations
        db_origen = data.get('db_origen', {})
        db_destino = data.get('db_destino', {})
        db_password_origen = data.get('db_password_origen', '')
        db_password_destino = data.get('db_password_destino', '')

        # Validate database configurations
        required_fields = ['host', 'user', 'database']
        for field in required_fields:
            if field not in db_origen:
                return jsonify({
                    "status": "error",
                    "message": f'Campo requerido en db_origen: {field}'
                }), 400
            if field not in db_destino:
                return jsonify({
                    "status": "error",
                    "message": f'Campo requerido en db_destino: {field}'
                }), 400

        # Extract configuration values
        db_origen_alias = db_origen.get('alias', 'origen')
        db_origen_host = db_origen['host']
        db_origen_user = db_origen['user']
        db_origen_database = db_origen['database']

        db_destino_host = db_destino['host']
        db_destino_user = db_destino['user']
        db_destino_database = db_destino['database']

        # Build command for sync.py
        cmd = [
            'python3', 'sync.py',
            f'{db_destino_host}:{db_destino_user}:{db_password_destino}:{db_destino_database}',
            '--sources', f'{db_origen_alias}={db_origen_host}:{db_origen_user}:{db_password_origen}:{db_origen_database}',
            '--modo', modo
        ]

        logger.info(f"Executing command: {' '.join(cmd)}")

        # Execute sync.py
        result = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )

        logger.info(f"Command return code: {result.returncode}")
        logger.info(f"Command output: {result.stdout}")
        if result.stderr:
            logger.error(f"Command stderr: {result.stderr}")

        if result.returncode != 0:
            error_message = result.stderr or result.stdout or "Unknown error"
            return jsonify({
                "status": "error",
                "message": f"Error durante la ejecución: {error_message}"
            }), 500

        # Try to parse output as JSON (sync.py should return JSON)
        try:
            output_json = json.loads(result.stdout)
            return jsonify(output_json)
        except json.JSONDecodeError:
            # If not JSON, return as plain text
            return jsonify({
                "status": "success",
                "message": result.stdout.strip() or "Operación completada exitosamente"
            })

    except subprocess.TimeoutExpired:
        return jsonify({
            "status": "error",
            "message": "La operación excedió el tiempo límite (5 minutos)"
        }), 408

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error interno del servidor: {str(e)}"
        }), 500

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """Get available databases configuration from alias.json"""
    try:
        alias_file = ASSETS_DIR / 'alias.json'
        if not alias_file.exists():
            return jsonify({
                "status": "error",
                "message": "Archivo alias.json no encontrado"
            }), 404

        with open(alias_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        return jsonify(data)

    except Exception as e:
        logger.error(f"Error loading alias.json: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error cargando configuración: {str(e)}"
        }), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get system status and recent logs"""
    try:
        status = {
            "status": "online",
            "timestamp": "2024-01-01T00:00:00Z",
            "version": "2.0.0-flask"
        }

        # Check if log files exist and get recent entries
        log_file = BASE_DIR / 'db_consolidation.log'
        if log_file.exists():
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    status['recent_logs'] = lines[-10:]  # Last 10 lines
            except Exception:
                status['recent_logs'] = []

        # Check if snapshot exists
        snapshot_file = BASE_DIR / 'consolidation_snapshot.json'
        if snapshot_file.exists():
            stat = snapshot_file.stat()
            status['last_snapshot'] = stat.st_mtime

        return jsonify(status)

    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error obteniendo estado: {str(e)}"
        }), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        "status": "error",
        "message": "Endpoint no encontrado"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({
        "status": "error",
        "message": "Error interno del servidor"
    }), 500

if __name__ == '__main__':
    # Create directories if they don't exist
    os.makedirs(TEMPLATES_DIR, exist_ok=True)
    os.makedirs(STATIC_DIR, exist_ok=True)
    
    # Run Flask development server
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )
