#!/bin/bash

# Flask Database Consolidation System Startup Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if we're in the right directory
if [ ! -f "app.py" ]; then
    log_error "app.py not found. Please run this script from the volcado_web directory."
    exit 1
fi

log_info "🚀 Starting Flask Database Consolidation System"
echo "=========================================================="

# Check Python version
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    log_info "Python version: $PYTHON_VERSION"
else
    log_error "Python 3 is required but not found"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    log_warning "Virtual environment not found, creating one..."
    python3 -m venv venv
    log_success "Virtual environment created"
fi

# Activate virtual environment
log_info "Activating virtual environment..."
source venv/bin/activate

# Install/upgrade dependencies
log_info "Installing/updating dependencies..."
pip install -r requirements.txt

# Check if MySQL is running (for XAMPP users)
if command -v mysql &> /dev/null; then
    if mysql -u root -e "SELECT 1;" &> /dev/null; then
        log_success "MySQL connection verified"
    else
        log_warning "Cannot connect to MySQL. Make sure XAMPP MySQL is running."
    fi
else
    log_warning "MySQL client not found in PATH"
fi

# Create necessary directories
mkdir -p templates static logs

# Check configuration files
if [ ! -f "assets/alias.json" ]; then
    log_warning "assets/alias.json not found. Please configure your databases."
fi

# Set environment variables
export FLASK_APP=app.py
export FLASK_ENV=development
export FLASK_DEBUG=1

log_success "Setup completed successfully!"
echo "=========================================================="
log_info "🌐 Starting Flask development server..."
log_info "📍 Access the application at: http://localhost:5001"
log_info "🛑 Press Ctrl+C to stop the server"
echo "=========================================================="

# Start Flask application
python3 run.py
