#!/bin/bash
# Railway Start Script
echo "Starting Railway deployment..."

# Debug: Show environment
echo "PATH: $PATH"
echo "PWD: $(pwd)"
echo "Files in current directory:"
ls -la

# Try different streamlit paths
echo "Looking for streamlit..."
which streamlit || echo "streamlit not in PATH"
ls -la /opt/venv/bin/ | grep streamlit || echo "streamlit not in /opt/venv/bin/"

# Activate virtual environment explicitly
source /opt/venv/bin/activate
echo "Virtual environment activated"
echo "PATH after activation: $PATH"

# Try to run streamlit
echo "Attempting to run streamlit..."
/opt/venv/bin/python -m streamlit run streamlit_annotation_railway.py --server.port=$PORT --server.address=0.0.0.0
