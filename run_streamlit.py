#!/usr/bin/env python3
"""
Railway Start Script - Python Version
Startet Streamlit für Railway Deployment
"""

import os
import sys
import subprocess

def main():
    print("Starting Streamlit for Railway...")
    
    # Set environment variables
    port = os.getenv('PORT', '8000')
    print(f"Using port: {port}")
    
    # Run streamlit
    cmd = [
        sys.executable, '-m', 'streamlit', 'run',
        'streamlit_annotation_railway.py',
        '--server.port', port,
        '--server.address', '0.0.0.0'
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running streamlit: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
