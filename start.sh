#!/bin/bash
# Railway Start Script
export PATH="/opt/venv/bin:$PATH"
streamlit run streamlit_annotation_railway.py --server.port=$PORT --server.address=0.0.0.0
