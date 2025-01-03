import uvicorn
from app.api import app  # Import the FastAPI app instance from api.py

# Run the FastAPI app with uvicorn when the script is executed directly
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)  # Ensure it binds to all interfaces
