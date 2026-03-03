"""Legacy entry point – use ap_executor/main.py instead."""
from ap_executor.main import app  # noqa: F401

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
