from fastapi import FastAPI
import hashlib

app = FastAPI()


@app.post("/predict")
async def predict(text: str) -> int:
    hashed = int(hashlib.md5(text.encode()).hexdigest(), 16)
    result = hashed % 4

    return result

# Endpoint for health check
@app.get("/health")
async def health_check():
    return {"status": "API is healthy"}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=6000)
