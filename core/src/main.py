from fastapi import FastAPI

app = FastAPI(title="PythonEE Core", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
async def ready() -> dict[str, str]:
    # TODO(step 10): check Postgres + Kafka connectivity, return 503 on failure
    return {"status": "ready"}
