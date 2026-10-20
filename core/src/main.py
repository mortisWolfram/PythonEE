"""Entrypoint for the Core service. Skeleton only — no business logic yet."""
from fastapi import FastAPI

app = FastAPI(title="PythonEE Core", version="0.1.0")
container = build_container(settings)

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
