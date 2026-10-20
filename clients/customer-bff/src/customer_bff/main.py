"""Entrypoint for the Customer BFF. Skeleton only — no business logic yet."""
from fastapi import FastAPI

app = FastAPI(title="PythonEE Customer BFF", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
