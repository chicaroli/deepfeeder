from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# import your routers
from fanout.api import router as fanout_router
from fanout.test_api import router as test_router


def create_app() -> FastAPI:
    app = FastAPI(title="DeepFeeder Fanout")

    # Optional: permissive CORS for testing; restrict in prod
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Mount routers
    app.include_router(test_router, prefix="/test", tags=["test"])
    app.include_router(fanout_router, prefix="/v1", tags=["fanout"])

    return app
