from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.routes_user import router as user_router
from routes.routes_vehicle import router as vehicle_router
from routes.routes_rental import router as rental_router
from routes.routes_branches import router as branch_router
from routes.routes_websocket import router as websocket_router
from routes.routes_auth import router as auth_router
from kafka_config.init_kafka import init_kafka_topics

app = FastAPI(
    title="Vehicle Rental API",)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user_router)
app.include_router(vehicle_router)
app.include_router(rental_router)
app.include_router(branch_router)
app.include_router(websocket_router)
app.include_router(auth_router)

# Initialize Kafka topics when the application starts
@app.on_event("startup")
async def startup_event():
    init_kafka_topics()

# cd 2100503-project
# uvicorn app:app --reload