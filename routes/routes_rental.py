from fastapi import APIRouter, HTTPException, status, Body, Depends
from typing import List, Dict, Any
from pydantic import ValidationError
from datetime import datetime

from database import rentals_collection, vehicles_collection, users_collection
from schemas import Rental, RentalStatus, Vehicle, User, AvailabilityStatus
from kafka_config.producer import KafkaProducer
from kafka_config.event_types import EventType
from routes.routes_websocket import manager
from middleware.auth import get_current_user, get_current_customer, get_current_employee

router = APIRouter()
employee_only = Depends(get_current_employee)
customer_only = Depends(get_current_customer)
authenticated_user = Depends(get_current_user)

# Initialize Kafka producer
kafka_producer = KafkaProducer()

@router.post("/rentals/", status_code=status.HTTP_201_CREATED, dependencies=[customer_only], summary="Create new rental")
async def create_rental(rental: Rental):
    """
    Create a new rental request with the provided details. 
    Rental status is always set to PENDING when creating new rental request.
    
    Customer access required.
    """
    # Force rental status to PENDING
    rental.rental_status = RentalStatus.PENDING.value
    
    # Check if rental ID already exists
    if rentals_collection.find_one({"_id": rental.rental_id}):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Rental ID already exists"
        )
    
    # Verify vehicle exists and is available
    vehicle = vehicles_collection.find_one({"_id": rental.vehicle_id})
    if not vehicle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vehicle not found"
        )
    
    vehicle_obj = Vehicle(**vehicle)
    if vehicle_obj.availability_status != AvailabilityStatus.AVAILABLE.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Vehicle is not available for rent"
        )
    
    # Verify customer exists
    customer = users_collection.find_one({"_id": rental.customer_id})
    if not customer:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Customer not found"
        )
    
    # Calculate total cost
    rental.total_cost = rental.calculate_total_cost(vehicle_obj.rental_price_per_day)
    
    
    rentals_collection.insert_one(rental.model_dump(by_alias=True))
    
    # Send WebSocket notification to employees about new rental request
    employee_notification = {
        "type": "new_rental_request",
        "rental_id": rental.rental_id,
        "customer_id": rental.customer_id,
        "vehicle_id": rental.vehicle_id,
        "status": rental.rental_status,
        "timestamp": datetime.now().isoformat()
    }
    await manager.broadcast_to_employees(employee_notification)
    
    # Produce Kafka events
    kafka_producer.produce_event(
        topic='rental_events',
        event_type=EventType.RENTAL_CREATED.value,
        event_data={
            'id': rental.rental_id,
            'rental_data': rental.model_dump(by_alias=True)
        }
    )

    
    return {"message": "Rental created successfully"}

@router.get("/rentals/", response_model=List[Rental], dependencies=[employee_only], summary="Get all rentals")
async def get_all_rentals():
    """
    Retrieve a list of all rentals in the system.
    
    Employee access required.
    """
    rentals = [Rental(**rental) for rental in rentals_collection.find()]
    return rentals

@router.get("/rentals/{rental_id}", response_model=Rental, dependencies=[authenticated_user], summary="Get rental details")
async def get_rental(rental_id: str):
    """
    Retrieve details for a specific rental by ID.
    
    Authentication required.
    """
    rental = rentals_collection.find_one({"_id": rental_id})
    if not rental:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Rental not found"
        )
    return Rental(**rental)

@router.put("/rentals/{rental_id}/rental_status", dependencies=[employee_only], summary="Update rental status")
async def update_rental_status(rental_id: str, rental_status: RentalStatus):
    """
    Update the status of a rental 
    
    Employee access required.
    """
    rental = rentals_collection.find_one({"_id": rental_id})
    if not rental:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Rental not found"
        )
    
    rentals_collection.update_one(
        {"_id": rental_id},
        {"$set": {"rental_status": rental_status.value}}
    )
    
    # Send WebSocket notification about rental status update
    status_notification = {
        "type": "rental_status_update",
        "rental_id": rental_id,
        "new_status": rental_status.value,
        "timestamp": datetime.now().isoformat()
    }
    await manager.broadcast_to_employees(status_notification)
    
    # Also notify the specific customer about their rental status
    rental = rentals_collection.find_one({"_id": rental_id})
    if rental:
        await manager.send_personal_message(status_notification, rental["customer_id"])
    
    # Produce Kafka events
    kafka_producer.produce_event(
        topic='rental_events',
        event_type=(EventType.RENTAL_COMPLETED.value if rental_status == RentalStatus.COMPLETED 
                   else EventType.RENTAL_APPROVED.value if rental_status == RentalStatus.APPROVED
                   else EventType.RENTAL_REJECTED.value if rental_status == RentalStatus.REJECTED
                   else "RENTAL_STATUS_UPDATED"),
        event_data={
            'id': rental_id,
            'new_status': rental_status.value
        }
    )
    
    if rental_status == RentalStatus.COMPLETED:
        # make vehicle available again
        vehicles_collection.update_one(
            {"_id": rental["vehicle_id"]},
            {"$set": {"availability_status": AvailabilityStatus.AVAILABLE.value}}
        )
        kafka_producer.produce_event(
            topic='vehicle_events',
            event_type=EventType.VEHICLE_STATUS_CHANGED.value,
            event_data={
                'id': rental['vehicle_id'],
                'status': AvailabilityStatus.AVAILABLE.value
            }
        )
    if rental_status == RentalStatus.APPROVED:
        # make vehicle unavailable
        vehicles_collection.update_one(
            {"_id": rental['vehicle_id']},
            {"$set": {"availability_status": AvailabilityStatus.RENTED.value}}
        )
        kafka_producer.produce_event(
            topic='vehicle_events',
            event_type=EventType.VEHICLE_STATUS_CHANGED.value,
            event_data={
                'id': rental['vehicle_id'],
                'status': AvailabilityStatus.RENTED.value
            }
        )
    
    return {"message": "Rental status updated successfully"}

@router.delete("/rentals/{rental_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[employee_only], summary="Delete rental")
async def delete_rental(rental_id: str):
    """
    Delete an existing rental by ID.
    
    Employee access required.
    """
    result = rentals_collection.delete_one({"_id": rental_id})
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Rental not found"
        )
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='rental_events',
        event_type='RENTAL_DELETED',
        event_data={
            'id': rental_id
        }
    )
    return {"message": "Rental deleted successfully"}
