from fastapi import APIRouter, HTTPException, status, Body, Depends
from typing import List, Dict, Any
from pydantic import ValidationError

from database import vehicles_collection, rentals_collection
from schemas import Vehicle, AvailabilityStatus, VehicleType, Rental, RentalStatus
#from middleware.auth import require_role

from routes.routes_websocket import manager
from kafka_config.producer import KafkaProducer
from kafka_config.event_types import EventType
from middleware.auth import get_current_user, get_current_customer, get_current_employee

router = APIRouter()
employee_only = Depends(get_current_employee)
customer_only = Depends(get_current_customer)
authenticated_user = Depends(get_current_user)

# Initialize Kafka producer
kafka_producer = KafkaProducer()

@router.post("/vehicles/", status_code=status.HTTP_201_CREATED, dependencies=[employee_only], summary="Create new vehicle")
async def create_vehicle(vehicle: Vehicle):
    """
    Create a new vehicle with the provided details.
    
    Employee access required.
    """
    if vehicles_collection.find_one({"_id": vehicle.vehicle_id}):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Vehicle ID already registered"
        )
    
    vehicles_collection.insert_one(vehicle.model_dump(by_alias=True))
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='vehicle_events',
        event_type=EventType.VEHICLE_CREATED.value,
        event_data={
            'id': vehicle.vehicle_id,
            'vehicle_data': vehicle.model_dump(by_alias=True)
        }
    )
    
    return {"message": "Vehicle created successfully"}

# @router.get("/vehicles/", response_model=List[Vehicle])
# async def get_all_vehicles():
#     vehicles = [Vehicle(**vehicle) for vehicle in vehicles_collection.find()]
#     return vehicles

@router.get("/vehicles/{vehicle_id}", response_model=Vehicle, dependencies=[authenticated_user], summary="Get vehicle details")
async def get_vehicle(vehicle_id: str):
    """
    Retrieve details for a specific vehicle by ID.
    
    Authentication required.
    """
    vehicle = vehicles_collection.find_one({"_id": vehicle_id})
    if not vehicle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vehicle not found"
        )
    return Vehicle(**vehicle)

@router.get("/vehicles/{vehicle_id}/rental_history", dependencies=[authenticated_user], summary="Get rental history for a vehicle")
async def get_rental_history(vehicle_id: str):
    """
    Retrieve the completed rental history for a specific vehicle by ID.
    
    Authentication required.
    """
    rentals = rentals_collection.find({"vehicle_id": vehicle_id, "rental_status": RentalStatus.COMPLETED.value})
    return [Rental(**rental) for rental in rentals]

@router.get("/vehicles/", dependencies=[authenticated_user], summary="Filter vehicles")
async def filter_vehicles(
    vehicle_type: VehicleType | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    location: str | None = None,
    availability_status: AvailabilityStatus | None = None
):
    """
    Get vehicles based on optional filters like type, price range, location, and availability.
    
    Authentication required.
    """
    # Start with empty filter
    filter_query = {}

    # Add optional filters
    if availability_status:
        filter_query["availability_status"] = availability_status.value

    if vehicle_type:
        filter_query["vehicle_type"] = vehicle_type.value

    if location:
        filter_query["location"] = location

    # Create price range filter if either min or max price is specified
    if min_price is not None or max_price is not None:
        price_filter = {}
        if min_price is not None:
            price_filter["$gte"] = min_price
        if max_price is not None:
            price_filter["$lte"] = max_price
        if price_filter:
            filter_query["rental_price_per_day"] = price_filter

    # Query the database with the constructed filter
    vehicles = vehicles_collection.find(filter_query)
    return [Vehicle(**vehicle) for vehicle in vehicles]


@router.put("/vehicles/{vehicle_id}", dependencies=[employee_only], summary="Update vehicle")
async def update_vehicle(vehicle_id: str, vehicle_updated: Vehicle):
    """
    Update all fields of an existing vehicle.
    
    Employee access required.
    """
    # Check if vehicle exists
    existing_vehicle = vehicles_collection.find_one({"_id": vehicle_id})
    if not existing_vehicle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vehicle not found"
        )
    
    # Check if vehicle_id matches with request body
    if vehicle_id != vehicle_updated.vehicle_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Vehicle ID does not match with request body"
        )
    
    vehicles_collection.replace_one(
        {"_id": vehicle_id},
        vehicle_updated.model_dump(by_alias=True)
    )

    kafka_producer.produce_event(
        topic='vehicle_events',
        event_type=EventType.VEHICLE_UPDATED.value,
        event_data={
            'id': vehicle_id,
            'vehicle_data': vehicle_updated.model_dump(by_alias=True)
        }
    )
    
    return {"message": "Vehicle updated successfully"}

@router.patch("/vehicles/{vehicle_id}", dependencies=[employee_only], summary="Partial vehicle update")
async def patch_vehicle(vehicle_id: str, update_data: Dict[str, Any] = Body(...)):
    """
    Update specific fields of an existing vehicle.
    
    Employee access required.
    """
    # Check if vehicle exists
    existing_vehicle = vehicles_collection.find_one({"_id": vehicle_id})
    if not existing_vehicle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vehicle not found"
        )
    
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )
    
    # Prevent updating vehicle_id
    if "_id" in update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot update vehicle_id"
        )
    
    # Validate fields against Vehicle model
    valid_fields = Vehicle.model_fields.keys()
    invalid_fields = [field for field in update_data.keys() if field not in valid_fields]
    if invalid_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid fields: {', '.join(invalid_fields)}"
        )
    
    # Validate update data by merging with existing vehicle and validating
    try:
        updated_vehicle_dict = {**existing_vehicle, **update_data}
        Vehicle(**updated_vehicle_dict)  # Validate the merged data
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    
    vehicles_collection.update_one(
        {"_id": vehicle_id},
        {"$set": update_data}
    )

    kafka_producer.produce_event(
        topic='vehicle_events',
        event_type=EventType.VEHICLE_UPDATED.value,
        event_data={
            'id': vehicle_id,
            'updated_fields': update_data
        }
    )
    
    
    return {"message": "Vehicle updated successfully"}

@router.put("/vehicles/{vehicle_id}/availability_status", dependencies=[employee_only], summary="Update vehicle status")
async def update_vehicle_status(vehicle_id: str, availability_status: AvailabilityStatus):
    """
    Update the availability status of a vehicle.
    
    Employee access required.
    """
    vehicle = vehicles_collection.find_one({"_id": vehicle_id})
    if not vehicle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vehicle not found"
        )
    
    vehicles_collection.update_one(
        {"_id": vehicle_id},
        {"$set": {"availability_status": availability_status.value}}
    )
    
    

    kafka_producer.produce_event(
        topic='vehicle_events',
        event_type=EventType.VEHICLE_STATUS_CHANGED.value,
        event_data={
            'id': vehicle_id,
            'status_update': availability_status.value
        }
    )

    notification = {
        "type": "vehicle_status_update",
        "vehicle_id": vehicle_id,
        "new_status": availability_status.value
    }
    await manager.broadcast_to_customers(notification)
    await manager.broadcast_to_employees(notification)
    
    return {"message": "Vehicle status updated successfully"}


@router.delete("/vehicles/{vehicle_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[employee_only], summary="Delete vehicle")
async def delete_vehicle(vehicle_id: str):
    """
    Delete an existing vehicle by ID.
    
    Employee access required.
    """
    result = vehicles_collection.delete_one({"_id": vehicle_id})
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vehicle not found"
        )
    
    kafka_producer.produce_event(
        topic='vehicle_events',
        event_type=EventType.VEHICLE_DELETED.value,
        event_data={
            'id': vehicle_id
        }
    )
    
    return {"message": "Vehicle deleted successfully"}

