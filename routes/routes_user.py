from fastapi import APIRouter, HTTPException, status, Body, Depends
from typing import List, Dict, Any
from pydantic import ValidationError

from database import users_collection
from schemas import User
from kafka_config.producer import KafkaProducer
from kafka_config.event_types import EventType
from middleware.auth import get_current_user, get_current_customer, get_current_employee
from middleware.password import hash_password

router = APIRouter()
employee_only = Depends(get_current_employee)
customer_only = Depends(get_current_customer)
authenticated_user = Depends(get_current_user)

# Initialize Kafka producer
kafka_producer = KafkaProducer()

@router.post("/users/", status_code=status.HTTP_201_CREATED, summary="Create new user")
async def create_user(user: User):
    """
    Create a new user with the provided details.
    
    No authentication required.
    """
    # Check if email already exists
    if users_collection.find_one({"email": user.email}):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    if users_collection.find_one({"_id": user.user_id}):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User ID already registered"
        )

    # Hash the password before saving
    #print(user.password, hash_password(user.password))
    user_dict = user.model_dump(by_alias=True)
    user_dict["password"] = hash_password(user_dict["password"])
    
    users_collection.insert_one(user_dict)
    
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='user_events',
        event_type=EventType.USER_CREATED.value,
        event_data={
            'id': user.user_id,
            'user_data': user_dict
        }
    )
    
    return {"message": "User created successfully"}

@router.get("/users/", response_model=List[User], dependencies=[employee_only], summary="Get all users")
async def get_all_users():
    """
    Retrieve a list of all users in the system.
    
    Employee access required.
    """
    users = [User(**user) for user in users_collection.find()]
    return users

@router.get("/users/{user_id}", response_model=User, dependencies=[employee_only], summary="Get user details")
async def get_user(user_id: str):
    """
    Retrieve details for a specific user by ID.
    
    Employee access required.
    """
    user = users_collection.find_one({"_id": user_id})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return User(**user)

@router.put("/users/{user_id}", dependencies=[employee_only], summary="Update user")
async def update_user(user_id: str, user_updated: User):
    """
    Update all fields of an existing user.
    
    Employee access required.
    """
    # Check if user exists
    existing_user = users_collection.find_one({"_id": user_id})
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # check if user_id matches with request body
    if user_id != user_updated.user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User ID does not match with request body"
        )
    
    # Hash the password before updating
    user_dict = user_updated.model_dump(by_alias=True)
    user_dict["password"] = hash_password(user_dict["password"])
    
    # Update user with hashed password
    users_collection.replace_one(
        {"_id": user_id},
        user_dict
    )
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='user_events',
        event_type=EventType.USER_UPDATED.value,
        event_data={
            'id': user_id,
            'user_data': user_dict
        }
    )
    
    return {"message": "User updated successfully"}

@router.patch("/users/{user_id}", dependencies=[employee_only], summary="Partial user update")
async def patch_user(user_id: str, update_data: Dict[str, Any] = Body(...)):
    """
    Update specific fields of an existing user.
    
    Employee access required.
    """
    # Check if user exists
    existing_user = users_collection.find_one({"_id": user_id})
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )
    
    # Prevent updating user_id
    if "_id" in update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot update user_id"
        )
    
    # Validate fields against User model
    valid_fields = User.model_fields.keys()
    invalid_fields = [field for field in update_data.keys() if field not in valid_fields]
    if invalid_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid fields: {', '.join(invalid_fields)}"
        )
    
    # Validate update data by merging with existing user and validating
    try:
        updated_user_dict = {**existing_user, **update_data}
        User(**updated_user_dict)  # Validate the merged data
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    
    # Hash password if it's being updated
    if "password" in update_data:
        update_data["password"] = hash_password(update_data["password"])
    
    users_collection.update_one(
        {"_id": user_id},
        {"$set": update_data}
    )
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='user_events',
        event_type=EventType.USER_UPDATED.value,
        event_data={
            'id': user_id,
            'updated_fields': update_data
        }
    )
    
    return {"message": "User updated successfully"}

@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[employee_only], summary="Delete user")
async def delete_user(user_id: str):
    """
    Delete an existing user by ID.
    
    Employee access required.
    """
    result = users_collection.delete_one({"_id": user_id})
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='user_events',
        event_type=EventType.USER_DELETED.value,
        event_data={
            'id': user_id
        }
    )
    
    return {"message": "User deleted successfully"}
    
    