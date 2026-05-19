from fastapi import APIRouter, HTTPException, status, Body, Depends
from typing import List, Dict, Any
from pydantic import ValidationError

from database import branches_collection
from schemas import Branch
from kafka_config.producer import KafkaProducer
from kafka_config.event_types import EventType
from middleware.auth import get_current_user, get_current_customer, get_current_employee

router = APIRouter()
employee_only = Depends(get_current_employee)
customer_only = Depends(get_current_customer)
authenticated_user = Depends(get_current_user)

# Initialize Kafka producer
kafka_producer = KafkaProducer()

@router.post("/branches/", status_code=status.HTTP_201_CREATED, dependencies=[employee_only], summary="Create new branch")
async def create_branch(branch: Branch):
    """
    Create a new branch location with the provided details.
    
    Employee access required.
    """
    # Check if branch ID already exists
    if branches_collection.find_one({"_id": branch.branch_id}):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Branch ID already exists"
        )
    
    branches_collection.insert_one(branch.model_dump(by_alias=True))
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='branch_events',
        event_type=EventType.BRANCH_CREATED.value,
        event_data={
            'id': branch.branch_id,
            'branch_data': branch.model_dump(by_alias=True)
        }
    )
    return {"message": "Branch created successfully"}

@router.get("/branches/", response_model=List[Branch], dependencies=[authenticated_user], summary="Get all branches")
async def get_all_branches():
    """
    Retrieve a list of all branch locations.
    
    Authentication required.
    """
    branches = [Branch(**branch) for branch in branches_collection.find()]
    return branches

@router.get("/branches/{branch_id}", response_model=Branch, dependencies=[authenticated_user], summary="Get branch details")
async def get_branch(branch_id: str):
    """
    Retrieve details for a specific branch by ID.
    
    Authentication required.
    """
    branch = branches_collection.find_one({"_id": branch_id})
    if not branch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Branch not found"
        )
    return Branch(**branch)

@router.put("/branches/{branch_id}", dependencies=[employee_only], summary="Update branch")
async def update_branch(branch_id: str, branch_updated: Branch):
    """
    Update all fields of an existing branch.
    
    Employee access required.
    """
    # Check if branch exists
    existing_branch = branches_collection.find_one({"_id": branch_id})
    if not existing_branch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Branch not found"
        )
    
    # Check if branch_id matches with request body
    if branch_id != branch_updated.branch_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Branch ID does not match with request body"
        )
    
    branches_collection.replace_one(
        {"_id": branch_id},
        branch_updated.model_dump(by_alias=True)
    )
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='branch_events',
        event_type=EventType.BRANCH_UPDATED.value,
        event_data={
            'id': branch_id,
            'branch_data': branch_updated.model_dump(by_alias=True)
        }
    )
    return {"message": "Branch updated successfully"}

@router.patch("/branches/{branch_id}", dependencies=[employee_only], summary="Partial branch update")
async def patch_branch(branch_id: str, update_data: Dict[str, Any] = Body(...)):
    """
    Update specific fields of an existing branch.
    
    Employee access required.
    """
    # Check if branch exists
    existing_branch = branches_collection.find_one({"_id": branch_id})
    if not existing_branch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Branch not found"
        )
    
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )
    
    # Prevent updating branch_id
    if "_id" in update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot update branch_id"
        )
    
    # Validate fields against Branch model
    valid_fields = Branch.model_fields.keys()
    invalid_fields = [field for field in update_data.keys() if field not in valid_fields]
    if invalid_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid fields: {', '.join(invalid_fields)}"
        )
    
    # Validate update data by merging with existing branch and validating
    try:
        updated_branch_dict = {**existing_branch, **update_data}
        Branch(**updated_branch_dict)  # Validate the merged data
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    
    branches_collection.update_one(
        {"_id": branch_id},
        {"$set": update_data}
    )
    
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='branch_events',
        event_type=EventType.BRANCH_UPDATED.value,
        event_data={
            'id': branch_id,
            'updated_fields': update_data
        }
    )
    
    return {"message": "Branch updated successfully"}

@router.delete("/branches/{branch_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[employee_only], summary="Delete branch")
async def delete_branch(branch_id: str):
    """
    Delete an existing branch by ID.
    
    Employee access required.
    """
    result = branches_collection.delete_one({"_id": branch_id})
    if result.deleted_count == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Branch not found"
        )
    # Produce Kafka event
    kafka_producer.produce_event(
        topic='branch_events',
        event_type=EventType.BRANCH_DELETED.value,
        event_data={
            'id': branch_id
        }
    )
    return {"message": "Branch deleted successfully"}
