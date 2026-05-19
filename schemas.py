from enum import Enum
from pydantic import BaseModel, EmailStr, Field
from datetime import datetime
from bson import ObjectId

class UserRole(Enum):
    CUSTOMER = "CUSTOMER"
    EMPLOYEE = "EMPLOYEE"

class VehicleType(Enum):
    CAR = "CAR"
    TRUCK = "TRUCK"
    SUV = "SUV"
    VAN = "VAN"
    MOTORCYCLE = "MOTORCYCLE"

class AvailabilityStatus(Enum):
    AVAILABLE = "AVAILABLE"
    RENTED = "RENTED"
    MAINTENANCE = "MAINTENANCE"

class RentalStatus(Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    COMPLETED = "COMPLETED"

class User(BaseModel):
    user_id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id") # generate a new ObjectId for each user if not provided
    name: str
    email: EmailStr
    password: str
    role: UserRole

    class Config:
        use_enum_values = True  

    def authenticate(self, password: str) -> bool:
        return self.password == password
    
    def is_employee(self) -> bool:
        return self.role == UserRole.EMPLOYEE

    def is_customer(self) -> bool:
        return self.role == UserRole.CUSTOMER

class Vehicle(BaseModel):
    vehicle_id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id") # generate a new ObjectId for each vehicle if not provided
    name: str
    model: str
    vehicle_type: VehicleType # changed from type to vehicle_type to avoid confusion with the type keyword
    rental_price_per_day: float = Field(..., gt=0, description="Rental price per day must be positive")
    availability_status: AvailabilityStatus
    location: str

    class Config:
        use_enum_values = True  

    def update_status(self, status: AvailabilityStatus) -> None:
        self.availability_status = status

    def calculate_rental_cost(self, days: int) -> float:
        return self.rental_price_per_day * max(0, days)

class Rental(BaseModel):
    rental_id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id") # generate a new ObjectId for each rental if not provided
    vehicle_id: str = Field(default=None) # foreign key to the vehicle collection
    customer_id: str = Field(default=None) # foreign key to the customer collection
    rental_start_date: datetime
    rental_end_date: datetime
    total_cost: float = Field(default=0)
    rental_status: RentalStatus = Field(default=RentalStatus.PENDING)  # added rental_status to the schema so that it can be approved or rejected by the employee

    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()  # Convert datetime to ISO format string
        }

    def calculate_total_cost(self, rental_price_per_day: float) -> float:
        return rental_price_per_day *  max(0, (self.rental_end_date - self.rental_start_date).days)

class Branch(BaseModel):
    branch_id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id") # generate a new ObjectId for each branch if not provided
    name: str
    location: str
    contact_number: str

class Token(BaseModel):
    access_token: str
    token_type: str

