from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import Optional
from datetime import datetime, timedelta
import jwt
from database import users_collection
from schemas import User, UserRole
from middleware.password import verify_password

# Security config
SECRET_KEY = "dK8Lp#9mN$vX2jR5hF3qY7wC0bA4tE6s" 
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="token",
    auto_error=True,
    scheme_name="OAuth2PasswordBearer"
)

def authenticate_user(email: str, password: str):
    user_doc = users_collection.find_one({"email": email})
    if not user_doc:
        return False
    if not verify_password(password, user_doc["password"]):
        return False
    return User(**user_doc)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now() + expires_delta
    else:
        expire = datetime.now() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception
    
    user_doc = users_collection.find_one({"email": email})
    if user_doc is None:
        raise credentials_exception
    
    return User(**user_doc)

async def get_current_employee(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != UserRole.EMPLOYEE.value:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only employees can access this endpoint"
        )
    return current_user

async def get_current_customer(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != UserRole.CUSTOMER.value:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only customers can access this endpoint"
        )
    return current_user
