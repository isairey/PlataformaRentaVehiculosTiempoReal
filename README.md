# Vehicle Rental System 🚗

This Vehicle Rental System is a modern FastAPI-based backend service that enables vehicle rental management with real-time updates. It features JWT authentication, role-based access control (customer/employee), and event-driven architecture using Apache Kafka for system events. It uses MongoDB as the database backend. Real-time updates are handled via WebSocket connections, allowing instant notifications for rental requests, approvals, and vehicle status changes.

## Demo 🎥
Check out our demo video to see the system in action:


https://github.com/user-attachments/assets/4bb6f9c1-6ee8-47b9-81a3-737d5f7fcebe



## Tech Stack 🛠️

### Core
- ⚡️ FastAPI - Web framework
- 🗄️ MongoDB - Database
- 📬 Apache Kafka - Event streaming
- 🔌 WebSocket - Real-time updates

### Key Libraries
- 🔐 PyJWT & OAuth2 & bcrypt - Authentication
- 📨 confluent-kafka-python - Kafka client
- ✅ pydantic - Data validation

## Prerequisites 📋

Before running this project, ensure you have the following installed:
- MongoDB 🗄️
- Apache Kafka 📬
- Conda (for environment management) 🐍

## Environment Setup

1. Create a new conda environment:
```bash
conda env create -f environment.yaml
```

2. Activate the environment:
```bash
conda activate vehicle-rental-system
```
## MongoDB Setup 🗄️

1. Ensure MongoDB is running on your system. You can verify this by:
   - On Windows: Check if MongoDB service is running in Services
   - On macOS/Linux: Run `ps aux | grep mongod`

2. Default MongoDB connection settings:
   - Host: localhost
   - Port: 27017
   - No authentication required for development

3. If MongoDB is not running:
   - Windows: Start MongoDB service
   - macOS: `brew services start mongodb-community`
   - Linux: `sudo systemctl start mongod`

4. Verify connection:
   ```bash
   mongosh
   ```
   You should see the MongoDB shell prompt if connected successfully.

## Kafka Setup 📬

Make sure Apache Kafka is running

if using macos/linux, run the following command to start kafka:

Start kafka and zookeeper:
```bash
brew services start kafka
brew services start zookeeper
```

## Running the Application 🚀

run the application
```bash
uvicorn app:app --reload
```

The application will be available at: http://localhost:8000

## API Documentation (Swagger) 📚

This project uses Swagger for API documentation. To access the Swagger UI:

Open your browser and navigate to:
http://localhost:8000/docs


## Testing 🧪

### WebSocket Testing 🔌

There are two ways to test WebSocket functionality:

1. Using the HTML Test Client:
Open the HTML test client in your browser
```bash
open tests/websocket_test.html
```
Then type user_id and click connect, you will see the real-time updates appear in the html page

2. Using python script:
Run the python script to connect to the websocket and you will see the real-time updates in the console
```bash
python tests/websocket_client.py
```

### Apache Kafka Event Testing 📊

There are two ways to test the kafka events:

1. Using the kafka-console-consumer:
for example, to test the vehicle_events topic, run the following command:
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle_events --from-beginning
```

2. Using python script:
Run the python script to consume the kafka events and you will see the events appear in the console
```bash
python tests/listen_event.py
```


## 📝 Note

This project was developed as part of the AIN3005 (Advanced Python Programming) project. For detailed methodology and findings, please refer to the project report.


