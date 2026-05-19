<div align="center">

<img width="220" src="https://cdn-icons-png.flaticon.com/512/744/744465.png" />

# 🚗 Vehicle Rental System

### Plataforma moderna de renta de vehículos en tiempo real ⚡

<p align="center">
  <b>Vehicle Rental System</b> es una plataforma backend desarrollada con FastAPI para la administración de vehículos, reservas y eventos en tiempo real mediante WebSockets y Apache Kafka.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white">
  <img src="https://img.shields.io/badge/MongoDB-Database-47A248?style=for-the-badge&logo=mongodb&logoColor=white">
  <img src="https://img.shields.io/badge/ApacheKafka-EventDriven-231F20?style=for-the-badge&logo=apachekafka&logoColor=white">
  <img src="https://img.shields.io/badge/WebSocket-RealTime-blue?style=for-the-badge">
</p>

<p align="center">
  <a href="#-acerca-del-proyecto">Acerca</a> •
  <a href="#-características">Características</a> •
  <a href="#-tecnologías-utilizadas">Tecnologías</a> •
  <a href="#-instalación">Instalación</a> •
  <a href="#-vista-previa">Vista previa</a>
</p>

</div>

---

# 🌌 Acerca del proyecto

**Vehicle Rental System** es una API moderna orientada a la gestión de alquiler de vehículos utilizando arquitectura orientada a eventos y comunicación en tiempo real.

El sistema fue diseñado para:

- 🚗 Gestionar vehículos
- 👥 Administrar clientes y empleados
- 🔐 Implementar autenticación JWT
- 📡 Enviar actualizaciones en tiempo real
- 📬 Procesar eventos mediante Kafka
- 📅 Gestionar reservas de vehículos
- ⚡ Escalar arquitecturas backend modernas

---

# ✨ Características

## 🚘 Gestión de vehículos

- 🚗 Registro de vehículos
- 📋 Información detallada
- 💰 Gestión de precios
- 📅 Disponibilidad en tiempo real
- ⚡ Actualización de estados

---

## 👥 Gestión de usuarios

- 🔐 Autenticación JWT
- 👤 Roles de usuario
- 🛡️ Control de accesos
- 👨‍💼 Roles CUSTOMER / EMPLOYEE
- 🔑 Seguridad con OAuth2 y bcrypt

---

## 📡 Tiempo real

- 🔌 WebSockets
- ⚡ Notificaciones instantáneas
- 📬 Eventos distribuidos
- 🚘 Actualización de reservas
- 🔄 Sincronización automática

---

## 📊 Arquitectura Event-Driven

- 📬 Apache Kafka
- 📡 Publicación de eventos
- ⚡ Procesamiento en tiempo real
- 🔄 Integración desacoplada
- 📈 Escalabilidad backend

---

# 👨‍💼 Módulos del sistema

## 🚗 Vehicle Module

Módulo encargado de la administración de vehículos.

### Funcionalidades:

- ➕ Registrar vehículos
- 📋 Consultar información
- 💰 Gestión de precios
- 📅 Disponibilidad
- 🚘 Control de estados

---

## 👤 Authentication Module

Módulo de autenticación y seguridad.

### Funcionalidades:

- 🔐 JWT Authentication
- 🛡️ OAuth2
- 🔑 Gestión de contraseñas
- 👥 Roles y permisos
- ⚡ Seguridad avanzada

---

## 📡 Real-Time Module

Módulo encargado de actualizaciones en tiempo real.

### Funcionalidades:

- 🔌 WebSocket Server
- 📬 Kafka Events
- ⚡ Eventos instantáneos
- 🔄 Actualización automática

---

# 🛠️ Tecnologías utilizadas

## ⚙️ Backend

<p>
  <img src="https://skillicons.dev/icons?i=python,fastapi" />
</p>

- Python 3
- FastAPI
- REST API
- Arquitectura Event-Driven
- Uvicorn

---

## 🗄️ Base de datos

<p>
  <img src="https://skillicons.dev/icons?i=mongodb" />
</p>

- MongoDB
- Persistencia NoSQL
- Gestión de colecciones
- Almacenamiento flexible

---

## 📬 Streaming y eventos

<p>
  <img src="https://skillicons.dev/icons?i=kafka" />
</p>

- Apache Kafka
- Event Streaming
- Comunicación distribuida
- Procesamiento asíncrono

---

## 🧰 Herramientas

<p>
  <img src="https://skillicons.dev/icons?i=git,github,vscode,postman" />
</p>

- Git
- GitHub
- Visual Studio Code
- Swagger
- Conda

---

# 📂 Estructura del proyecto

```bash
PlataformaRentaVehiculosTiempoReal/
│
├── app/                         # Lógica principal
├── tests/                       # Testing y WebSocket tests
├── kafka/                       # Eventos Kafka
├── websocket/                   # Comunicación tiempo real
├── database/                    # Configuración MongoDB
├── environment.yaml             # Entorno Conda
├── requirements.txt
├── README.md
└── LICENSE
```

---

# ⚡ Instalación

## 📋 Requisitos

- Python 3+
- MongoDB
- Apache Kafka
- Conda
- Navegador moderno

---

# 🚀 Configuración del proyecto

## 1️⃣ Clonar repositorio

```bash
git clone https://github.com/isairey/PlataformaRentaVehiculosTiempoReal.git
```

---

## 2️⃣ Entrar al proyecto

```bash
cd PlataformaRentaVehiculosTiempoReal
```

---

## 3️⃣ Crear entorno Conda

```bash
conda env create -f environment.yaml
```

---

## 4️⃣ Activar entorno

```bash
conda activate vehicle-rental-system
```

---

## 5️⃣ Ejecutar MongoDB

Verificar conexión:

```bash
mongosh
```

---

## 6️⃣ Ejecutar Kafka y Zookeeper

```bash
brew services start kafka
brew services start zookeeper
```

---

## 7️⃣ Ejecutar servidor

```bash
uvicorn app:app --reload
```

---

## 8️⃣ Abrir documentación Swagger

```bash
http://localhost:8000/docs
```

---

# 📡 Testing

## 🔌 WebSocket Testing

### Cliente HTML

```bash
open tests/websocket_test.html
```

---

### Cliente Python

```bash
python tests/websocket_client.py
```

---

## 📬 Kafka Testing

### Consumer Kafka

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle_events --from-beginning
```

---

### Listener Python

```bash
python tests/listen_event.py
```

---

# 📊 Funcionalidades principales

## 🚘 Gestión vehicular

- Registro de vehículos
- Disponibilidad dinámica
- Administración de reservas
- Actualización de estados

---

## 🔐 Seguridad

- JWT Authentication
- OAuth2
- Roles de usuario
- Encriptación de contraseñas

---

## ⚡ Tiempo real

- Notificaciones instantáneas
- Eventos Kafka
- WebSockets
- Actualización automática

---

# 📸 Vista previa

## 🖥️ Interfaces y arquitectura

<div align="center">

### 🚗 Gestión de vehículos
![Vehicles](https://images.unsplash.com/photo-1494976388531-d1058494cdd8?q=80&w=1200)

### 📡 Arquitectura backend
![Backend](https://images.unsplash.com/photo-1558494949-ef010cbdcc31?q=80&w=1200)

### 🔐 Seguridad y autenticación
![Security](https://images.unsplash.com/photo-1516321318423-f06f85e504b3?q=80&w=1200)

### ⚡ Tiempo real y eventos
![Realtime](https://images.unsplash.com/photo-1451187580459-43490279c0fa?q=80&w=1200)

</div>

---

# 🧠 Objetivos del proyecto

## 🎯 Aprendizaje y arquitectura moderna

- Desarrollo backend avanzado
- Arquitectura orientada a eventos
- APIs modernas con FastAPI
- Integración MongoDB
- Sistemas en tiempo real
- Seguridad JWT
- Comunicación distribuida

---

# 🚧 Roadmap

## 🔮 Próximas mejoras

- 📱 Aplicación móvil
- ☁️ Deploy cloud
- 💳 Integración de pagos
- 🤖 IA para recomendaciones
- 📊 Dashboard analítico
- 🌐 Microservicios
- 🔔 Push notifications

---

# 🤝 Contribuciones

Las contribuciones son bienvenidas ❤️

## Cómo contribuir

1. Fork del proyecto

```bash
git checkout -b feature/nueva-funcionalidad
```

2. Commit

```bash
git commit -m "✨ Nueva funcionalidad"
```

3. Push

```bash
git push origin feature/nueva-funcionalidad
```

4. Pull Request 🚀

---

# 👨‍💻 Desarrollador

<div align="center">

## Isai Reyes — Full Stack Developer

Desarrollador apasionado por arquitecturas modernas, FastAPI y sistemas en tiempo real 🚀

</div>

---

# 🌟 Apoya el proyecto

⭐ Dale una estrella  
🍴 Haz fork  
📢 Comparte el proyecto

---

# 📜 Licencia

Proyecto open source bajo licencia MIT orientado al aprendizaje de arquitecturas backend modernas y sistemas distribuidos.

---

<div align="center">

### 🚗 Vehicle Rental System — backend moderno y tiempo real ⚡

</div>
