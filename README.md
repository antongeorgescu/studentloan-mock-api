# Student Loan Management API

An Azure Functions-based API for managing student loans, payments, and student information.

## Overview

This API provides endpoints to manage:
- Student registration and information updates
- Loan creation and management
- Payment processing and tracking
- Statistical reporting for loans and payments
- Educational institution tracking

## Technical Stack

- Python 3.9+
- Azure Functions
- SQL Server Database
- OpenAPI 3.0 Specification

## Project Structure

```
studentloan-azfuncs-2/
├── function_app.py         # Main application code with Azure Functions
├── swagger/               
│   └── openapi.yaml       # API documentation
├── requirements.txt       # Python dependencies
├── host.json             # Azure Functions host configuration
└── local.settings.json   # Local development settings
```

## API Endpoints

### User Account Management
- `GET /students/lastname/{lastname}` - Search students by last name
- `POST /student/create-nonregistered` - Create new non-registered student
- `POST /student/update/communication` - Update student contact information
- `POST /student/update/address` - Update student address

### Loan Management
- `POST /student/update/loan` - Add loan to student profile
- `POST /loan/update/study-info` - Update loan study information
- `GET /students/loan/near-completion/{threshold}` - Get students near loan completion

### Payments
- `POST /loans/make-payment` - Process loan payment
- `GET /loans/{loanid}/payments` - Get payment history for a loan
- `GET /financial/payment/stats` - Get financial institution payment statistics

### Statistics
- `GET /provinces/student-count` - Get student count by province
- `GET /payments/monthly-by-province` - Get monthly payments by province
- `GET /stats/yearly/loan/{loanid}/payments` - Get yearly payment statistics

## Getting Started

### Prerequisites

- Python 3.9 or higher
- Azure Functions Core Tools
- Visual Studio Code with Azure Functions extension
- SQL Server instance

### Local Development Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd studentloan-azfuncs-2
```

2. Create and activate virtual environment:
```bash
python -m venv .venv
.venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure local settings:
- Rename `local.settings.sample.json` to `local.settings.json`
- Update the database connection string

5. Start the local server:
```bash
func start
```

### Database Setup

1. Create required database tables using the provided SQL scripts
2. Configure connection string in application settings

## API Documentation

Full API documentation is available in OpenAPI format. To view:

1. Install Swagger UI tools
2. Navigate to `swagger/openapi.yaml`
3. Or use online Swagger Editor: [https://editor.swagger.io/](https://editor.swagger.io/)

## Error Handling

The API uses standard HTTP status codes:
- 200: Success
- 201: Resource created
- 400: Bad request
- 404: Resource not found
- 409: Conflict
- 500: Server error

## Install ODBC driver 
Open SSH console to Azure Function App container.
1. Ensure the ODBC Driver is Installed:
Azure Functions on Linux might not have the ODBC Driver 17 for SQL Server pre-installed. You can install it by adding the following commands to your function app's startup script:
```
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
sudo apt-get install -y unixodbc-dev
```
2. Update the Connection String:
If you are using Python 3.11, Azure Functions might come with ODBC Driver 18 instead of 17. Update your connection string to use the correct driver:
```
conn_str = "Driver={ODBC Driver 18 for SQL Server};Server=<server>;Database=<database>;Trusted_Connection=yes;
```
3. Verify the ODBC Configuration:
Ensure that the ODBC driver is correctly listed in the /etc/odbcinst.ini file. You can add the following entry if it is missing:
```
[ODBC Driver 17 for SQL Server]
Description=Microsoft ODBC Driver 17 for SQL Server
Driver=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1
```
4. Create a Symbolic Link (if necessary):
If the driver is installed but not found, create a symbolic link:
```
sudo ln -s /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1 /usr/lib/libmsodbcsql-17.0.so.1.1
```

## Resolve SSL Certificate validation error
If you get a SSL certificate verification failure when trying to connect to Sql Server, Here are a few steps to resolve this issue:
1. Trust the Server Certificate:
Add TrustServerCertificate=yes to your connection string to bypass the certificate validation:
```
conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=<server>;DATABASE=<database>;UID=<username>;PWD=<password>;TrustServerCertificate=yes;"
```
2. Install a Trusted Certificate:
If you prefer to use a trusted certificate, you can install it on your SQL Server and configure your client to trust it. This involves obtaining a certificate from a trusted Certificate Authority (CA) and configuring SQL Server to use it
3. Disable Encryption (if appropriate):
You can disable encryption by setting Encrypt=no in your connection string, though this is not recommended for production environments due to security concerns:
```
conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=<server>;DATABASE=<database>;UID=<username>;PWD=<password>;Encrypt=no;"
```
4. Use the -C Flag with sqlcmd:
If you are using sqlcmd, you can use the -C flag to trust the server certificate:
```
sqlcmd -S <server> -U <username> -P <password> -C
```
These steps should help resolve the SSL certificate verification error and allow you to connect to SQL Server using ODBC Driver 18

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Open a Pull Request

## License

[Add your license information here]