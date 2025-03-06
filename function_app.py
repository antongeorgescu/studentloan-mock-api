import azure.functions as func
import logging
import json
import pyodbc
import os
from azure.functions import HttpResponse
from dotenv import load_dotenv
from decimal import Decimal
from datetime import date

# Load environment variables
load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Database connection parameters
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

# Connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

def get_db_connection():
    return pyodbc.connect(conn_str)

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    return obj

@app.route(route="students/lastname/{lastname}")
def get_students_by_lastname(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    lastname = req.route_params.get('lastname')
    if not lastname:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'Lastname parameter is required'
            }),
            status_code=400,
            mimetype="application/json"
        )

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                s.StudentID,
                s.FirstName,
                s.LastName,
                s.HomeAddress,
                c.PhoneNumber,
                c.Email,
                c.Preference as CommunicationPreference,
                l.EnrollmentType,
                l.LoanAmount,
                l.DisbursementDate,
                l.LoanBalance,
                l.PercentagePaid,
                si.ProgramOfStudy,
                si.ProgramCode,
                ei.CollegeName,
                ei.City as CollegeCity,
                p.Province
            FROM Student s
            LEFT JOIN Communication c ON s.CommunicationID = c.CommunicationID
            LEFT JOIN LoanInfo l ON s.LoanInfoID = l.LoanInfoID
            LEFT JOIN StudyInfo si ON l.StudyInfoID = si.StudyInfoID
            LEFT JOIN EducationInstitution ei ON l.EducationInstitutionID = ei.EducationInstitutionID
            LEFT JOIN Province p ON ei.ProvinceID = p.ProvinceID
            WHERE s.LastName LIKE ?
        """
        
        cursor.execute(query, f'%{lastname}%')
        
        columns = [column[0] for column in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
            
        return HttpResponse(
            json.dumps({
                'status': 'success',
                'count': len(results),
                'data': json.loads(json.dumps(results, default=str))
            },default=decimal_default),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="provinces/student-count", auth_level=func.AuthLevel.ANONYMOUS)
def get_province_student_count(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                p.Province,
                COUNT(DISTINCT s.StudentID) as StudentCount
            FROM Province p
            LEFT JOIN EducationInstitution ei ON p.ProvinceID = ei.ProvinceID
            LEFT JOIN LoanInfo l ON ei.EducationInstitutionID = l.EducationInstitutionID
            LEFT JOIN Student s ON l.LoanInfoID = s.LoanInfoID
            GROUP BY p.Province
            ORDER BY p.Province
        """
        
        cursor.execute(query)
        
        columns = [column[0] for column in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        return HttpResponse(
            json.dumps({
                'status': 'success',
                'count': len(results),
                'data': json.loads(json.dumps(results, default=str))
            },default=decimal_default),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="loans/{loanid}/payments", auth_level=func.AuthLevel.ANONYMOUS)
def get_loan_payments(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    loan_id = req.route_params.get('loanid')
    if not loan_id:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'Loan ID parameter is required'
            }),
            status_code=400,
            mimetype="application/json"
        )

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                p.PaymentID,
                p.LoanInfoID,
                p.Amount,
                FORMAT(p.Paydate, 'yyyy-MM-dd') as PaymentDate,
                f.InstitutionName as FinInstitution,
                f.Code as FinCode,
                f.Type as FinType,
                l.LoanAmount,
                l.LoanBalance,
                s.FirstName + ' ' + s.LastName as StudentName
            FROM Payment p
            JOIN LoanInfo l ON p.LoanInfoID = l.LoanInfoID
            JOIN Student s ON l.LoanInfoID = s.LoanInfoID
            JOIN FinancialInstitution f ON f.FinancialInstitutionID = p.FinancialInstitutionID
            WHERE p.LoanInfoID = ?
            ORDER BY p.Paydate
        """
        
        cursor.execute(query, loan_id)
        
        columns = [column[0] for column in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
            
        if not results:
            return HttpResponse(
                json.dumps({
                    'status': 'error',
                    'message': f'No payments found for loan ID: {loan_id}'
                }), 
                status_code=404,
                mimetype="application/json"
            )
            
        return HttpResponse(
            json.dumps({
                'status': 'success',
                'count': len(results),
                'data': json.loads(json.dumps(results, default=str))
            },default=decimal_default),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="payments/monthly-by-province", auth_level=func.AuthLevel.ANONYMOUS)
def get_monthly_payments_by_province(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                p.Province,
                YEAR(pay.Paydate) as PaymentYear,
                MONTH(pay.Paydate) as PaymentMonth,
                FORMAT(pay.Paydate, 'MMMM') as MonthName,
                COUNT(DISTINCT s.StudentID) as NumberOfStudents,
                SUM(pay.Amount) as TotalPayments
            FROM Province p
            JOIN EducationInstitution ei ON p.ProvinceID = ei.ProvinceID
            JOIN LoanInfo l ON ei.EducationInstitutionID = l.EducationInstitutionID
            JOIN Student s ON l.LoanInfoID = s.LoanInfoID
            JOIN Payment pay ON l.LoanInfoID = pay.LoanInfoID
            GROUP BY 
                p.Province, 
                YEAR(pay.Paydate), 
                MONTH(pay.Paydate),
                FORMAT(pay.Paydate, 'MMMM')
            ORDER BY 
                p.Province, 
                PaymentYear DESC, 
                PaymentMonth DESC
        """
        
        cursor.execute(query)
        
        columns = [column[0] for column in cursor.description]
                
        # Organize data by province and year
        province_data = {}
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            province = row_dict['Province']
            year = row_dict['PaymentYear']
            
            if province not in province_data:
                province_data[province] = {
                    'province': province,
                    'years': {}
                }
                
            if year not in province_data[province]['years']:
                province_data[province]['years'][year] = {
                    'year': year,
                    'totalAmount': 0,
                    'months': []
                }
            
            month_info = {
                'month': row_dict['MonthName'],
                'numberOfStudents': row_dict['NumberOfStudents'],
                'totalAmount': float(row_dict['TotalPayments'])
            }
            
            province_data[province]['years'][year]['months'].append(month_info)
            province_data[province]['years'][year]['totalAmount'] += month_info['totalAmount']
        
        # Convert to final format
        results = []
        for province in sorted(province_data.keys()):
            province_info = province_data[province]
            years_list = []
            
            province_total = 0
            for year in sorted(province_info['years'].keys(), reverse=True):
                year_data = province_info['years'][year]
                years_list.append({
                    'year': year,
                    'totalAmount': year_data['totalAmount'],
                    'monthlyBreakdown': sorted(year_data['months'], 
                                             key=lambda x: x['month'])
                })
                province_total += year_data['totalAmount']
            
            results.append({
                'province': province,
                'totalAmount': province_total,
                'yearlyBreakdown': years_list
            })
        
        return HttpResponse(
            json.dumps({
                'status': 'success',
                'count': len(results),
                'data': json.loads(json.dumps(results, default=str))
            },default=decimal_default),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="stats/yearly/loan/{loanid}/payments", auth_level=func.AuthLevel.ANONYMOUS)
def get_loan_payments_yearly_stats(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    loan_id = req.route_params.get('loanid')
    if not loan_id:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'Loan ID parameter is required'
            }),
            status_code=400,
            mimetype="application/json"
        )

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get loan information
        cursor.execute("""
            SELECT 
                l.LoanAmount,
                l.LoanBalance,
                FORMAT(l.DisbursementDate, 'yyyy-MM-dd') as DisbursementDate,
                l.PercentagePaid,
                FORMAT(l.PayoffDate, 'yyyy-MM-dd') as PayoffDate,
                s.FirstName + ' ' + s.LastName as StudentName,
                ei.CollegeName,
                si.ProgramOfStudy
            FROM LoanInfo l
            JOIN Student s ON s.LoanInfoID = l.LoanInfoID
            JOIN StudyInfo si ON l.StudyInfoID = si.StudyInfoID
            JOIN EducationInstitution ei ON l.EducationInstitutionID = ei.EducationInstitutionID
            WHERE l.LoanInfoID = ?
        """, loan_id)
        
        loan_info = cursor.fetchone()
        if not loan_info:
            return HttpResponse(
                json.dumps({
                    'status': 'error',
                    'message': f'No payments found for loan ID: {loan_id}'
                }), 
                status_code=404,
                mimetype="application/json"
            )
        # Get yearly payment statistics
        cursor.execute("""
            SELECT 
                YEAR(Paydate) as PaymentYear,
                COUNT(*) as NumberOfPayments,
                SUM(Amount) as TotalAmount,
                MIN(FORMAT(Paydate, 'yyyy-MM-dd')) as FirstPayment,
                MAX(FORMAT(Paydate, 'yyyy-MM-dd')) as LastPayment
            FROM Payment
            WHERE LoanInfoID = ?
            GROUP BY YEAR(Paydate)
            ORDER BY PaymentYear DESC
        """, loan_id)

        # Process results
        columns = ['year', 'numberOfPayments', 'totalAmount', 'firstPayment', 'lastPayment']
        yearly_stats = []
        
        for row in cursor.fetchall():
            payment_data = dict(zip(columns, row))
            payment_data['totalAmount'] = float(payment_data['totalAmount'])
            yearly_stats.append(payment_data)

        # Create response
        loan_details = {
            'loanAmount': float(loan_info[0]),
            'loanBalance': float(loan_info[1]),
            'disbursementDate': loan_info[2],
            'percentagePaid': loan_info[3],
            'payoffDate': loan_info[4],
            'studentName': loan_info[5],
            'collegeName': loan_info[6],
            'programOfStudy': loan_info[7]
        }

        return HttpResponse(
            json.dumps({
                'status': 'success',
                'loanDetails': loan_details,
                'yearlyPayments': {
                    'numberOfYears': len(yearly_stats),
                    'totalPayments': sum(y['numberOfPayments'] for y in yearly_stats),
                    'totalAmountPaid': sum(y['totalAmount'] for y in yearly_stats),
                    'statistics': yearly_stats
                }
            },default=decimal_default),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="students/incomplete-registration", auth_level=func.AuthLevel.ANONYMOUS)
def get_students_incomplete_registration(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                s.StudentID,
                s.FirstName,
                s.LastName,
                s.HomeAddress,
                c.PhoneNumber,
                c.Email,
                c.Preference,
                CASE 
                    WHEN s.LoanInfoID IS NULL THEN 'Missing'
                    ELSE 'Present'
                END as LoanStatus,
                CASE 
                    WHEN l.StudyInfoID IS NULL THEN 'Missing'
                    ELSE 'Present'
                END as StudyInfoStatus,
                CASE 
                    WHEN l.EducationInstitutionID IS NULL THEN 'Missing'
                    ELSE 'Present'
                END as InstitutionStatus
            FROM Student s
            JOIN Communication c ON s.CommunicationID = c.CommunicationID
            LEFT JOIN LoanInfo l ON s.LoanInfoID = l.LoanInfoID
            WHERE s.LoanInfoID IS NULL
               OR l.StudyInfoID IS NULL
               OR l.EducationInstitutionID IS NULL
            ORDER BY s.LastName, s.FirstName
        """
        
        cursor.execute(query)
        
        columns = [column[0] for column in cursor.description]
        students = []
        
        for row in cursor.fetchall():
            student_data = dict(zip(columns, row))
            # Add missing requirements list
            missing_items = []
            if student_data['LoanStatus'] == 'Missing':
                missing_items.append('Loan Information')
            if student_data['StudyInfoStatus'] == 'Missing':
                missing_items.append('Program of Study')
            if student_data['InstitutionStatus'] == 'Missing':
                missing_items.append('Education Institution')
            
            student_data['missingRequirements'] = missing_items
            students.append(student_data)
        
        return HttpResponse(
            json.dumps({
                'status': 'success',
                'count': len(students),
                'data': json.loads(json.dumps(students, default=str))
            },default=decimal_default),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="loans/make-payment", auth_level=func.AuthLevel.ANONYMOUS)
def post_loan_payment(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Validate request body
    payment_data = req.get_json()
    if not payment_data or 'loanid' not in payment_data:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'Loan ID is required'
            }),
            status_code=400,
            mimetype="application/json"
        )
    loan_id = payment_data['loanid']
    
    if 'amount' not in payment_data:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'Payment amount is required'
            }),
            status_code=400,
            mimetype="application/json"
        )
    payment_amount = float(payment_data['amount'])
    if payment_amount <= 0:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'Payment amount must be greater than zero'
            }),
            status_code=400,
            mimetype="application/json"
        )
    

    conn = get_db_connection()
    cursor = conn.cursor()

    # Begin transaction
    conn.autocommit = False

    try:
        # Get current loan information
        cursor.execute("""
            SELECT LoanAmount, LoanBalance
            FROM LoanInfo
            WHERE LoanInfoID = ?
        """, loan_id)
        
        loan_info = cursor.fetchone()
        if not loan_info:
            return HttpResponse(
                json.dumps({
                    'status': 'error',
                    'message': f'Loan ID {loan_id} not found'
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        loan_amount, current_balance = loan_info

        # Validate payment amount
        if payment_amount > current_balance:
            return HttpResponse(
                json.dumps({
                    'status': 'error',
                    'message': 'Payment amount cannot exceed current balance'
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Calculate new balance and percentage paid
        new_balance = round(float(current_balance) - payment_amount, 2)
        percentage_paid = f"{int(((float(loan_amount) - new_balance) / float(loan_amount)) * 100)}%"
        today = date.today()

        # Get a random financial institution
        cursor.execute("SELECT TOP 1 FinancialInstitutionID FROM FinancialInstitution ORDER BY NEWID()")
        financial_institution_id = cursor.fetchone()[0]

        # Insert payment record
        cursor.execute("""
            INSERT INTO Payment (LoanInfoID, Amount, Paydate, FinancialInstitutionID)
            VALUES (?, ?, ?, ?)
        """, loan_id, payment_amount, today, financial_institution_id)

        # Update loan balance and percentage paid
        cursor.execute("""
            UPDATE LoanInfo
            SET LoanBalance = ?,
                PercentagePaid = ?,
                PayoffDate = ?
            WHERE LoanInfoID = ?
        """, new_balance, percentage_paid, 
            today if new_balance == 0 else None, 
            loan_id)

        conn.commit()
        
        return HttpResponse(
            json.dumps({
                'status': 'success',
                'data': {
                    'loanId': loan_id,
                    'paymentAmount': payment_amount,
                    'paymentDate': today.isoformat(),
                    'newBalance': new_balance,
                    'percentagePaid': percentage_paid,
                    'isFullyPaid': new_balance == 0
                }},default=decimal_default),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        conn.rollback()
        raise e

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="student/update/communication", auth_level=func.AuthLevel.ANONYMOUS)
def update_student_communication(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get update data from request body
    update_data = req.get_json()
    if not update_data:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'No post data provided'
            }),
            status_code=400,
            mimetype="application/json"
        )
    
    if not 'studentid' in update_data:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': 'No Student ID value provided'
            }),
            status_code=400,
            mimetype="application/json"
        )
    student_id = update_data['studentid']

    # Validate required fields
    required_fields = ['phoneNumber', 'email', 'preference']
    missing_fields = [field for field in required_fields if field not in update_data]
    if missing_fields:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': f'Missing required fields: {", ".join(missing_fields)}'
            }),
            status_code=400,
            mimetype="application/json"
        )
    
    # Validate communication preference
    valid_preferences = ['SMS', 'Call', 'Email']
    if update_data['preference'] not in valid_preferences:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': f'Invalid preference. Must be one of: {", ".join(valid_preferences)}'
            }),
            status_code=400,
            mimetype="application/json"
        )
    
    conn = get_db_connection()
    cursor = conn.cursor()

    # Begin transaction
    conn.autocommit = False

    try:
        # Check if student exists
        cursor.execute("""
            SELECT CommunicationID
            FROM Student
            WHERE StudentID = ?
        """, student_id)
        
        student = cursor.fetchone()
        if not student:
            return HttpResponse(
                json.dumps({
                    'status': 'error',
                    'message': f'Student ID {student_id} not found'
                }),
                status_code=404,
                mimetype="application/json"
            )
        
        communication_id = student[0]

        # Update communication information
        cursor.execute("""
            UPDATE Communication
            SET PhoneNumber = ?,
                Email = ?,
                Preference = ?
            WHERE CommunicationID = ?
        """, update_data['phoneNumber'], 
            update_data['email'], 
            update_data['preference'], 
            communication_id)

        # Get updated information
        cursor.execute("""
            SELECT s.StudentID,
                    s.FirstName,
                    s.LastName,
                    c.PhoneNumber,
                    c.Email,
                    c.Preference
            FROM Student s
            JOIN Communication c ON s.CommunicationID = c.CommunicationID
            WHERE s.StudentID = ?
        """, student_id)

        columns = [column[0] for column in cursor.description]
        updated_info = dict(zip(columns, cursor.fetchone()))

        conn.commit()

        return HttpResponse(
            json.dumps({
                'status': 'success',
                'message': 'Communication information updated successfully',
                'data': updated_info
            }),
            status_code=200,
            mimetype="application/json"
        )
    
    except Exception as e:
        conn.rollback()
        raise e

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@app.route(route="student/update/address", auth_level=func.AuthLevel.ANONYMOUS)
def update_student_address(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Get update data from request body
        update_data = req.get_json()
        if not update_data or 'studentid' not in update_data:
            return HttpResponse(
                json.dumps({
                    'status': 'error',
                    'message': 'Student ID is required'
                }),
                status_code=400,
                mimetype="application/json"
            )
        student_id = update_data['studentid']

        if not 'homeAddress' not in update_data:
            return HttpResponse(
                json.dumps({
                    'status': 'error',
                    'message': 'Home address is required'
                }),
                status_code=400,
                mimetype="application/json"
            )
                        
        conn = get_db_connection()
        cursor = conn.cursor()

        # Begin transaction
        conn.autocommit = False

        try:
            # Check if student exists and update address
            cursor.execute("""
                UPDATE Student
                SET HomeAddress = ?
                OUTPUT 
                    inserted.StudentID,
                    inserted.FirstName,
                    inserted.LastName,
                    inserted.HomeAddress
                WHERE StudentID = ?
            """, update_data['homeAddress'], student_id)
            
            updated_row = cursor.fetchone()
            if not updated_row:
                return HttpResponse(
                    json.dumps({
                        'status': 'error',
                        'message': f'Student ID {student_id} not found'
                    }),
                    status_code=400,
                    mimetype="application/json"
                )
            
            # Create result dictionary
            columns = ['studentId', 'firstName', 'lastName', 'homeAddress']
            updated_info = dict(zip(columns, updated_row))

            conn.commit()

            return HttpResponse(
                json.dumps({
                    'status': 'success',
                    'message': 'Home address updated successfully',
                    'data': updated_info
                }),
                status_code=200,
                mimetype="application/json"
            )
        except Exception as e:
            conn.rollback()
            raise e

    except Exception as e:
        return HttpResponse(
            json.dumps({
                'status': 'error',
                'message': str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()