import pyodbc
import pandas as pd
from datetime import datetime

class Database(object):
    conn = ""
    cursor = ""

    def __init__(self, login, senha, banco):
        try:
            self.connectDatabase(login, senha, banco)
            print('Conectado ao banco.')
        except Exception as e:
            print(e)

    def connectDatabase(self, login, senha, banco):
        self.conn = pyodbc.connect('Driver={SQL Server};'
                                   'Server=sqlserver.cuykhgf0tnoa.us-east-1.rds.amazonaws.com;'
                                   f'Database={banco};'
                                   f'UID={login};'
                                   f'PWD={senha};'
        )
        self.cursor = self.conn.cursor()

    def closeConnexionDatabase(self):
        self.conn.close()

    def executeDatabase(self, query):
        exec_query = query
        self.cursor.execute(exec_query)
        self.cursor.commit()

    def showQuery(self, sql):
        return pd.read_sql(sql, self.conn)

    def updateToday(self):
        query = '''SELECT
                    Novo.*
                    FROM
                    (SELECT
                    te.Id as StudentId,
                    te.State,
                    te.City,
                    te.UniversityId,
                    te.CourseId,
                    te.SignupSource,
                    FORMAT(te.RegisteredDate ,'yyyyMMdd', 'en-US') RegisteredDate,
                    FORMAT(te.RegisteredDate,'hh:mm:ss', 'en-US') RegisteredHour,
                    FORMAT(ts.SessionStartTime,'yyyyMMdd', 'en-US') SessionDate,
                    FORMAT(ts.SessionStartTime,'hh:mm:ss', 'en-US') SessionHour,
                    ts.StudentClient,
                    tsb.PaymentDate,
                    tsb.PlanType,
                    CASE 
                        WHEN tsb.PlanType = 'Mensal' THEN FORMAT(DATEADD(day,30,tsb.PaymentDate) ,'yyyyMMdd', 'en-US') 
                        WHEN tsb.PlanType = 'Anual' THEN FORMAT(DATEADD(year,1,tsb.PaymentDate ) ,'yyyyMMdd', 'en-US')
                        END AS ExpirationDate,
                    CASE 
                        WHEN tsb.PlanType = 'Mensal' THEN FORMAT(DATEADD(day,30,tsb.PaymentDate) ,'hh:mm:ss', 'en-US') 
                        WHEN tsb.PlanType = 'Anual' THEN FORMAT(DATEADD(year,1,tsb.PaymentDate ) ,'hh:mm:ss', 'en-US')
                        END AS ExpirationTime
                    FROM tb_students as te
                    LEFT JOIN tb_sessions as ts on te.id = ts.StudentId
                    LEFT JOIN tb_subscriptions as tsb on te.id = tsb.StudentId) AS Novo
                    
                    LEFT JOIN
                    
                    (SELECT StudentId, SessionDate, SessionHour, CONCAT(CONCAT(StudentId,FORMAT(SessionDate ,'yyyyMMdd', 'en-US')),FORMAT(SessionHour,N'hh\:mm\:ss')) Chave FROM tb_sessions_students_profile) AS PF
                    ON CONCAT(CONCAT(Novo.StudentId,Novo.SessionDate),Novo.SessionHour) = PF.Chave
                    WHERE Chave IS NULL;'''

        self.cursor.execute(query)
        print('UPDATE iniciando.')
        exist = self.cursor.fetchall()
        if len(exist) == 0:
            print(f'tb_sessions_students_profile sem alteracoes.')
        else:
            query_insert = '''INSERT INTO tb_sessions_students_profile
                               (StudentId, State, City, UniversityId, CourseId, SignupSource,
                               RegisteredDate, RegisteredHour, SessionDate, SessionHour, StudentClient,
                               PaymentDate, PlanType, ExpirationDate, ExpirationTime)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                               '''
            self.cursor.executemany(query_insert, exist)
            self.cursor.commit()
            print(f'{len(exist)} linhas foram adicionadas a tabela de analise de perfil.')

    def insert(self, arquivo, tabela):
        if tabela == 'tb_universities':
            lista = []

            for row in arquivo:
                id = int(row.get('Id'))
                name = str(row.get('Name'))

                self.cursor.execute(f"SELECT Id, Name FROM tb_universities"
                                    f" WHERE Id = {id}")
                exist = self.cursor.fetchall()
                if exist is None:
                    add = (id, name)
                    lista.append(add)

            query = "INSERT INTO tb_universities (Id, Name) VALUES (?, ?)"

            if len(lista) > 0:
                self.cursor.executemany(query, lista)
                self.cursor.commit()
                print(f"{tabela} atualizada/inserida com sucesso.")
            else:
                print(f"{tabela} sem alteracoes.")

        elif tabela == 'tb_courses':
            lista = []

            for row in arquivo:
                id = int(row.get('Id'))
                name = str(row.get('Name'))

                self.cursor.execute(f"SELECT Id, Name FROM tb_courses"
                                    f" WHERE Id = {id}")
                exist = self.cursor.fetchall()
                if exist is None:
                    add = (id, name)
                    lista.append(add)

            query = "INSERT INTO tb_courses (Id, Name) VALUES (?, ?)"

            if len(lista) > 0:
                self.cursor.executemany(query, lista)
                self.cursor.commit()
                print(f"{tabela} atualizada/inserida com sucesso.")
            else:
                print(f"{tabela} sem alteracoes.")
        elif tabela == 'tb_subjects':
            lista = []

            for row in arquivo:
                id = int(row.get('Id'))
                name = str(row.get('Name'))
                self.cursor.execute(f"""SELECT Id, Name FROM tb_subjects
                                        WHERE Id = {id}""")
                exist = self.cursor.fetchall()
                if exist is None:
                    add = (id, name)
                    lista.append(add)

            query = "INSERT INTO tb_subjects (Id, Name) VALUES (?, ?)"

            if len(lista) > 0:
                self.cursor.executemany(query, lista)
                self.cursor.commit()
                print(f"{tabela} atualizada/inserida com sucesso.")
            else:
                print(f"{tabela} sem alteracoes.")
        elif tabela == 'tb_students':
            lista = []

            for row in arquivo:
                id = str(row.get('Id'))
                registeredDate = datetime.strptime(row.get('RegisteredDate'), '%Y-%m-%d %H:%M:%S.%f')
                state = str(row.get('State'))
                city = str(row.get('City'))
                universityId = int(row.get('UniversityId'))
                courseId = int(row.get('CourseId'))
                signupSource = str(row.get('SignupSource'))

                self.cursor.execute(f"SELECT Id, RegisteredDate, State, City, UniversityId, CourseId, SignupSource"
                                             f" FROM tb_students"
                                             f" WHERE Id = '{id}'"
                                             )
                exist = self.cursor.fetchall()
                if exist is None:
                    add = (id, registeredDate, state, city, universityId, courseId, signupSource)
                    lista.append(add)

            query = "INSERT INTO tb_students (Id, RegisteredDate, State, City, UniversityId, CourseId, SignupSource)" \
                    "VALUES (?, ?, ?, ?, ?, ?, ?)"

            if len(lista) > 0:
                self.cursor.executemany(query, lista)
                self.cursor.commit()
                print(f"{tabela} atualizada/inserida com sucesso.")
            else:
                print(f"{tabela} sem alteracoes.")
        elif tabela == 'tb_sessions':
            lista = []
            for row in arquivo:
                StudentId = str(row.get('StudentId'))
                SessionStartTime = datetime.strptime(row.get('SessionStartTime'), '%Y-%m-%d %H:%M:%S')
                StudentClient = str(row.get('StudentClient'))

                self.cursor.execute(
                    f"SELECT StudentId, SessionStartTime, StudentClient"
                    f" FROM tb_sessions"
                    f" WHERE"
                    f" StudentId = '{StudentId}'"
                    f" AND CAST(SessionStartTime AS VARCHAR) = CAST(CONVERT(DATETIME, '{SessionStartTime}', 120) AS VARCHAR)"
                    f" AND StudentClient = '{StudentClient}'")
                exist = self.cursor.fetchall()
                if exist is None:
                    add = (StudentId, SessionStartTime, StudentClient)
                    lista.append(add)

            query = "INSERT INTO tb_sessions (StudentId, SessionStartTime, StudentClient)" \
                    "VALUES (?, ?, ?)"

            if len(lista) > 0:
                self.cursor.executemany(query, lista)
                self.cursor.commit()
                print(f"{tabela} atualizada/inserida com sucesso.")
            else:
                print(f"{tabela} sem alteracoes.")
        elif tabela == 'tb_subscriptions':
            lista = []

            for row in arquivo:
                StudentId = str(row.get('StudentId'))
                PaymentDate = datetime.strptime(row.get('PaymentDate')[:19], '%Y-%m-%d %H:%M:%S')
                PlanType = str(row.get('PlanType'))
                self.cursor.execute(
                    f"SELECT StudentId, PaymentDate, PlanType"
                    f" FROM tb_subscriptions"
                    f" WHERE"
                    f" StudentId = '{StudentId}'"
                    f" AND CAST(PaymentDate AS VARCHAR) = CAST(CONVERT(DATETIME, '{PaymentDate}', 121) AS VARCHAR)"
                    f" AND PlanType = '{PlanType}'")
                exist = self.cursor.fetchall()
                if exist is None:
                    add = (StudentId, PaymentDate, PlanType)
                    lista.append(add)

            query = "INSERT INTO tb_subscriptions (StudentId, PaymentDate, PlanType)" \
                    "VALUES (?, ?, ?)"

            if len(lista) > 0:
                self.cursor.executemany(query, lista)
                self.cursor.commit()
                print(f"{tabela} atualizada/inserida com sucesso.")
            else:
                print(f"{tabela} sem alteracoes.")
        elif tabela == 'tb_student_follow_subject':
            lista = []

            for row in arquivo:
                StudentId = str(row.get('StudentId'))
                SubjectId = int(row.get('SubjectId'))
                FollowDate = datetime.strptime(row.get('FollowDate')[:19], '%Y-%m-%d %H:%M:%S')
                self.cursor.execute(
                    f"SELECT StudentId, SubjectId, FollowDate"
                    f" FROM tb_student_follow_subject"
                    f" WHERE"
                    f" StudentId = '{StudentId}'"
                    f" AND SubjectId = {SubjectId}"
                    f" AND CAST(FollowDate AS VARCHAR) = CAST(CONVERT(DATETIME, '{FollowDate}', 120) AS VARCHAR)")
                exist = self.cursor.fetchall()
                if exist is None:
                    add = (StudentId, SubjectId, FollowDate)
                    lista.append(add)

            query = "INSERT INTO tb_student_follow_subject (StudentId, SubjectId, FollowDate)" \
                    "VALUES (?, ?, ?)"

            if len(lista) > 0:
                self.cursor.executemany(query, lista)
                self.cursor.commit()
                print(f"{tabela} atualizada/inserida com sucesso.")
            else:
                print(f"{tabela} sem alteracoes.")
        else:
            print(f"Tabela {tabela} nao encontrado.")