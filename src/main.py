from config.connect_sqlserver import Database
from config import creds
import json

#Script python criado para o desafio de Data Engineer da Passei Direto.
#Os dados do arquivo "creds" foram substituidos pelo arquivo "creds_example" para que os dados sensíveis não fiquem publicos.
#Mas dentro do arquivo tem os nomes das variáveis que devem ser preenchidas para a correta execução do script.

if __name__ == "__main__":
    #Criação do objeto que vai se conectar com o banco e manipular os dados.
    objSQLServer = Database(creds.login_aws, creds.senha_aws, creds.banco_aws)

    #Caminho dos arquivos json
    caminho = creds.caminho_arquivos_json

    #Nome das bases jsons
    universities = "universities"
    courses = "courses"
    sessions = "sessions"
    studentfollowsubject = "student_follow_subject"
    students = "students"
    subjects = "subjects"
    subscriptions = "subscriptions"

    jsons = [universities, courses, subjects, students, subscriptions, sessions, studentfollowsubject]

    #Loop criado para fazer a inserção se o dado não existir na tabela do banco
    for file in jsons:
        tabela = f"tb_{file}"
        with open(caminho + file + ".json", encoding='utf-8') as f:
            file = json.loads(f.read())
        objSQLServer.insert(file, tabela)
        f.close()

    #Função que cria a tabela "tb_sessions_students_profile" para avaliar o comportamento dos usuários da plataforma
    objSQLServer.updateToday()