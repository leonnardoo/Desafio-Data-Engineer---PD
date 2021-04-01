# Desafio Data Engineer - PD

<img src="https://img.shields.io/apm/l/vim-mode"/>
<img src="https://img.shields.io/badge/Challenge-PasseiDireto-green"/>
<img src="https://img.shields.io/badge/DataEngineer-ETL-brightgreen"/>

# Objetivo
Este projeto visa atender ao desafio técnico para Data Engineer da empresa Passei Direto.

O projeto esta dividido em 2 partes:

1 parte:
- Modelagem do banco de dados
- Construcao de um banco de dados relacional (SQL Server)
- Insercao de dados ao banco
- Criacao de uma tabela para analise de dados de comportamento

2 parte (Utilizando framework Spark):
- Leitura dos dados do SQL Server da parte 1
- Leitura dos jsons separados em partes
- Analise de comportamento do usuario

# Preparação

Para replicar a solução feita é necessário ter:
- Python 3
- Conta na Databricks
- Conta na AWS

# Descrição da solução encontrada
- Criação de banco SQL Server como serviço AWS RDS
- Criação de script Python que le os arquivos json da 'Base A' e insere os dados no RDS.
- Criacao de uma base de dados para analise comportamental a apartir do RDS
- Criacao de notebook na Databricks para leitura e analise dos jsons da 'Base B'
- Notebook com as analises de comportamento e acesso a plataforma

# Licenca
Licenca MIT

# Autor
Leonnardo Pereira - Data Engineer