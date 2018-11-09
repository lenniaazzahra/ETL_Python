# -*- coding: utf-8 -*-
import fdb
import pandas as pd
import mysql.connector as conn
import config

class ClienteModel:
    def __init__(self):
        #colunas da tabela
        self.cod_cli = ""
        self.codigo = ""
        self.pessoa = ""
        self.sexo = ""
        self.nome = ""
        self.ativo = "s"
        
        self._campos_insert = (
                'cod_cli',
                'codigo',
                'pessoa',
                'sexo',
                'nome',
                'ativo'
        ) 
        
    #retorna para sequencia dos campos para o Insert na tabela
    def campos_insert(self):
        campos = "("
        for campo in self._campos_insert:
            campos += campo + ","
        campos = campos[:-1] + ") "
        return campos
    
    #retorna as posições de cada capo para o Insert na tabela
    def campos_tipos(self):
        tipo = "("
        for cont in range (0, len(self._campos_insert)):
            tipo += "%s,"
        tipo = tipo[:-1] + ")"
        return tipo
    
    #Retorna uma tupla com os valores para serem inseridos na tabela segundo a ordem especificada
    def valores(self):
        return (
                self.cod_cli,
                self.codigo,
                self.pessoa,
                self.sexo,
                self.nome,
                self.ativo
        )

class ClienteDimension:
    def __init__(self):
        self.tabela = 'd_cliente'
        self.mydb = conn.connect(
                host = config.db['host'],
                user = config.db['user'],
                passwd = config.db['passwd'],
                database = config.db['database']
        )
        self.cursor = self.mydb.cursor()
    
    def insere(self, model):
        sql  = "INSERT INTO "+ self.tabela + model.campos_insert()
        sql += " VALUES " + model.campos_tipos()
        self.cursor.execute(sql, model.valores())
        
    def commit(self):
        self.mydb.commit()
        
def etl():
    print("Iniciando ETL de clientes...")
    f_con = fdb.connect(dsn="localhost:/var/lib/firebird/3.0/data/cplus.fdb", 
                  user="SYSDBA", 
                  password="m74e71",
                 charset="iso8859_1")
    
    sql = "SELECT CLIENTE.CODCLI, CLIENTE.CODIGO, CLIENTE.NOMECLI, CLIENTE.CNPJ, CLIENTE.CPF, CLIENTE.SEXO " + \
          "FROM CLIENTE"
    c_cliente = pd.read_sql_query(sql, f_con)
    
    cliente = ClienteModel()
    dim = ClienteDimension()
    
    total = len(c_cliente.index)
    nomes_pd = pd.read_csv('nome.csv', sep=',')
    for index in range(0, total):
        cliente.cod_cli = c_cliente.iloc[index]['CODCLI']
        cliente.codigo = c_cliente.iloc[index]['CODIGO']
        cliente.nome = c_cliente.iloc[index]['NOMECLI']
        
        if c_cliente.iloc[index]['CPF'] == None:
            cliente.pessoa = 'J'
            cliente.sexo = None
        else:
            cliente.pessoa = 'F'
            s = c_cliente.iloc[index]['SEXO']
            if (s == None):
                nomes = cliente.nome.split(" ")
                if nomes[0][-1::] == 'a':
                    cliente.sexo = 'F'
                elif nomes[0][-1::] == 'o':
                    cliente.sexo = 'M'
                else: 
                    n = nomes_pd[nomes_pd['nome'] == nomes[0].upper()]
                    if len(n) > 0:
                        cliente.sexo = n.iloc[0]['sexo']
                    else:
                        cliente.sexo = 'N'
            else:
                cliente.sexo = s
                    
        dim.insere(cliente)
        dim.commit()
    print("ETL de clientes finalizado!")

if __name__ == "__main__":
    etl()