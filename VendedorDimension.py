# -*- coding: utf-8 -*-
import fdb
import pandas as pd
import mysql.connector as conn
import config

class VendedorModel:
    def __init__(self):
        #colunas da tabela
        self.codvended = ""
        self.codigo = ""
        self.nome = ""
        
    #retorna para sequencia dos campos para o Insert na tabela
    def campos_insert(self):
        return " (codvended, codigo, nome) "
    
    #retorna as posições de cada capo para o Insert na tabela
    def campos_tipos(self):
        return "(%s, %s, %s)"
    
    #Retorna uma tupla com os valores para serem inseridos na tabela segundo a ordem especificada
    def valores(self):
        return (self.codvended,
                self.codigo,
                self.nome)

class VendedorDimension:
    def __init__(self):
        self.tabela = 'd_vendedor'
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
    f_con = fdb.connect(dsn="localhost:/var/lib/firebird/3.0/data/cplus.fdb", 
                  user="SYSDBA", 
                  password="m74e71",
                 charset="iso8859_1")
    
    vendedor_cplus = pd.read_sql_query("select CODVENDED, CODIGO, NOMEVENDED as NOME from VENDEDOR", f_con)
    
    vendedor = VendedorModel()
    dim = VendedorDimension()
    
    #cadastra vendedor indefinido
    vendedor.codigo = '000000'
    vendedor.codvended = '00000000'
    vendedor.nome = 'indefinido'
    dim.insere(vendedor)
    dim.commit()
    
    total = len(vendedor_cplus.index)
    for index in range(0, total):
        vendedor.codigo = vendedor_cplus.iloc[index]['CODIGO']
        vendedor.codvended = vendedor_cplus.iloc[index]['CODVENDED']
        vendedor.nome = vendedor_cplus.iloc[index]['NOME']
        dim.insere(vendedor)
        dim.commit()
        
if __name__ == "__main__":
    etl()
