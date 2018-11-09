#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import mysql.connector as conn
import config

"""
Modelo que representa as colunas de uma linha na tabela d_tempo
"""
class HoraModel:
    def __init__(self):
        #colunas da tabela
        self.hora = ""
        self.minuto = ""
        self.periodo = ""
        self.ordinal = 0
        
    #retorna para sequencia dos campos para o Insert na tabela
    def campos_insert(self):
        return " (hora, minuto, periodo, ordinal) "
    
    #retorna as posições de cada capo para o Insert na tabela
    def campos_tipos(self):
        return "(%s, %s, %s, %s)"
    
    #Retorna uma tupla com os valores para serem inseridos na tabela segundo a ordem especificada
    def valores(self):
        return (self.hora,
                self.minuto,
                self.periodo,
                self.ordinal)
        
#Classe que grava o modelo DataModel no banco de dados
class HoraDimension:
    def __init__(self):
        self.tabela = 'd_hora'
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
        
#prepara e utiliza as classes acima para criar a dimensão tempo
def etl():
    hora = HoraModel()
    dim = HoraDimension()
    
    hora.hora = '00'
    hora.minuto = '00'
    hora.periodo = 'madrugada'
    hora.ordinal = 1
    ordinal = 1
    
    while ordinal <= 1440:
        dim.insere(hora)
        dim.commit()
        
        h = int(hora.hora)
        if h >= 0 and h <= 5:
            hora.periodo = 'madrugada'
        elif h >= 6 and h <= 11:
            hora.periodo = 'manhã'
        elif h >= 12 and h <= 17:
            hora.periodo = 'tarde'
        elif h >= 18 and h <= 23:
            hora.periodo = 'noite'
            
        h += 1
        if hora.minuto == '59':
            hora.hora = "0" + str(h) if(h <= 9) else str(h)
            hora.minuto = '00'
        else:
            novo_minuto = int(hora.minuto) + 1
            hora.minuto = "0"+str(novo_minuto) if (novo_minuto<=9) else str(novo_minuto)
            
        ordinal += 1
        hora.ordinal = ordinal
        
if __name__ == "__main__":
    etl()