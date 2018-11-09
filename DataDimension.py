#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import mysql.connector as conn
import pandas as pd
from datetime import date
import config

"""
Modelo que representa as colunas de uma linha na tabela d_tempo
"""
class DataModel:
    def __init__(self):
        #colunas da tabela
        self.data = ""
        self.ordinal = 0
        self.dia_semana = ""
        self.semana = ""
        self.dia = ""
        self.mes = ""
        self.ano = ""
        self.feriado = ""
        self.ferias = ""
        
        #Carrega o arquivo com os feriados
        self.feriados = pd.read_csv(config.feriados, sep=';')
        
    #retorna para sequencia dos campos para o Insert na tabela
    def campos_insert(self):
        return " (data, ordinal, dia_semana, semana, dia, mes, ano, fim_semana, feriado, ferias) "
    
    #retorna as posições de cada capo para o Insert na tabela
    def campos_tipos(self):
        return "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    #A partir da data (no formato yyyy-mm-dd) calcula todas as outras medidas
    def completar(self):
        d = self.data.split("-")
        data = date(int(d[0]), int(d[1]), int(d[2]))
        weekday = self._dia_da_semana(data.weekday())
        
        self.ordinal = int(data.toordinal())
        self.dia = str(data.day)
        self.mes = str(data.month)
        self.ano = str(data.year)
        self.dia_semana = str(weekday)
        self.fim_semana = self._fim_semana(weekday)
        self.ferias = self._ferias(data.month)
        self.feriado = self._feriado(data)
    
    #Transforma o dia da semana para o padrão onde domingo é igual a 1 e sábado 7
    def _dia_da_semana(self, weekday):
        if weekday>=0 and weekday<=5:
            return weekday+2
        else:
            return 1
        
    #Retorna 's' se o mês é mês de férias
    def _ferias(self, mes):
        return 's' if (mes in config.meses_de_ferias) else 'n'
        
    #Retorna 's' se o dia é final de semana
    def _fim_semana(self, weekday):
        return 's' if (weekday in config.dias_final_semana) else 'n'
    
    #Retorna 's' se o dia é um feriado
    def _feriado(self, data):
        if(data.day <= 9):
            dia = "0" + str(data.day)
        else:
            dia = str(data.day)
        d = dia + "/" + str(data.month) + "/" + str(data.year)
        return 'n' if(self.feriados[self.feriados.data==d].empty) else 's'
        
    #Retorna uma tupla com os valores para serem inseridos na tabela segundo a ordem especificada
    def valores(self):
        return (self.data,
                self.ordinal,
                self.dia_semana,
                self.semana,
                self.dia,
                self.mes,
                self.ano,
                self.fim_semana,
                self.feriado,
                self.ferias)
        
    #Devolve a próxima data (self.data + 1)
    def proxima_data(self):
        d = self.data.split("-")
        data = date(int(d[0]), int(d[1]), int(d[2]))
        ordinal = data.toordinal()+1
        nova_data = data.fromordinal(ordinal)
        str_data = str(nova_data.year) + "-" + str(nova_data.month) + "-" + str(nova_data.day)
        return str_data
    
#Classe que grava o modelo DataModel no banco de dados
class DataDimension:
    def __init__(self):
        self.mydb = conn.connect(
                host = config.db['host'],
                user = config.db['user'],
                passwd = config.db['passwd'],
                database = config.db['database']
        )
        self.cursor = self.mydb.cursor()
    
    def insere(self, model):
        sql  = "INSERT INTO d_tempo "+ model.campos_insert()
        sql += " VALUES " + model.campos_tipos()
        self.cursor.execute(sql, model.valores())
        
    def commit(self):
        self.mydb.commit()
        
#prepara e utiliza as classes acima para criar a dimensão tempo
def etl():
    data = DataModel()
    dim = DataDimension()
    
    data.data = config.data_inicial
    data.completar()
    ano_final = config.ano_final
    
    while int(data.ano) <= ano_final:
        data.completar()
        
        if(data.dia == '1'):
            data.semana = '1'
        elif(data.dia_semana == '1'):
            nova_semana = int(data.semana) + 1
            data.semana = str(nova_semana)
            
        dim.insere(data)
        dim.commit()
        
        data.data = data.proxima_data()
        
if __name__ == "__main__":
    etl()