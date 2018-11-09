# -*- coding: utf-8 -*-
import fdb
import pandas as pd
import mysql.connector as conn
import config
import math

class VendaModel:
    def __init__(self):
        #colunas da tabela
        self.cod_venda = ""
        self.data = ""
        self.quantidade = 0
        self.valor = 0
        self.valor_total = float(0)
        self.custo_real = 0
        self.custo_total = 0
        self.sk_hora = 0
        self.sk_vendedor = 0
        self.sk_cliente = 0
        self.sk_produto = 0
        
        self._campos_insert = (
                'cod_venda',
                'data',
                'quantidade',
                'valor',
                'valor_total',
                'custo_real',
                'custo_total',
                'sk_hora',
                'sk_vendedor',
                'sk_cliente',
                'sk_produto'
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
        return "(%s, %s, %d, %f, %f, %f, %f, %s, %s, %s, %s)"
    
    #Retorna uma tupla com os valores para serem inseridos na tabela segundo a ordem especificada
    def valores(self):
        return (
                str(self.cod_venda),
                self.data,
                int(self.quantidade),
                float(self.valor),
                float(self.valor_total),
                float(self.custo_real),
                float(self.custo_total),
                int(self.sk_hora),
                int(self.sk_vendedor),
                int(self.sk_cliente),
                int(self.sk_produto)
        )

class VendaFato:
    def __init__(self):
        self.tabela = 'f_vendas'
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
    print("Iniciando ETL dos fatos de vendas...")
    f_con = fdb.connect(dsn="localhost:/var/lib/firebird/3.0/data/cplus.fdb", 
                  user="SYSDBA", 
                  password="m74e71",
                 charset="iso8859_1")
    
    sql = 'SELECT CODMOVENDA, CODVENDED, DATA, HORA, CODCLI FROM MOVENDA'
    c_movenda = pd.read_sql_query(sql, f_con)
    
    sql = 'SELECT CODMOVENDA, CODPROD, QUANTIDADE, VALORUNITARIO, CUSTOREAL FROM MOVENDAPROD'
    c_movendaprod = pd.read_sql_query(sql, f_con)
    
    #Dados para conexão com mysql
    mydb = conn.connect(
                host = config.db['host'],
                user = config.db['user'],
                passwd = config.db['passwd'],
                database = config.db['database']
        )
        
    #carrega as dimensões
    sql = "SELECT * FROM d_cliente"
    d_cliente = pd.read_sql_query(sql, mydb)
    
    sql = "SELECT * FROM d_hora"
    d_hora = pd.read_sql_query(sql, mydb)
    
    sql = "SELECT * FROM d_vendedor"
    d_vendedor = pd.read_sql_query(sql, mydb)
    
    sql = "SELECT * FROM d_produto"
    d_produto = pd.read_sql_query(sql, mydb)
    
    venda = VendaModel()
    fato = VendaFato()
    
    #Inicia a gravação na tabela fatos
    total = len(c_movendaprod.index)
    for index in range(0, total):
        #localiza registro na tabela movenda do cplus
        codmovenda = c_movendaprod.iloc[index]['CODMOVENDA']
        movenda = c_movenda[c_movenda['CODMOVENDA'] == codmovenda].iloc[0]
        
        #prepara data
        venda.cod_venda = movenda['CODMOVENDA']
        venda.data = movenda['DATA']
        
        #prepara a hora
        hora_movenda = movenda['HORA']
        hora_minuto = str(hora_movenda).split(':')
        hora = d_hora[(d_hora['hora'] == hora_minuto[0]) & \
                      (d_hora['minuto'] == hora_minuto[1])]
        venda.sk_hora = str(hora.iloc[0]['sk_hora'])
        
        #prepara o vendedor
        vendedor = d_vendedor[d_vendedor['codvended'] == movenda['CODVENDED']]
        if len(vendedor.index) < 1:
            venda.sk_vendedor = 11
        else:
            venda.sk_vendedor = str(vendedor.iloc[0]['sk_vendedor'])
        
        #prepara o cliente
        if movenda['CODCLI'] is not None:
            cliente = d_cliente[d_cliente['cod_cli'] == movenda['CODCLI']]
        else:
            cliente = d_cliente[d_cliente['cod_cli'] == '00000195']
        venda.sk_cliente = str(cliente.iloc[0]['sk_cliente'])
        
        #pega a linha de movendaprod para relacionar com d_produto e preparar as medidas
        codprod = c_movendaprod.iloc[index]['CODPROD']
        produto = d_produto[d_produto['cod_produto'] == codprod]

        #prepara o produto
        if len(produto.index) == 0:
            venda.sk_produto = 1
        else:
            venda.sk_produto = str(produto.iloc[0]['sk_produto'])
        
        #prepara as medidas
        movendaprod = c_movendaprod[c_movendaprod['CODMOVENDA'] == movenda['CODMOVENDA']].iloc[0]
        venda.quantidade = movendaprod['QUANTIDADE']
        venda.valor = valor_valido(movendaprod['VALORUNITARIO'])
        venda.valor_total = valor_valido(venda.valor * venda.quantidade)
        venda.custo_real = valor_valido(movendaprod['CUSTOREAL'])
        venda.custo_total = valor_valido(venda.custo_real * venda.quantidade)
        
        fato.insere(venda)
        fato.commit()
        #print("Commit %d / %d - %d\%"%(index, total, (index*100/total)))
        print("Commit {0} / {1} - {2:.2f}%".format(index, total, index*100/total))

def valor_valido(val):
    valor_ajustado = float(val)
    return 0 if math.isnan(valor_ajustado) else valor_ajustado
    
    
if __name__ == "__main__":
    etl()