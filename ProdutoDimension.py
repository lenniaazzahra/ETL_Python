# -*- coding: utf-8 -*-
import fdb
import pandas as pd
import mysql.connector as conn
import config

class ProdutoModel:
    def __init__(self):
        #colunas da tabela
        self.cod_produto = ""
        self.nome = ""
        self.secao_1 = ""
        self.secao_2 = ""
        self.secao_3 = ""
        self.ativo = "s"
        
        self._campos_insert = (
                'cod_produto',
                'nome',
                'secao_1',
                'secao_2',
                'secao_3',
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
                self.cod_produto,
                self.nome,
                self.secao_1,
                self.secao_2,
                self.secao_3,
                self.ativo,
        )

class ProdutoDimension:
    def __init__(self):
        self.tabela = 'd_produto'
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
    #Prepara as variáveis com o sql do firebird
    print("Preparando SQL...")
    f_con = fdb.connect(dsn="localhost:/var/lib/firebird/3.0/data/cplus.fdb", 
                  user="SYSDBA", 
                  password="m74e71",
                 charset="iso8859_1")
    sql_produtos  = "SELECT PRODUTO.CODPROD, PRODUTO.CODSEC, PRODUTO.NOMEPROD, " + \
                         "SECAO.CLASSIFICACAO, SECAO.NOMESECAO " + \
                         "FROM PRODUTO " + \
                         "inner join SECAO ON PRODUTO.CODSEC=SECAO.CODSEC"
    c_produtos = pd.read_sql_query(sql_produtos, f_con)
    
    sql_secao = "SELECT * FROM SECAO"
    c_secoes = pd.read_sql_query(sql_secao, f_con)
    
    #Mantem na memória a base de dados que será gravada na tabela do MySql d_produto
    produtos = {
        'cod_produto': [],
        'produto': [],
        'classificacao': [],
        'secao_1': [],
        'secao_2': [],
        'secao_3': [],
        'ativo': []
    }
    print("Sql ajustado!")
    print("Carregando a tabela de produtos...")
    
    #Coloca todos os produtos de c_produtos (firebird) em produtos (memória)
    total_produtos = len(c_produtos.index)
    for index in range(0, total_produtos):
        produtos['cod_produto'].append(c_produtos.iloc[index]['CODPROD'])
        
        if (c_produtos.iloc[index]['NOMEPROD'] is None):
            nomeprod = 'indefinido'
        else:
            nomeprod = c_produtos.iloc[index]['NOMEPROD']
        produtos['produto'].append(nomeprod)
        
        if (c_produtos.iloc[index]['CLASSIFICACAO'] is None):
            classificacao = 'indefinido'
        else:
            classificacao = c_produtos.iloc[index]['CLASSIFICACAO']
        produtos['classificacao'].append(classificacao)
        
        produtos['secao_1'].append('')
        produtos['secao_2'].append('')
        produtos['secao_3'].append('')
        produtos['ativo'].append('s')
    print("Tabela de produtos carregada!")
    
    #Ajusta as seções e coloca em produtos (memória)
    print("Ajustando seção")
    for index, classificacao in enumerate(produtos['classificacao']):
        if classificacao != 'indefinido':
            nome_classificacao = c_secoes[c_secoes['CLASSIFICACAO'] == classificacao]
            if(len(nome_classificacao) > 0):        
                c3 = nome_classificacao.iloc[0]['NOMESECAO']
            else:
                c3 = None
            
            nome_classificacao = c_secoes[c_secoes['CLASSIFICACAO'] == classificacao[0:5]]
            if(len(nome_classificacao) > 0):        
                c2 = nome_classificacao.iloc[0]['NOMESECAO']
            else:
                c2 = None

            nome_classificacao = c_secoes[c_secoes['CLASSIFICACAO'] == classificacao[0:2]]
            if(len(nome_classificacao) > 0):        
                c1 = nome_classificacao.iloc[0]['NOMESECAO']
            else:
                c1 = None

            for a in range (0, 3):
                if c1 is None:
                    c1 = c2
                    c2 = c3
                    c3 = None
                elif c2 is None:
                    c2 = c3
                    c3 = None
                    
        else:
            c1 = c2 = c3 = 'indefinido'
            
        produtos['secao_1'][index] = c1
        produtos['secao_2'][index] = c2
        produtos['secao_3'][index] = c3
    print("Seções ajustadas!")
    
    #Grava os produtos na tabela d_produtos
    print("Gravando no banco de dados...")
    p_model = ProdutoModel()
    dim = ProdutoDimension()
    
    #Cria produto indefinido
    p_model.cod_produto = '0000000000'
    p_model.nome = 'indefinido'
    p_model.secao_1 = 'indefinido'
    p_model.secao_2 = 'indefinido'
    p_model.secao_3 = 'indefinido'
    p_model.ativo = produtos['ativo'][index]
    dim.insere(p_model)
    dim.commit()
    
    #cadastra todos os produtos
    for index, nome in enumerate(produtos['produto']):
        p_model.cod_produto = produtos['cod_produto'][index]
        p_model.nome = produtos['produto'][index][0:50]
        p_model.secao_1 = produtos['secao_1'][index]
        p_model.secao_2 = produtos['secao_2'][index]
        p_model.secao_3 = produtos['secao_3'][index]
        p_model.ativo = produtos['ativo'][index]
        dim.insere(p_model)
        dim.commit()
    print("ETL finalizado!")
    
if __name__ == "__main__":
    etl()