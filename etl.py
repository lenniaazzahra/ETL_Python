# -*- coding: utf-8 -*-
from VendedorDimension import etl as vendedor_etl
from ClienteDimension import etl as cliente_etl
from ProdutoDimension import etl as produto_etl
from DataDimension import etl as data_etl
from HoraDimension import etl as hora_etl
from VendaFato import etl as fato_venda_etl

if __name__ == "__main__":
    print("ETL vendedor 1/6")
    vendedor_etl()
    
    print("ETL cliente 2/6")
    cliente_etl()
    
    print("ETL cliente 3/6")
    produto_etl()
    
    print("ETL data 4/6")
    data_etl()
    
    print("ETL hora 5/6")
    hora_etl()
    
    print("ETL fatos vendas 6/6")
    fato_venda_etl()
    print("ETL completo!")
    