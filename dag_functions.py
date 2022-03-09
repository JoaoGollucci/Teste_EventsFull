import sys
sys.dont_write_bytecode = True
from datetime import timedelta, datetime, date
import dateutil.relativedelta
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#from airflow.models import Variable
import pandas_gbq
import re

def GetClient():
    """
    Essa funcao retorna o client do bigquery.
    """

    return bigquery.Client()

def GetSpark():
    """
    Essa funcao retorna o spark session.
    """

    # start spark session
    return SparkSession.builder.appName("""SPARK_SESSION""").getOrCreate()

def GetDate(workflow:str):
    """
    Essa funcao coleta os parametros do airflow para identificar qual data sera utilizada para o processo.

    :param workflow: indica qual e o workflow para coletar os parametros customizados do airflow
    """

    # Coleta os parametros de datas dustomizadas
    tipo_data = str(getArgument("tipo_data_{workflow}".format(workflow=workflow.lower())))
    custom_data = str(getArgument("custom_data_{workflow}".format(workflow=workflow.lower())))
    print("TIPO DE DATA DO PROCESSO: " + tipo_data)
    print("DATA PERSONALIZADA: " + custom_data)

    # Verifica qual data deve ser utilizada para o processo
    if tipo_data == "1":
        return custom_data
    elif tipo_data == "0":
        return datetime.now().strftime("%Y%m%d")
    else:
        raise Exception('PARAMETRO DE DATA INVALIDO')

def GetDateDict(dataref:datetime):
    """
    Essa funcao retorna um dicionario com varias datas baseadas na data do processo.

    Datas disponiveis:
    - "D-1": Data de referencia menos um dia
    - "D-2": Data de referencia menos dois dias
    - "D-3": Data de referencia menos tres dias
    - "M-0": Data 'D-1' com formato de mes
    - "M-1": Data 'D-1' menos um mes
    - "M-2": Data 'D-1' menos dois meses
    - "M-3": Data 'D-1' menos dois meses
    - "M-4": Data 'D-1' menos dois meses
    - "FIRST_D-1": Primeiro dia do mes da data 'D-1'
    - "FIRST_D-2": Primeiro dia do mes da data 'D-2'

    :param dataref: data base do processo
    """

    date_dict = {}

    # Coleta a data para D-1
    print("COLETANDO DATA PARA D-1")
    day_ant = (datetime.strptime(dataref, "%Y%m%d") - dateutil.relativedelta.relativedelta(days=1)).strftime("%Y%m%d")
    print("DATA D-1: {data}".format(data=day_ant))

    # Adiciona o valor ao dicionario
    date_dict.update({"D-1":day_ant})

    # Coleta a data para D-2
    print("COLETANDO DATA PARA D-2")
    day_ant2 = (datetime.strptime(dataref, "%Y%m%d") - dateutil.relativedelta.relativedelta(days=2)).strftime("%Y%m%d")
    print("DATA D-2: {data}".format(data=day_ant2))

    # Adiciona o valor ao dicionario
    date_dict.update({"D-2":day_ant2})

    # Coleta a data para D-3
    print("COLETANDO DATA PARA D-3")
    day_ant3 = (datetime.strptime(dataref, "%Y%m%d") - dateutil.relativedelta.relativedelta(days=3)).strftime("%Y%m%d")
    print("DATA D-3: {data}".format(data=day_ant3))

    # Adiciona o valor ao dicionario
    date_dict.update({"D-3":day_ant3})

    # Coleta a data para M-0
    print("COLETANDO DATA PARA M-0")
    mes_atual = (datetime.strptime(day_ant, "%Y%m%d")).strftime("%Y%m")
    print("DATA M-0: {data}".format(data=mes_atual))

    # Adiciona o valor ao dicionario
    date_dict.update({"M-0":mes_atual})

    # Coleta a data para M-1
    print("COLETANDO DATA PARA M-1")
    mes_ant = (datetime.strptime(day_ant, "%Y%m%d") - dateutil.relativedelta.relativedelta(months=1)).strftime("%Y%m")
    print("DATA M-1: {data}".format(data=mes_ant))

    # Adiciona o valor ao dicionario
    date_dict.update({"M-1":mes_ant})

    # Coleta a data para M-2
    print("COLETANDO DATA PARA M-2")
    mes_ant2 = (datetime.strptime(day_ant, "%Y%m%d") - dateutil.relativedelta.relativedelta(months=2)).strftime("%Y%m")
    print("DATA M-2: {data}".format(data=mes_ant2))

    # Adiciona o valor ao dicionario
    date_dict.update({"M-2":mes_ant2})

    # Coleta a data para M-3
    print("COLETANDO DATA PARA M-3")
    mes_ant3 = (datetime.strptime(day_ant, "%Y%m%d") - dateutil.relativedelta.relativedelta(months=3)).strftime("%Y%m")
    print("DATA M-3: {data}".format(data=mes_ant3))

    # Adiciona o valor ao dicionario
    date_dict.update({"M-3":mes_ant3})

    # Coleta a data para M-4
    print("COLETANDO DATA PARA M-4")
    mes_ant4 = (datetime.strptime(day_ant, "%Y%m%d") - dateutil.relativedelta.relativedelta(months=4)).strftime("%Y%m")
    print("DATA M-4: {data}".format(data=mes_ant4))

    # Adiciona o valor ao dicionario
    date_dict.update({"M-4":mes_ant4})

    # Retorna o primeiro dia do mes
    print("COLETANDO PRIMEIRO DIA DO MES DE D-1")
    first_day = datetime.strftime(date(datetime.strptime(day_ant, "%Y%m%d").year, datetime.strptime(day_ant, "%Y%m%d").month, 1), "%Y%m%d")
    print(f"PRIMEIRO DIA DO MES DE D-1: {first_day}")

    # Adiciona o valor ao dicionario
    date_dict.update({"FIRST_D-1":first_day})

    # Retorna o primeiro dia do mes
    print("COLETANDO PRIMEIRO DIA DO MES DE D-2")
    first_day2 = datetime.strftime(date(datetime.strptime(day_ant2, "%Y%m%d").year, datetime.strptime(day_ant2, "%Y%m%d").month, 1), "%Y%m%d")
    print(f"PRIMEIRO DIA DO MES DE D-2: {first_day2}")

    # Adiciona o valor ao dicionario
    date_dict.update({"FIRST_D-2":first_day2})


    return date_dict

def FormatDate(dataref:datetime, date_type:str):
    """
    Essa funcao retorna um dicionario com a data formatada.

    Formatos de datas disponiveis:
    - "AAAA-MM"
    - "AAAA-MM-DD"

    :param dataref: data base do processo
    :param date_type: Tipo de data, poder D para o formato "%Y%m%d" ou M para o formato "%Y%m"
    """

    # Identifica qual é o formato de data adequado
    if date_type.upper() == 'D':
        data_struct = "%Y%m%d"

        date_dict = {}
        # Formata a Data para AAAA-MM-DD
        print("FORMATANDO DATA PARA AAAA-MM-DD")
        aaaa_mm_dd = datetime.strftime(datetime.strptime(dataref, data_struct), "%Y-%m-%d")
        print(f"DATA FORMATADA PARA AAAA-MM-DD: {aaaa_mm_dd}")

        # Adiciona o valor ao dicionario
        date_dict.update({"AAAA-MM-DD":aaaa_mm_dd})

    elif date_type.upper() == 'M':
        data_struct = "%Y%m"

        date_dict = {}
        # Formata a Data para AAAA-MM
        print("FORMATANDO DATA PARA AAAA-MM")
        aaaa_mm = datetime.strftime(datetime.strptime(dataref, data_struct), "%Y-%m")
        print(f"DATA FORMATADA PARA AAAA-MM: {aaaa_mm}")

        # Adiciona o valor ao dicionario
        date_dict.update({"AAAA-MM":aaaa_mm})

    else:
        raise Exception('TIPO DE DATA INVALIDO')

    return date_dict

def LoadData(sql:str):
    """
    Essa funcao inicia o client do BigQuery e coleta os dados de acordo com a string SQL passada no parametro.

    :param sql: String sql para realizar a coleta de dados
    """

    # Client for BigQuery
    client = GetClient()

    try:
        print("COLETANDO DADOS")
        # Executa a query no BigQuery
        return client.query(sql).to_dataframe()
    except Exception as e:
        raise Exception(str(e))

def DeleteData(sql):
    """
    Essa funcao inicia o client do BigQuery e excuta a query de delete.

    :param sql: String sql para realizar o delete dos dados
    """

    # Client for BigQuery
    client = GetClient()

    try:
        print("DELETANDO DADOS")
        query_job = client.query(sql)
        query_job.result()
        print("DADOS DELETADOS")
    except Exception as e:
        raise Exception(str(e))

def InsertData(dataframe, project, table_name, schema, mode):
    """
    Essa funcao Insere os dados na tabela passada como parametro.

    :param sql: String sql para realizar a coleta de dados
    """

    try:
        # Insere os dados no BigQuery utilizando a biblioteca pandas_gbq
        pandas_gbq.to_gbq(dataframe, table_name, project, table_schema=schema, if_exists=mode)
    except Exception as e:
        raise Exception(str(e))

def GetFuncionalidades(env):
    """
        Coleta as funcionalidades cadastradas na tabela de funcionalidades.

    :param sql: String sql para realizar a coleta de dados
    """

    spark = GetSpark()
    client = GetClient()

    functions_schema = StructType([
        StructField('desc_funcionalidade', StringType(), True)
    ])

    # Load data from Google BigQuery
    functions = client.query(f"""
                                SELECT
                                  desc_funcionalidade
                                FROM
                                  `telefonica-digitalsales.b2b_mve_silver.dash_funcionalidade_all`
                                where
                                  id_plataforma = {env}
                              """).to_dataframe()

    # Convert Pandas dataFrame to SparkDataFrame
    functions = spark.createDataFrame(functions, functions_schema)

    funcionalidades = []
    for value in functions.collect():
        space = re.sub(r' ', '_', value['desc_funcionalidade']).lower()
        special = re.sub(r'[ç]', 'c', space)
        a = re.sub(r'[áâãà]', 'a', special)
        e = re.sub(r'[éêè]', 'e', a)
        i = re.sub(r'[íîì]', 'i', e)
        o = re.sub(r'[óôõò]', 'o', i)
        u = re.sub(r'[úûù]', 'u', o)
        out = re.sub(r'[\(\)]', '', u)
        funcionalidades.append(out)

    return funcionalidades