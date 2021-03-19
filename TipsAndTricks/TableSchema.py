#Formas de passar o Schema no Pyspark

## Dataframe com os nomes alturas e datas de nascimento dos muggywaras: One Piece-> https://onepieceex.net/

linhas = [("Luffy",19,"5 de Maio",1.74),
          ("Zoro",21,"11 de Novembro",1.81), 
          ("Nami",20,"3 de Julho",1.61),
          ("Usopp",19,"1 de Abril",1.76),
          ("Sanji",20,"2 de Março",1.80),
          ("Chopper",17,"24 de Dezembro",0.90),
          ("Robin",30,"6 de Fevereiro",1.88),
          ("Franky",36,"9 de Março",2.40),
          ("Brook",90,"2 de Abril",2.77)
          ]
colunas = ["Muggywara", "Idade", "Data de Nascimento", "Altura"]
## Automático
MuggyDF = spark.createDataFrame(linhas, colunas)
### O Dataframe fica:
"""
+---------+-----+------------------+------+
|Muggywara|Idade|Data de Nascimento|Altura|
+---------+-----+------------------+------+
|    Luffy|   19|         5 de Maio|  1.74|
|     Zoro|   21|    11 de Novembro|  1.81|
|     Nami|   20|        3 de Julho|  1.61|
|    Usopp|   19|        1 de Abril|  1.76|
|    Sanji|   20|        2 de Março|   1.8|
|  Chopper|   17|    24 de Dezembro|   0.9|
|    Robin|   30|    6 de Fevereiro|  1.88|
|   Franky|   36|        9 de Março|   2.4|
|    Brook|   90|        2 de Abril|  2.77|
+---------+-----+------------------+------+
"""
### Usando o printSchema teremos 
MuggyDF.printSchema()
"""
root
 |-- Muggywara: string (nullable = true)
 |-- Idade: long (nullable = true)
 |-- Data de Nascimento: string (nullable = true)
 |-- Altura: double (nullable = true)
"""
## Usando os métodos de DataTypes
from pyspark.sql.types import DoubleType, StringType, IntegerType,StructField, StructType

schema_1 = StructType([StructField("Muggywara", StringType(), False),
      StructField("Idade", StringType(), False), # passei como string Propositalmente
      StructField("Data de Nascimento", StringType(), False),
      StructField("Altura", DoubleType(), False)])
MuggyDF2 = spark.createDataFrame(linhas, schema_1)
MuggyDF2.printSchema()
"""
root
 |-- Muggywara: string (nullable = false)
 |-- Idade: string (nullable = false)
 |-- Data de Nascimento: string (nullable = false)
 |-- Altura: double (nullable = false)
"""

## Usando String
schema_2 = "Muggywara STRING, Idade INT, `Data de Nascimento` STRING, Altura DOUBLE"
MuggyDF3 = spark.createDataFrame(linhas, schema_2)
MuggyDF3.printSchema()
"""
root
 |-- Muggywara: string (nullable = true)
 |-- Idade: integer (nullable = true)
 |-- Data de Nascimento: string (nullable = true)
 |-- Altura: double (nullable = true)
"""

