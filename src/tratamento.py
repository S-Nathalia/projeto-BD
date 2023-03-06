from pandas import DataFrame, pandas
from unidecode import unidecode
import re

class TratamentoDados:

    def __init__(self, filename: str, encoding: str='utf-8') -> None:
        self.db = pandas.read_csv(filename, encoding=encoding)

    def padronizar_coluna(self, coluna: DataFrame, nome_coluna: str) -> DataFrame:
        coluna_unicoded_list = []
        for i in coluna:
            dado = unidecode(str(i))
            dado = re.sub('\\s+', ' ', dado)
            dado = re.sub('\n', ' ', dado)
            coluna_unicoded_list.append(dado.strip().strip('"').strip("'").lower().strip())

        return DataFrame(coluna_unicoded_list, columns=[nome_coluna])

    def padronizar(self) -> None:
        colunas = DataFrame()
        for i in self.db:
            colunas = colunas.join(self.padronizar_coluna(self.db.get(i), i), how='right')
        
        self.db = colunas

    def get_db(self) -> DataFrame: 
        return self.db

    def media(self, numeros: list) -> float:
        soma = 0
        for n in numeros:
            soma += float(n)
        
        return soma / len(numeros)

    def check_converter_int(self, converter: bool, num: float):
        if converter:
            return int(num)
        return num

    def preencher_com_media(self, coluna_nome: str, media_integer=False) -> None:
        coluna = self.db.get(coluna_nome)
        numeros = []
        for i in coluna:
            if i != 'nan':
                numeros.append(i)
        
        media = self.media(numeros)
        media = self.check_converter_int(media_integer, media)
        
        dados = []
        for dado in coluna:
            if dado == 'nan':
                dados.append(media)
            else:
                dados.append(self.check_converter_int(media_integer, float(dado)))

        self.db = self.db.drop(columns=[coluna_nome])
        self.db = self.db.join(DataFrame(dados, columns=[coluna_nome]), how='right')

    def get_chave_max(self, dados: dict) -> str:
        maior = max(dados.values())
        for i in dados:
            if maior == dados[i]:
                return i

    def preencher_com_moda(self, coluna_nome) -> None:
        coluna = self.db.get(coluna_nome)
        dados = {}
        for dado in coluna: 
            if dado != 'nan':
                if dado in dados:
                    dados[dado] += 1
                else:
                    dados[dado] = 1

        key = self.get_chave_max(dados)
        
        dados = []
        for dado in coluna: 
            if dado == 'nan':
                dados.append(key)
            else: 
                dados.append(dado)
        
        self.db = self.db.drop(columns=[coluna_nome])
        self.db = self.db.join(DataFrame(dados, columns=[coluna_nome]), how='right')

    def preencher_com_caractere_vazio(self, coluna_nome, padroniza_int=False):
        coluna = self.db.get(coluna_nome)
        dados = []
        for dado in coluna:
            if dado == 'nan':
                dados.append(None)
            else: 
                if padroniza_int:
                    dados.append(int(dado.split(".")[0]))
                else:
                    dados.append(dado)
        
        self.db = self.db.drop(columns=[coluna_nome])
        self.db = self.db.join(DataFrame(dados, columns=[coluna_nome]), how='right')

    def ordenar_colunas(self, ordem) -> None:
        db = self.db.copy() 
        db_ordenado = DataFrame()
        for coluna in ordem:
            db_ordenado = db_ordenado.join(DataFrame(db.get(coluna), columns=[coluna]), how='right')
        
        self.db = db_ordenado

    def adicionar_etiqueta(self, etiqueta, nome_da_coluna="DB"):
        quant_linhas = len(self.db.index)
        etiquetas = []
        for i in range(quant_linhas):
            etiquetas.append(etiqueta)
        coluna = DataFrame(etiquetas, columns=[nome_da_coluna])
        self.db = self.db.join(coluna)
