from pandas import DataFrame, pandas

class Converter:
    def __init__(self, base: DataFrame) -> None:
        self.db = base
        self.train_data = None

    def gerar_train_data(self, numero_exemplos: int=None, indice:str ='id') -> dict:
        if numero_exemplos == None:
            return self.gerar_train_data(len(self.db.index))
        else:
            cont = numero_exemplos
            dados = {}
            for dado in self.db.iterrows():
                colunas = {}
                for coluna in dado[1].index:
                    colunas[coluna] = dado[1][coluna]
                dados[dado[1][indice]] = colunas
                cont -= 1
                if cont == 0:
                    break
            
            self.train_data = dados
            return dados
    
    def salvar_dados(self, filename: str) -> None:
        f = open(f"{filename}.json", 'w')
        f.write(str(self.train_data))
        f.close