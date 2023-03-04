import dedupe
from src.tratamento import *
from src.converter import *
import pandas as pd

import os
import csv
import logging
import optparse

## Setup

input_file_1 = 'database/DBLP-Scholar/DBLP1.csv'
input_file_2 = 'database/DBLP-Scholar/Scholar.csv'
output_file = 'database/DBLP-Scholar/dblp-scholar.csv'
settings_file = 'dblp-scholar'
training_file = 'database/dblp-scholar.json'

# Tratamento dos dados
trat_dblp = TratamentoDados(input_file_1, 'ISO-8859-1')
trat_scholar = TratamentoDados(input_file_2)

trat_dblp.padronizar()
trat_dblp.preencher_com_caractere_vazio('venue')
trat_dblp.preencher_com_caractere_vazio('authors')
trat_dblp.ordenar_colunas(["id","title","authors","venue","year"])
trat_dblp.adicionar_etiqueta('dblp')

trat_scholar.padronizar()
trat_scholar.preencher_com_caractere_vazio('year', padroniza_int=True)
trat_scholar.preencher_com_caractere_vazio('venue')
trat_scholar.ordenar_colunas(["id","title","authors","venue","year"])
trat_scholar.adicionar_etiqueta('scholar')

dblp = trat_dblp.get_db()
scholar = trat_scholar.get_db()

# Junta as bases
db = pd.concat([dblp, scholar])
# Embaralha as dados
db = db.sample(frac=1)
# print(db)
## Logging

# Dedupe usa o log do Python para mostrar ou suprimir a saída detalhada. 
# Este bloco de código permite alterar o nível de login na linha de comando. 
# Você não precisa disso se não quiser. Para habilitar o registro detalhado, 
# execute `python <caminho_do_arquivo> -v`
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING
if opts.verbose:
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

print('Gerando arquivo de dados ...')
conversor = Converter(db)
data_d = conversor.gerar_train_data(numero_exemplos=5000)
# conversor.salvar_dados(filename='dados_treino_100')

# Se já existir um arquivo de configurações, basta carregá-lo e pular o treinamento
if os.path.exists(settings_file):
    print('Lendo de ', settings_file)
    with open(settings_file, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)
else:
    ## Treinando

    # Defina os campos aos quais o desduplicador prestará atenção
    campos = [
        {'field' : 'title', 'type': 'String'},
        {'field' : 'authors', 'type': 'Name'},
        {'field' : 'venue', 'type': 'Address', 'has missing':True},
        {'field' : 'year', 'type': 'Exact', 'has missing':True},
        ]

    # Crie um novo objeto deduplicador e passe o modelo de dados para ele.
    deduper = dedupe.Dedupe(campos, num_cores=0, in_memory=False)

    # Se tivermos dados de treinamento salvos de uma execução anterior de desduplicação, 
    # procure-os e carregue-os. obs. se você quiser treinar do zero, exclua o arquivo de treino
    if os.path.exists(training_file):
        print('Lendo arquivo de treino de ', training_file)
        with open(training_file, 'rb') as f:
            deduper.prepare_training(data_d, f)
    else:
        deduper.prepare_training(data_d)

    ## Aprendizado ativo
    # Dedupe encontrará o próximo par de registros sobre o qual tem menos certeza e solicitará 
    # que você os rotule como duplicados ou não. use as teclas 'y', 'n' e 'u' para sinalizar 
    # duplicatas pressione 'f' quando terminar
    print('Iniciando aprendizagem ativa...')

    dedupe.console_label(deduper)

    # Usando os exemplos que acabamos de rotular, treine o desduplicador 
    # e aprenda os predicados de blocking
    deduper.train()

    # Quando terminar, salva o treinamento no disco
    with open(training_file, 'w') as tf:
        deduper.write_training(tf)

    # Salve os pesos e predicados no disco. Se o arquivo de configurações existir, 
    # pularemos todo o treinamento e aprendizado na próxima vez que executarmos esse arquivo.
    with open(settings_file, 'wb') as sf:
        deduper.write_settings(sf)

## Clustering

# `partition` retornará conjuntos de registros que a desduplicação acredita estarem 
# todos se referindo à mesma entidade.

print('Clustering...')
clustered_dupes = deduper.partition(data_d, 0.5)

print('# Sets duplicados', len(clustered_dupes))

## Escrevendo resultados

# Grava os dados originais em um CSV com uma nova coluna chamada 
# 'Cluster ID' que indica quais registros se referem uns aos outros.

cluster_membership = {}
for cluster_id, (records, scores) in enumerate(clustered_dupes):
    for record_id, score in zip(records, scores):
        cluster_membership[record_id] = {
            "Cluster ID": cluster_id,
            "confidence_score": score
        }

input_file = 'database/DBLP-Scholar/Scholar.csv'
with open(output_file, 'w') as f_output, open(input_file) as f_input:

    reader = csv.DictReader(f_input)
    fieldnames = ['Cluster ID', 'confidence_score'] + reader.fieldnames

    writer = csv.DictWriter(f_output, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        row_id = int(row['id'])
        row.update(cluster_membership[row_id])
        writer.writerow(row)