import dedupe
from src.tratamento import TratamentoDados

trat_dblp = TratamentoDados('database/DBLP-Scholar/DBLP1.csv', 'ISO-8859-1')
trat_scholar = TratamentoDados('database/DBLP-Scholar/Scholar.csv')

trat_dblp.padronizar()
trat_dblp.preencher_com_moda('venue')
trat_dblp.preencher_com_caractere_vazio('authors')
trat_dblp.ordenar_colunas(["id","title","authors","venue","year"])

trat_scholar.padronizar()
trat_scholar.preencher_com_media('year', media_integer=True)
trat_scholar.preencher_com_caractere_vazio('venue')
trat_scholar.ordenar_colunas(["id","title","authors","venue","year"])

dblp = trat_dblp.get_db()
scholar = trat_scholar.get_db()

print(dblp)
print(scholar)