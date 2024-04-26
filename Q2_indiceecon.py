import os.path

import requests
import pandas as pd
from config import datai, dataf, cod_serie, plot_serie_decomposition, mean_absolute_percentage_error, plot_serie
from sklearn.metrics import mean_squared_error
from math import sqrt
from statsmodels.tsa.stattools import coint


def request_api_bcb():
    # https://dadosabertos.bcb.gov.br/dataset/24363-indice-de-atividade-economica-do-banco-central---ibc-br
    # https://www3.bcb.gov.br/sgspub/consultarmetadados/consultarMetadadosSeries.do?method=consultarMetadadosSeriesInternet&hdOidSerieSelecionada= 24363  CODIGO DOS DADOS PARA API
    # Defina o código da série do IBC-Br
    # construindo o request
    url = f'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{cod_serie}/dados?formato=json&dataInicial={datai}&dataFinal={dataf}'

    # requisicao a API
    response = requests.get(url)

    # capturar resposta da API:
    if response.status_code == 200:
        # Ajuste aqui: use response.text em vez de response.content
        df_indice = pd.read_json(response.text)
        df_indice['data'] = pd.to_datetime(df_indice['data'], format='%d/%m/%Y')
        df_indice = df_indice.rename(columns={'valor': 'indice'})
        #salvar como CSV
        df_indice.to_csv('Q3_indice_ibc_br.csv', sep=';', header=True, index=False, encoding='utf-8-sig')
        # print(df_indice.head())
    else:
        print("Erro na solicitação: ", response.status_code)
    return df_indice

def read_icei(path_icei):
    #https://www.portaldaindustria.com.br/estatisticas/icei-indice-de-confianca-do-empresario-industrial/
    # Ler o arquivo Excel especificando que queremos pular as primeiras 7 linhas (portanto começando na linha 8) e ler as próximas 3 linhas
    # Usamos header=None para que pandas não trate a primeira linha lida como cabeçalho do DataFrame
    df_linhas = pd.read_excel(path_icei, sheet_name='Geral', header=None, skiprows=7, nrows=3)
    # Selecionar apenas as linhas de interesse, que são a primeira e a terceira linha lidas (equivalentes às linhas 8 e 10 do arquivo original)
    df_selecionado = df_linhas.iloc[[0, 2]]
    # Cortando as colunas 0, 1, 2
    df_selecionado = df_selecionado.drop(columns=[0, 1, 2])
    # Transpondo o DataFrame para que as linhas se tornem colunas
    df_indice_icei = df_selecionado.transpose()
    # renoamendo colunas
    df_indice_icei.columns = ['data', 'indice']
    #salvardataframe
    df_indice_icei.to_csv('Q3_indice_ICEI.csv', sep=';', header=True, index=False, encoding='utf-8-sig')
    # Exibição do DataFrame resultante
    print(df_selecionado)
    return df_indice_icei

def aggregate(df_consumo, group_columns, sum_columns):
    #agregar o dataframe estruturado nas outras etapas da funcao consumo
    aggregated_df = df_consumo.groupby(group_columns)[sum_columns].sum().reset_index()
    return aggregated_df


#fazer o request na API e download do indice IBC-Br
df_indice = request_api_bcb()

#ler indice ICEI - indice-de-confianca-do-empresario-industrial
path_icei = os.path.join('arquivos','indicedeconfiancadoempresarioindustrial_serierecente_marco2024.xlsx')
df_indice_icei = read_icei(path_icei)


# Carregando o DataFrame de consumo agregado
CSV_read = 'agg_consumo_ramo.csv'
df_consumo = pd.read_csv(CSV_read, encoding='utf-8-sig', sep=';', decimal='.', parse_dates=['Mês'])
#retirar a coluna TAG
cols_to_drop = [col for col in df_consumo.columns if "tag" in col.lower()]
df_consumo = df_consumo.drop(columns=cols_to_drop)
#agregar com base nas colunas com a lista de soma
sum_columns = ['Consumo Perfil Agente - MWmed']
group_columns = ['Mês']
df_consumo_mes = aggregate(df_consumo, group_columns, sum_columns)
# print(df_consumo_mes.head())
df_consumo_mes.to_csv('agg_consumo_mes.csv', sep=';', header=True, index=False, encoding='utf-8-sig')

# Renomear coluna de 'Mês' para 'data' em df_consumo_mes para correspondência
df_consumo_mes.rename(columns={'Mês': 'data'}, inplace=True)
# Realizar o merge IBC e consumo
df_final_icb = pd.merge(df_consumo_mes, df_indice, on='data', how='left').reset_index()
# Realizar o merge ICEI e consumo
df_final_icei = pd.merge(df_consumo_mes, df_indice_icei, on='data', how='left').reset_index()
#salvando DF
df_final_icb.to_csv('Q3_df_consumo_mes_indice.csv', sep=';', header=True, index=False, encoding='utf-8-sig')
df_final_icei.to_csv('Q3_df_consumo_mes_indice_icei.csv', sep=';', header=True, index=False, encoding='utf-8-sig')




##### APENAS PARA O EXERCICIO 4 #####
# salvar o CSV filtrando apenas o setor de "METALURGIA E PRODUTOS DE METAL" para usar no questionamento 4
df_consumo_mes_metalurgia = df_consumo[df_consumo['Ramo de Atividade'].str.contains('METALURGIA E PRODUTOS DE METAL', case=False, na=False)]
#retirar a coluna ramo da atividade
cols_to_drop = [col for col in df_consumo_mes_metalurgia.columns if "ramo" in col.lower()]
df_consumo_mes_metalurgia = df_consumo_mes_metalurgia.drop(columns=cols_to_drop)
# Renomear coluna de 'Mês' para 'data' em df_consumo_mes para correspondência
df_consumo_mes_metalurgia.rename(columns={'Mês': 'data'}, inplace=True)
#merge com o indice
df_final_metalurgia = pd.merge(df_consumo_mes_metalurgia, df_indice, on='data', how='left')
df_final_metalurgia.to_csv('Q4_df_consumo_mes_metalurgia_indice.csv', sep=';', header=True, index=False, encoding='utf-8-sig')



#analisando estatisticamente ambas as series:
# Ajuste para garantir que o índice seja temporal para df_consumo_mes
df_consumo_mes.set_index('data', inplace=True)
# Ajuste para garantir que o índice seja temporal para df_indice
df_indice.set_index('data', inplace=True)
df_indice_icei.set_index('data', inplace=True)

# # Filtrar df_indice para incluir apenas linhas a partir de datai
df_indice = df_indice[df_indice.index >= datai]
df_indice = df_indice[df_indice.index <= dataf]
df_indice_icei = df_indice_icei[df_indice_icei.index >= datai]
df_indice_icei = df_indice_icei[df_indice_icei.index <= dataf]

#rodando a decomposicao temporal
# plot_serie_decomposition(df_consumo_mes, model='additive', period=12)
# plot_serie_decomposition(df_indice, model='additive', period=12)
#plotar os dois graficos juntos:
nomeindice = 'ibc'
plot_serie(df_consumo_mes,df_indice,nomeindice)
nomeindice = 'icei'
plot_serie(df_consumo_mes,df_indice_icei, nomeindice)


# Verificando as colunas disponíveis após o merge
print("Colunas em df_final_icei:", df_final_icei.columns)



df_final_indices_totais = pd.merge(df_final_icb, df_final_icei, on='data', how='left').reset_index()
# df_final_icei['indice'] = pd.to_numeric(df_final_icei['indice'], errors='coerce')
# df_final_icb['indice'] = pd.to_numeric(df_final_icb['indice'], errors='coerce')
df_final_indices_totais['indice_y'] = pd.to_numeric(df_final_indices_totais['indice_y'], errors='coerce')

##METRICAS:
# Calculando a correlação de pearson
correlacao_icb = df_final_indices_totais.corr(numeric_only=True)['Consumo Perfil Agente - MWmed_x']['indice_x']
print(f"Correlação ICB: {correlacao_icb}")
correlacao_icei = df_final_indices_totais.corr(numeric_only=True)['Consumo Perfil Agente - MWmed_x']['indice_y']
print(f"Correlação ICEI: {correlacao_icei}")
#calculando MAPE
mean_absolute_percentage_error(df_consumo_mes['Consumo Perfil Agente - MWmed'], df_indice['indice'])
mean_absolute_percentage_error(df_consumo_mes['Consumo Perfil Agente - MWmed'], df_indice_icei['indice'])
#https://stephenallwright.com/good-mape-score/
#calculando a cointegracao: tendencia das series nao compartilham?
score, p_value, _ = coint(df_consumo_mes['Consumo Perfil Agente - MWmed'], df_indice['indice'])
print(f"Cointegração score: {score}")
print(f"P-value: {p_value}")

#correlacao de pearson eh favoravel, acima de 70%
#MAPE eh bem aceitavel com valores menores que 10%
#a cointegracao tem um p-valor de 0.84, mostrando que a tendencia das series nao compartilham uma tendencia conjunta de longo prazo

##sugestoes futuras:
#utilizar outras variaveis relacionadas, como: PIB, entre outros
#encontrar outros metodos para entender se uma variavel acompanha outra
