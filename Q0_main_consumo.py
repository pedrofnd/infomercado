import pandas as pd
import os
import calendar
from config import path_db, lista


def consumo(file_path, nome_arquivo):
    df_consumo = pd.read_excel(
        file_path,
        sheet_name="003 Consumo", #procura a aba indicada
        header=7) #esse header indica que vamos comecar a ler a partir da linha padrao que contenha o 'link' do indice
    # perfis.to_excel("listaperfis_base.xlsx", index=False) #checar o aspecto do dataframe inicial
    print(f'lido o arquivos {file_path}/{nome_arquivo} aba 003 Consumo')
    # Tabelas disponíveis: retornar apenas a string das tabelas disponiveis nessa planilha (tem que passar para NA para usar o metodo str.contains)
    tabelas_disp = df_consumo.loc[df_consumo["Índice"].fillna("").str.contains("Tabela", na=False), "Índice"].tolist()

    # Células Vazias na Planilha: determinar o indice da tabela disponivel
    celulas_vazias = df_consumo["Índice"].isna()

    ##########   CONSUMO - PRIMEIRA TABELA   ##########
    # Definindo intervalo de referência da tabela apos o encontrar a tabela disponivel - indice = 0 para 2021 em diante. 2020 para tras =1 pois tem a tabela de horas
    if int(nome_arquivo[-4:]) > 2020:
        indice_primeiro_tabela = df_consumo.loc[df_consumo["Índice"].fillna("").str.contains("Tabela", na=False), "Índice"].index[0]
    else:
        indice_primeiro_tabela = df_consumo.loc[df_consumo["Índice"].fillna("").str.contains("Tabela", na=False), "Índice"].index[1]
    inicio = indice_primeiro_tabela + 1

    #define o indice inicial da tabela mais um e o fim da primeira tabela
    indice_celulas_vazias = celulas_vazias[celulas_vazias].index
    indice_proximo_tabela = indice_celulas_vazias[indice_celulas_vazias > indice_primeiro_tabela][0]
    fim = indice_proximo_tabela - 1

    # Filtrando tabela com base nos indices definidos anteriormente
    df_consumo = df_consumo.loc[inicio:fim]

    # Definindo nomes das colunas como a primeira linha da tabela e assegurar que sejam strings para momento de deletar as col desnecessarias
    # consumo.columns = consumo.iloc[0]
    df_consumo.columns = [str(col) for col in df_consumo.iloc[0]]
    df_consumo = df_consumo[1:]

    # Remove colunas onde todos os valores são NaN (axis=1 para coluna)
    df_consumo = df_consumo.dropna(axis=1, how='all')

    # Tirando as coluna que nao sao importantes:
    # inserindo restricoes para evitar que pequenas mudancas nos titulos incorram em erros no futuro
    cols_to_drop = [
        col for col in df_consumo.columns
        if col.lower().startswith("cn") #versao de 2019 nao existe
        if "sigla" in col.lower()
        or "cidade" in col.lower()
        or "distribuidora" in col.lower()
        or "consumo no ambiente livre" in col.lower()
        or "consumo de energia ajustado da parcela cativa" in col.lower()
    ]
    df_consumo = df_consumo.drop(columns=cols_to_drop)
    #Sigla / Nome Empresarial / CNPJ / Cidade / Data de Migração / Cód. Perfil Distribuidora / Consumo de energia ajustado da parcela cativa da carga

    # Convertendo coluna 'Mês' para datetime e filtrando valores inválidos para deletalos (caso de 2020 para tras estava dando erro)
    df_consumo['Mês'] = pd.to_datetime(df_consumo['Mês'], errors='coerce')
    df_consumo = df_consumo.dropna(subset=['Mês'])

    # Convertendo coluna de data (faremos o uso do 'if' por conta de algum possivel erro de nomecao na planilha)
    if "Data de Migração" in df_consumo.columns: # Convertendo a coluna para datetime e para string no formato desejado
        df_consumo['Data de Migração'] = pd.to_datetime(df_consumo['Data de Migração'])
        df_consumo['Data de Migração'] = df_consumo['Data de Migração'].dt.strftime('%m/%Y')
    if "Mês" in df_consumo.columns: # Convertendo a coluna para datetime e para string no formato desejado
        df_consumo['Mês'] = pd.to_datetime(df_consumo['Mês'])
        df_consumo['Mês'] = df_consumo['Mês'].dt.strftime('%m/%Y')

    #ajustar nome coluna
    df_consumo.rename(columns={"Consumo de energia ajustado de uma parcela de carga - MWh (RC c,j)": "Consumo Perfil Agente - MWh"},inplace=True)
    # Transformando em MWmédio
    df_consumo["Mês"] = pd.to_datetime(df_consumo["Mês"])
    # Converte a coluna 'Consumo Perfil Agente - MWh' para numérico, usando 'coerce' para tratar possíveis erros de conversão como NaN
    df_consumo["Consumo Perfil Agente - MWh"] = pd.to_numeric(df_consumo["Consumo Perfil Agente - MWh"],errors='coerce')
    # Calcula o Consumo Médio Diário em MW (MWmédio): denominador calcula o número total de horas em cada mês
    df_consumo["Consumo Perfil Agente - MWmed"] = df_consumo["Consumo Perfil Agente - MWh"] / (df_consumo["Mês"].apply(lambda x: calendar.monthrange(x.year, x.month)[1]) * 24)

    # Remover linhas onde a coluna "ramo" tem valores NaN ou vazios
    df_consumo = df_consumo.dropna(subset=['Ramo de Atividade'])
    #inserir coluna com o nome do arquivo
    df_consumo.insert(0, 'TAG', nome_arquivo)

    # Salvando o excel e CSV da capacidade de carga
    # df_consumo.to_excel(f'{csv_nome}_{nome_arquivo}.xlsx', index=False)
    # df_consumo.to_csv(f'{csv_nome}_{nome_arquivo}.csv', index=False, encoding='utf-8-sig')
    print(f'leitura e ajustes para o dataframe {nome_arquivo} \n')
    return df_consumo

def aggregate_and_save(df_consumo, csv_nome, group_columns, sum_columns, dic_colunas):
    csv_nome = 'agg_' + csv_nome #determinar path correto para salvar/ler o CSV
    #agregar o dataframe estruturado nas outras etapas da funcao consumo
    aggregated_df = df_consumo.groupby(group_columns)[sum_columns].sum().reset_index()
    create_csv(aggregated_df, csv_nome, dic_colunas)

def create_csv(df,csv_nome,dic_colunas):
    #colunas em cada dataframe que serão colocadas no dropduplicates
    ###salvar os dataframe:
    try:
        # print(f"vamos tentar encontrar a base de dados no diretorio atual",csv_nome)
        df_outside = pd.read_csv(f'{csv_nome}.csv', sep=';', decimal='.', parse_dates=['Mês'])
        # print("terminou de ler o banco de dados local",f'{csv_nome}.csv')
        # print("vamos printar o nome do csv e as colunas a remover:",csv_nome,' / ',dic_colunas)
        # print('comparar dataframes: df e df_outside:',df.head(), df_outside.head())
        colunas_para_drop_indices = dic_colunas  # Obtém os indices das colunas a serem removidas
        colunas_para_drop_nomes = df_outside.columns[colunas_para_drop_indices]  # Obtém os nomes das colunas a partir dos índices
        df_outside = pd.concat([df_outside, df]).drop_duplicates(subset=colunas_para_drop_nomes)
        print("concatenou os dois dataframes (dados novos + antigos) e removeu os duplicatas",f'{csv_nome}.csv\n')
        df_outside.to_csv(f'{csv_nome}.csv', sep=';', header=True, index=False, encoding='utf-8-sig')
    except FileNotFoundError:
        print("ops ... nao tinha os csv, vamos salvar somente com a data solcicitada: ",csv_nome)
        df.to_csv(f'{csv_nome}.csv', sep=';', header=True, index=False, encoding='utf-8-sig')
        print(f'salvou um novo csv com a data {csv_nome}\n')
    except Exception as e:
        print(f"Outro erro: {e}\n")
    return df


def verificar_duplicatas(csv_nome, dic_colunas):
    try:
        #ler o dataframe externo e setar as colunas a serem verificadas como duplicatas:
        df_outside = pd.read_csv(f'{csv_nome}.csv', encoding='utf-8-sig', sep=';', decimal='.', on_bad_lines='skip')
        colunas_para_drop_indices = dic_colunas  # Obtém os indices das colunas a serem removidas
        colunas_para_drop_nomes = df_outside.columns[colunas_para_drop_indices]  # Obtém os nomes das colunas a partir dos índices        # Lendo o CSV complelto com todas as bases
        # Verificando duplicatas
        duplicatas = df_outside.duplicated(subset=colunas_para_drop_nomes, keep=False)
        # Contando duplicatas
        num_duplicatas = duplicatas.sum()
        # Se houver duplicatas, exibe um relatório
        if num_duplicatas > 0:
            print(f"Encontradas {num_duplicatas} duplicatas com base nas colunas: {colunas_para_drop_nomes}.")
            # Opcional: Exibir as linhas duplicadas
            print(df_outside[duplicatas],'\n')
        else:
            print("*nao foram encontradas duplicatas com base nas colunas especificadas.\n")
    except FileNotFoundError:
        print("verificar duplicatas: ops ... nao tinha os csv \n")
    except Exception as e:
        print(f"Outro erro: {e}\n")



csv_nome = 'consumo'
path_agg = 'csv_agregado'
#dicionario com as colunas que quero usar no remover duplicatas
for anos in lista:
    nome_arquivo = 'dez' + anos
    file_path = os.path.join(path_db, "InfoMercado_Dados_Individuais-" + nome_arquivo + ".xlsx")
    #funcao para ler os arquivos do infomercado para cada ano da lista e salvar CSV
    print('#####  comencando func consumo  #####')
    df_consumo = consumo(file_path, nome_arquivo)
    #funcao criar CSV conjunto
    # dicionario com as colunas que quero usar no remover duplicatas
    dic_colunas =[0, 1, 2, 3, 4, 5]
    print('#####  comencando func csv  #####')
    create_csv(df_consumo,csv_nome,dic_colunas)
    #verificar se ha duplicatas:
    print('#####  comencando func verificar duplicatas  #####')
    verificar_duplicatas(csv_nome, dic_colunas)
    #####  salvar os CSVs agregados  #####:
    print('#####  comencando func de agregar as colunas para analises  #####')
    sum_columns = ['Consumo Perfil Agente - MWmed']
    #1 agregado pelo Nome Empresarial: somar para o nome empresarial e ramo (por infomercado e mes)
    dic_colunas =[0, 1, 2, 3]
    group_columns = ['TAG', 'Mês', 'Nome Empresarial', 'Ramo de Atividade']
    print('colunas a serem agregadas: ',group_columns)
    aggregate_and_save(df_consumo, csv_nome + '_nome', group_columns, sum_columns, dic_colunas)
    verificar_duplicatas('agg_' + csv_nome + '_nome', dic_colunas)
    #2 agregado pelo codigo da carga: somar para o codigo de carga e ramo (por infomercado e mes)
    dic_colunas =[0, 1, 2, 3, 4]
    group_columns = ['TAG', 'Mês', 'Cód. Carga', 'Carga', 'Ramo de Atividade']
    print('colunas a serem agregadas: ',group_columns)
    aggregate_and_save(df_consumo, csv_nome + '_carga', group_columns, sum_columns, dic_colunas)
    verificar_duplicatas('agg_' + csv_nome + '_carga', dic_colunas)
    #3 agregado pelo Ramo da Atividade: somar por ramo de atividade (por infomercado e mes)
    dic_colunas =[0, 1, 2]
    group_columns = ['TAG', 'Mês', 'Ramo de Atividade']
    print('colunas a serem agregadas: ',group_columns)
    aggregate_and_save(df_consumo, csv_nome + '_ramo', group_columns, sum_columns, dic_colunas)
    verificar_duplicatas('agg_' + csv_nome + '_ramo', dic_colunas)
    #4 agregado pelo Ramo da Atividade e Submercado: por ramo de atividade e submercado (por infomercado e mes)
    dic_colunas =[0, 1, 2, 3]
    group_columns = ['TAG', 'Mês', 'Ramo de Atividade', 'Submercado']
    print('colunas a serem agregadas: ',group_columns)
    aggregate_and_save(df_consumo, csv_nome + '_ramosub', group_columns, sum_columns, dic_colunas)
    verificar_duplicatas('agg_' + csv_nome + '_ramosub', dic_colunas)


##sugestoes futuras:
#utilizar outra fonte de dados (Base de Dados Geográfica da Distribuidora - BDGD)
##nao consegui, estava dando erro: https://dadosabertos.aneel.gov.br/dataset/base-de-dados-geografica-da-distribuidora-bdgd
#salvar dados no banco (tabela ja criada)
#entender se eh melhor usar bibliotecas como: xlwings, openpyxl, ...
#usar formato de classe para o projeto
