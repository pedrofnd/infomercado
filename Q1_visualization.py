import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def aggregate(df_consumo, group_columns, sum_columns):
    #agregar o dataframe estruturado nas outras etapas da funcao consumo
    aggregated_df = df_consumo.groupby(group_columns)[sum_columns].mean().reset_index()
    return aggregated_df

def plot_consumo_setor(df_consumo_mes,column):
    #fazer o pivot da tabela para que os meses fiquem pra direita e os ramos pra baixo
    pivot_table = df_consumo_mes.pivot_table(index='Ramo de Atividade', columns= column, values='Consumo Perfil Agente - MWmed', aggfunc='mean')
    #criar a coluna media no final, na direita
    pivot_table['Média'] = pivot_table.mean(axis=1).astype(int)
    # Ordena a pivot table pela coluna 'Média' em ordem decrescente
    pivot_table = pivot_table.sort_values('Média', ascending=False)
    #se for mensal: gerar uma tabela com a sazo
    if column == 'Mês_num':
        # Copia o DataFrame para evitar alterações no original
        pivot_tabela_sazo = pivot_table.copy()
        # Calcula a média e normaliza os valores (exceto pela coluna 'Média')
        media = pivot_tabela_sazo['Média']
        for col in pivot_tabela_sazo.columns[:-1]:  # Exclui a coluna 'Média' da normalização
            pivot_tabela_sazo[col] = pivot_tabela_sazo[col] / media
        pivot_tabela_sazo.to_csv('pivot_table_mes_sazo.csv', sep=';', header=True, index=False, encoding='utf-8-sig')
        pivot_table.to_csv('pivot_table_mes.csv', sep=';', header=True, index=False, encoding='utf-8-sig')
    else:
        pivot_table.to_csv('pivot_table_ano.csv', sep=';', header=True, index=False, encoding='utf-8-sig')

    # Calculando os totais
    pivot_table.loc['Total', :] = pivot_table.sum().astype(int)

    ## parte de PLOT da tabela  ##

    # # Normalizar os dados para o mapa de calor (0-1)
    # norm = plt.Normalize(pivot_table.min().min(), pivot_table.max().max())
    # colors = plt.cm.Reds(norm(pivot_table.values))

    fig, ax = plt.subplots(figsize=(16, 10))  # Tamanho da figura
    fig.subplots_adjust(left=0.3, right=0.9)  # Ajuste as margens conforme necessário
    ax.axis('off') #esconder os quadrados que aparecem no grafico

    table = ax.table(
        cellText=pivot_table.values,
        colLabels=pivot_table.columns,
        rowLabels=pivot_table.index,
        loc='center',
        cellLoc='right',
        rowLoc='right',
        fontsize=14,
        # cellColours=colors
    )
    table.scale(1, 2)  # Aumentar o segundo parâmetro para mais espaço vertical
    # os textos estavam sendo omitidos para esquerda, por isso o objetivo foi alinnhar para direita
    for pos, cell in table.get_celld().items():
        if pos[1] == -1:  #seleciona as células dos rótulos das linhas
            cell.set_text_props(ha='right')  # Alinha o texto à direita
    plt.show()


# Carregando o DataFrame
CSV_read = 'agg_consumo_ramo.csv'
df_consumo = pd.read_csv(CSV_read, encoding='utf-8-sig', sep=';', decimal='.', parse_dates=['Mês'])
# Converter para float primeiro para lidar com NaNs
df_consumo['Consumo Perfil Agente - MWmed'] = pd.to_numeric(df_consumo['Consumo Perfil Agente - MWmed'],errors='coerce')
# Depois, preencher NaNs com um valor inteiro e converter para int
df_consumo['Consumo Perfil Agente - MWmed'] = df_consumo['Consumo Perfil Agente - MWmed'].fillna(0).astype(int)
## Convertendo valores para numérico, com coerção de erros
# Adicionando a coluna 'Mês Numérico'
df_consumo['Mês_num'] = df_consumo['Mês'].dt.month
#agregar 1: mes com base nas colunas com a lista de soma
sum_columns = ['Consumo Perfil Agente - MWmed']
group_columns = ['Mês_num', 'Ramo de Atividade']
df_consumo_mes = aggregate(df_consumo, group_columns, sum_columns)
df_consumo_mes['Consumo Perfil Agente - MWmed'] = pd.to_numeric(df_consumo['Consumo Perfil Agente - MWmed'], errors='coerce').fillna(0).astype(float)
df_consumo_mes.to_csv('agg_consumo_ramo_mes.csv', sep=';', header=True, index=False, encoding='utf-8-sig')

#plotar tabela
plot_consumo_setor(df_consumo_mes,'Mês_num')


##sugestoes futuras:
#fazer um mapa de calor para a tabela de mes e ano