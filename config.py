import os
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
import pandas as pd
import numpy as np

path_db = 'arquivos'

#dados API
datai = '2021-01-01'
datai = pd.to_datetime(datai)
dataf = '2023-12-31'
dataf = pd.to_datetime(dataf)
print(datai,dataf)
cod_serie = '24363'

#dados para leitura do infomercado:
lista = ['2021', '2022', '2023']

#funcoes para decomposicao de series temporais:
def plot_serie_decomposition(data, model, period=12, title=None):
    fig, axes = plt.subplots(4, 1, figsize=(10, 10))
    decomposition = seasonal_decompose(data, model=model, period=period)
    if title is not None: fig.suptitle(title, y=1.001)  # Ajuste o valor de y para controlar a distância entre os títulos
    # Plotar a componente observada
    axes[0].plot(data.index, decomposition.observed)
    axes[0].set_ylabel('Observado')
    axes[0].set_title('Decomposição dos Dados - Observado')
    # Plotar a componente de tendência
    axes[1].plot(data.index, decomposition.trend)
    axes[1].set_ylabel('Tendência')
    axes[1].set_title('Decomposição dos Dados - Tendência')
    # Plotar a componente sazonal
    axes[2].plot(data.index, decomposition.seasonal)
    axes[2].set_ylabel('Sazonalidade')
    axes[2].set_title('Decomposição dos Dados - Sazonalidade')
    # Plotar os resíduos
    axes[3].plot(data.index, decomposition.resid)
    axes[3].set_ylabel('Resíduos')
    axes[3].set_title('Decomposição dos Dados - Resíduos')
    # Ajustar o layout para evitar sobreposição
    plt.tight_layout()
    plt.show()

def plot_serie(df_consumo_mes,df_indice,nomeindice):
    # decomposicao das series temporais
    decomposicao_consumo = seasonal_decompose(df_consumo_mes['Consumo Perfil Agente - MWmed'], model='additive',period=12)
    decomposicao_indice = seasonal_decompose(df_indice['indice'], model='additive', period=12)
    # Definindo o tamanho da figura para acomodar todos os subplots
    plt.figure(figsize=(14, 10))
    # Plota a decomposição do consumo
    plt.subplot(3, 2, 1)
    plt.plot(decomposicao_consumo.trend, label='Tendência', color='blue')
    plt.title('Tendência do Consumo')
    plt.legend()
    plt.subplot(3, 2, 3)
    plt.plot(decomposicao_consumo.seasonal, label='Sazonalidade', color='blue')
    plt.title('Sazonalidade do Consumo')
    plt.legend()
    plt.subplot(3, 2, 5)
    plt.plot(decomposicao_consumo.resid, label='Resíduo', color='blue')
    plt.title('Resíduo do Consumo')
    plt.legend()
    # Plota a decomposição do índice IBC-Br
    plt.subplot(3, 2, 2)
    plt.plot(decomposicao_indice.trend, label='Tendência', color='orange')
    plt.title(f'Tendência do Índice {nomeindice}')
    plt.legend()
    plt.subplot(3, 2, 4)
    plt.plot(decomposicao_indice.seasonal, label='Sazonalidade', color='orange')
    plt.title(f'Sazonalidade do Índice {nomeindice}')
    plt.legend()
    plt.subplot(3, 2, 6)
    plt.plot(decomposicao_indice.resid, label='Resíduo', color='orange')
    plt.title(f'Resíduo do Índice {nomeindice}')
    plt.legend()
    # Ajusta o layout para evitar sobreposição
    plt.tight_layout()
    # Exibe o gráfico
    plt.show()

#calcular MAPE
def mean_absolute_percentage_error(serie1, serie2):
    mape = np.mean(np.abs((serie1 - serie2) / serie1)) * 10
    print('mape = ', mape)
    return mape