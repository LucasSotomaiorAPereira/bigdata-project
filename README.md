# Projeto de Big Data: Análise de Combustível e Emissões de Carros (2000-2013)

## Visão Geral do Projeto
Este projeto analisa o consumo de combustível e os dados de emissão de CO2 de carros de 2000 a 2013 usando Hadoop MapReduce e PySpark. O conjunto de dados fornece insights sobre vários atributos de carros, tipos de combustível e seu impacto ambiental.

## Conjunto de Dados
O conjunto de dados usado neste projeto contém informações sobre carros e suas emissões de CO2 com base no consumo de combustível. As colunas no conjunto de dados incluem:

* Arquivo: Nome do arquivo de dados.
* Ano: Ano de coleta de dados.
* Fabricante: Nome do fabricante do carro.
* Modelo: Nome do modelo do carro.
* Descrição: Descrição do carro.
* Padrão Euro: Nível de conformidade com o padrão de emissão europeu.
* Faixa de Imposto: Faixa de imposto sobre o combustível.
* Transmissão: Modelo de transmissão.
* Tipo de Transmissão: Manual ou Automática.
* Capacidade do Motor: Potência do motor.
* Tipo de Combustível: Tipo de combustível usado.
* Métrica Urbana: Eficiência do combustível em áreas urbanas (sistema métrico).
* Métrica Extra Urbana: Eficiência do combustível em áreas extra urbanas (sistema métrico).
* Métrica Combinada: Eficiência de combustível combinada (sistema métrico).
* Imperial Urbano: Eficiência de combustível em áreas urbanas (sistema imperial).
* Imperial Extra Urbano: Eficiência de combustível em áreas extra urbanas (sistema imperial).
* Imperial Combinado: Eficiência de combustível combinada (sistema imperial).
* Nível de Ruído: Nível de ruído em decibéis.
* CO2: Emissão de CO2 em g/km.
* Emissões de THC: Emissões totais de hidrocarbonetos em g/km.
* Emissões de CO: Emissões totais de monóxido de carbono em g/km.
* Emissões de NOx: Emissões totais de óxido de nitrogênio em g/km.
* Emissões de THC NOx: Emissões combinadas de THC e NOx em g/km.
* Emissões de Partículas: Emissões de material particulado em g/km.
* Custo de Combustível 12.000 Milhas: Custo estimado de combustível para dirigir 12.000 milhas.
* Custo de Combustível 6.000 Milhas: Custo estimado de combustível para dirigir 6.000 milhas.
* Padrão 12 Meses: Taxa de imposto padrão do veículo por 12 meses.
* Padrão 6 Meses: Taxa de imposto padrão do veículo por 6 meses.
* Primeiro Ano 12 Meses: Taxa de imposto do veículo no primeiro ano por 12 meses.
* Primeiro Ano 6 Meses: Taxa de imposto do veículo no primeiro ano por 6 meses.
* Data da Mudança: Data da mudança da taxa de imposto do veículo.

## Tarefas Hadoop MapReduce e PySpark
O projeto inclui várias tarefas Hadoop MapReduce e PySpark para analisar os dados:

**Fácil:**

* Tarefa 1: Calcular as emissões totais de CO.
* Tarefa 2: Calcular o nível médio de ruído de carros com transmissão manual.
* Tarefa 3: Contar as ocorrências de cada modelo de carro.
* Tarefa 4: Calcular a eficiência média de combustível (Métrica Combinada).

**Médio:**

* Tarefa 1: Encontrar o modelo de carro mais eficiente em termos de combustível para cada tipo de combustível com base na Métrica Combinada.
* Tarefa 2: Calcular a média das emissões de CO2 para cada fabricante de automóveis.
* Tarefa 3: Calcular o custo médio de combustível para cada tipo de combustível para 6.000 e 12.000 milhas.
* Tarefa 4: Calcular o imposto médio do veículo para cada modelo de carro para taxas padrão e do primeiro ano (6 e 12 meses).

**Difícil:**

* Tarefa 9: Determinar o Padrão Euro com a maior contagem.
* Tarefa 10: Calcular a redução média anual de emissão de CO2 para cada fabricante.

## Código
Cada tarefa inclui classes Mapper, Reducer e Driver para Hadoop MapReduce e scripts PySpark com consultas SparkSQL para análise equivalente.
