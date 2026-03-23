📊 Análise de Estoque Segmentado — SQL + Google Sheets
🎯 Objetivo

Realizar a análise de níveis de estoque por projeto e segmentação, considerando a classificação mais recente de cada produto e consolidando os dados para suporte à tomada de decisão.

🧩 Contexto

Empresas que operam múltiplos projetos precisam monitorar continuamente seus níveis de estoque. A segmentação permite priorização operacional e identificação de riscos.

⚙️ Metodologia

A solução foi construída utilizando SQL no padrão BigQuery e organizada em três etapas:

Identificação da segmentação mais recente por produto e projeto com funções analíticas (ROW_NUMBER()).
Integração da segmentação atual com a tabela de estoque.
Consolidação por data, projeto e segmento com métricas analíticas.
📈 Resultados e análises

Foram desenvolvidos indicadores para avaliar:

• Estoque total consolidado
• Distribuição por segmentação
• Participação percentual
• Quantidade de projetos e categorias
• Identificação do maior segmento
• Avaliação de itens sem segmentação
• Comparação entre projetos

🔍 Principais insights

A análise evidenciou elevada concentração de estoque na categoria SEM_SEGMENTACAO, indicando possível lacuna no processo de classificação dos itens e risco operacional.

🛠️ Tecnologias utilizadas
SQL (BigQuery)
Google Sheets
Funções analíticas (Window Functions)
Modelagem analítica
Análise exploratória
🔗 Acesso ao projeto

👉 https://docs.google.com/spreadsheets/d/1-h6qOj1uoPBf0ykCGN7Wuql_kwIaVdTp9k8MQ7-V0NU/edit?usp=sharing