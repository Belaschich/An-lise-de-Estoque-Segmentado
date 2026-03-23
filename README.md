🚀 📊 Análise de Estoque Segmentado — SQL + Python + Google Sheets
🎯 Objetivo

Realizar a análise dos níveis de estoque por projeto e segmentação, considerando a classificação mais recente de cada produto e consolidando os dados para suporte à tomada de decisão.

🧩 Contexto

Empresas que operam múltiplos projetos precisam monitorar continuamente seus níveis de estoque. A segmentação de itens permite priorização operacional, planejamento de reposição e identificação de riscos.

⚙️ Metodologia

A solução foi desenvolvida com um pipeline completo de dados, contemplando ingestão, tratamento, modelagem e análise.

🔹 1. Preparação e tratamento dos dados

Os arquivos brutos foram processados utilizando Python, com apoio de bibliotecas para manipulação e limpeza dos dados, garantindo consistência antes da análise.

🔹 2. Criação do banco e modelagem

Foi criado um banco de dados relacional local utilizando DBeaver como ferramenta de administração e execução de SQL.

As tabelas foram estruturadas para armazenar os dados de estoque e segmentação, permitindo consultas analíticas e validação da qualidade dos dados.

🔹 3. Análise com SQL (padrão BigQuery)

A análise principal foi realizada utilizando SQL com funções analíticas, considerando:

Identificação da segmentação mais recente por produto e projeto (ROW_NUMBER()).
Integração da segmentação atual com a tabela de estoque.
Consolidação por data, projeto e segmento com métricas analíticas.
🔹 4. Consolidação e visualização

A base final foi disponibilizada no Google Sheets, onde foram construídos indicadores e análises para avaliação do estoque.

📈 Resultados e análises

Foram desenvolvidos indicadores para avaliar:

• Estoque total consolidado
• Distribuição por segmentação
• Participação percentual
• Quantidade de projetos e categorias
• Identificação do maior segmento em estoque
• Avaliação de itens sem segmentação definida
• Comparação entre projetos

🔍 Principais insights

A análise evidenciou elevada concentração de estoque na categoria SEM_SEGMENTACAO, indicando possível lacuna no processo de classificação dos itens e risco operacional.

Esse cenário pode impactar:

• priorização operacional
• planejamento de reposição
• visibilidade sobre itens críticos
• eficiência da gestão de estoque

🛠️ Tecnologias utilizadas

Linguagens e ferramentas

Python
SQL (padrão BigQuery)

Bibliotecas

pandas (manipulação e limpeza de dados)
mysql-connector / SQLAlchemy (conexão com banco)
keyring (gestão segura de credenciais)

Banco de dados e ferramentas

MySQL (banco relacional local)
DBeaver (administração e execução de consultas)

Análise e visualização

Google Sheets
Funções analíticas (Window Functions)
Modelagem analítica
Análise exploratória de dados

🔗 Acesso ao projeto
👉 https://docs.google.com/spreadsheets/d/1-h6qOj1uoPBf0ykCGN7Wuql_kwIaVdTp9k8MQ7-V0NU/edit?usp=sharing