# Pipeline Ranking Pokemon

Este projeto extrai dados de Pokemons do BigQuery, complementa com dados da [PokeAPI](https://pokeapi.co/docs/v2) e salva em um banco PostgreSQL.

## Configuração do Ambiente
### Pré Requisitos
- [Python 3.8+](https://www.python.org/)
- [Docker](https://docs.docker.com/desktop/install/windows-install/)

Clone o projeto
```
git clone https://github.com/pulszao/ranking_pokemon.git
```

Adicione suas credenciais de acesso ao BigQuery no arquivo [credentials/big_query_credentials.json](/credentials/big_query_credentials.json)

### Configuração do Discord (Opcional)
Para receber notificações sobre o status do pipeline:

1. Configure um webhook no seu servidor Discord
2. Edite o arquivo [config.py](/config.py) e substitua a URL do webhook:
```
DISCORD_WEBHOOK_URL = None  # TODO: Add your Discord webhook URL
```

O pipeline enviará notificações automáticas quando:
- Iniciar a execução via cron
- Concluir com sucesso (informando quantos registros foram processados)  
- Falhar com erro (informando o motivo do erro)

## Rodando projeto

### Rodando via docker
No diretório do projeto, execute o comando abaixo para criar e inicializar os containers Docker:
```
docker-compose up --build -d
```
Este comando ativa automaticamente a cron, que está programada para executar o script [pokemon_pipeline.py](/pokemon_pipeline.py) uma vez por dia as 02:00

#### Executar o pipeline manualmente no container
Para executar o pipeline imediatamente sem esperar o agendamento:
```
docker exec -it ranking_pokemon-cron-1 python /cron/pokemon_pipeline.py
```

## Acessar o Banco de Dados

### Via pgAdmin (Interface Web)
- URL: [http://localhost:5050](http://localhost:5050/)
- Email: admin@admin.com
- Senha: admin

### Configurações de Conexão no pgAdmin:
- Host: db
- Port: 5432
- Database: ranking_pokemon
- Username: postgres
- Password: p

### Tabelas do banco
- `ranking_pokemon_bq` - Ranking proveniente do BigQuery
- `ranking_pokemon_api` - Dados complementares vindos da API [PokeAPI](https://pokeapi.co/docs/v2)
- `ranking_pokemon_merged` - Dados combinados das duas tabelas (`ranking_pokemon_bq` e `ranking_pokemon_api`)

## Fluxo do Pipeline

O pipeline executa as seguintes etapas de forma automatizada:

1. **Extração BigQuery** - Conecta ao BigQuery usando as credenciais configuradas e extrai dados de ranking dos Pokémons
2. **Armazenamento BQ** - Salva os dados extraídos na tabela `ranking_pokemon_bq` do PostgreSQL
3. **Enriquecimento PokeAPI** - Para cada Pokémon do ranking, busca dados complementares na PokeAPI (tipos, estatísticas, sprites, etc.)
4. **Armazenamento API** - Salva os dados da API na tabela `ranking_pokemon_api`
5. **Merge de Dados** - Combina os dados das duas fontes e salva na tabela final `ranking_pokemon_merged`
6. **Agendamento** - Todo o processo é executado automaticamente via cron às 02:00 diariamente

```
BigQuery → ranking_pokemon_bq → PokeAPI → ranking_pokemon_api → ranking_pokemon_merged
```
