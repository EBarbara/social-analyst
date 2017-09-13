# social-analyst
Um projeto de detecção e classificação de eventos em redes sociais. 
<br>
Trabalho das matérias Tópicos Especiais em Bancos de Dados e Busca e Recuperação de Informações

Instruções:
> O projeto está todo em python 3, php 5 e javascript.

>Captura de tweets
>>Necessário instalar as bibliotecas tweepy e python-dotenv<br>
>>Copiar o arquivo .env para a pasta do projeto<br>
>>Rodar o arquivo TwitterGetterStructuredModule.py

>Classificação
>>Necessário PySpark<br>
>>Criar as pastas tweets e classified_data na pasta do projeto<br>
>>Descompactar o arquivo trained_data.zip na pasta do projeto<br>
>>Rodar o arquivo App.py

>Visualização
>>Copiar a pasta map do github para um servidor html<br>
>>No arquivo data.ini, alterar o valor de DATA_DIR para o caminho para a pasta classified_data, dentro pasta do projeto<br>
>>Com o servidor funcionando, acessar localhost/map