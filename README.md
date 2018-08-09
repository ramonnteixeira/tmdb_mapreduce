Map reduce para aula de BigData da Universidade Barriga Verde.

Para rodar, utilize preferencialmente python >= 3

## Instalando dependências

```
pip install pymongo
pip install pandas
```


## Subindo mongodb

Caso não tenha um banco mongodb rodando em sua máquina, utilize o comando abaixo para subir uma imagem docker.

```
docker run -d -p 27017:27017 -e AUTH=no --name mongodb mongo
```


Após ter as dependências instalada e o mongo rodando, pode-se rodar os comandos:

1. ``` python import.py ``` : Fará a inclusão do dataset
2. ``` python reduce.py ``` : Executará o map reduce do dataset e exibirá os 20 artistas com maior número de participação em filmes

