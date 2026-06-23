# Descrevendo aqui um passo a passo para futuros projetos com maven, kafka e docker

## Criar maven project 

### Gera o projeto:
```bash
mvn archetype:generate "-DgroupId=br.ufes.soe" "-DartifactId=mvnapp" "-DarchetypeArtifactId=maven-archetype-quickstart" "-DarchetypeVersion=1.4" "-DinteractiveMode=false"
```
### Altera no pom.xml:
```bash
<maven.compiler.release>21</maven.compiler.release>
```
### Compilar e executar:
```bash
mvn package
```
```bash
mvn compile
```
```bash
mvn exec:java "-Dexec.mainClass=br.ufes.soe.Pipe"
```

## Comandos Kafka:

### Criar topicos:
```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --create --topic streams-plaintext-input --bootstrap-server localhost:9092 
```
### Entrar no terminal do produtor:
```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-producer.sh --topic streams-plaintext-input --bootstrap-server localhost:9092
```
### Entrar no terminal do consumidor:
```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-consumer.sh --topic streams-pipe-output --from-beginning --bootstrap-server localhost:9092
```

## Docker:
### Sobe:
```bash
docker compose up -d
```
### Desce:
```bash
docker compose down -v
```
### Lista:
```bash
docker ps
```