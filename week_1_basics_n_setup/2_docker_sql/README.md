# 02 Docker Setup

## Docker en WSL

* Iniciar docker daemon

```bash
sudo dockerd
docker run hello-world
```

## Docker en Windows

* Iniciar Docker Desktop
* Para ejecutar estos comandos se puede usar CMD o GitBash 

```bash
sudo dockerd
docker run hello-world
```

* Para usar una imagen de ubuntu:
`docker run -it ubuntu bash`

* Creamos una imagen de docker en un Dockerfile la cual instala la libreria pandas


## En DockerFile:

```bash
FROM python:3.9

RUN pip install pandas

ENTRYPOINT [ "bash" ]
```
Luego ejecutamos el siguiente comando para construir el contenedor con la instalacion de pandas, con la imagen de python 3.9

## En cmd:

```bash
docker build -t test:pandas .
```
Una vez creada, probamos el contenedor

```bash
docker run -it test:pandas
```

```wsl
python
```

```python
import pandas as pd
print(pd.__version__)
```

Luego de probar este primer contenedor, se creara un archivo `.py`:

```python
import sys
import pandas as pd

#Some fancy stuff with pandas

day = sys.argv[1]
print(f'Argumentos: '{sys.argv})
print(f'Version de pandas: '{pd.__version__})
print(f'job finished succesfully for day: '{day})
```
y ajustamos el `Dockerfile`:

```bash
FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]
```

Despues construimos y corremos el contenedor, donde en el run colocamos el argumento que queramos, como la fecha:

```bash
docker build -t test:pandas .
docker run -it test:pandas 2018-04-18
```





# POSTGRES CON DOCKER

## WSL: 
```bash
sudo su
sudo chmod -R 744 /home/ytapasco88/Learning/DE_Zoomcamp/week_1_basics_n_setup/2_docker_sql
sudo chmod -R 744 /home/ytapasco88/Learning/DE_Zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data/

sudo chmod 777 /home/ytapasco88/DE_Zoomcamp/week_1_basics_n_setup/2_docker_sql
sudo chmod 777 /home/ytapasco88/DE_Zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /$(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```


## CMD

```cmd

docker run -it ^
  -e POSTGRES_USER="root" ^
  -e POSTGRES_PASSWORD="root" ^
  -e POSTGRES_DB="ny_taxi" ^
  -v "postgresql-volume:/var/lib/postgresql/data" ^
  -v c:/Users/ytapa/OneDrive/Learning/DE_Zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data ^
  -p 5432:5432 ^
  postgres:13
```

```bash
pip install pgcli
pgcli --help
pgcli -h localhost -p 5432 -u root -W root -d ny_taxi
\dt
SELECT 1;
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
pip install psycopg2-binary 

```

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4


http://localhost:8080/browser/

docker network create pg-network

```

### Network


```bash
docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /$(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13

pgcli -h localhost -p 5432 -u root -W root -d ny_taxi

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4

```


```bash
pip install ipynb-py-convert
ipynb-py-convert upload-data.ipynb upload-data.py

```

pgcli -h localhost -p 5432 -u root -W root -d taxi

python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"



