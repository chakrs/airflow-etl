docker build --no-cache -t airflow .

docker run -d -p 8080:8080 --name airflow airflow

# remove container
docker rm -f airflow

#debug
docker run -it -p 8080:8080 --name airflow --entrypoint /bin/bash airflow

# To stop the container
docker stop airflow