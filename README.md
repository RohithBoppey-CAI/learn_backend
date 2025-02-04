Learning Docs: https://docs.google.com/document/d/1vdq-VQtikMhhyqlc6cKbCYe6rvg_pwlLdoLJKJQzJvo/edit?usp=sharing

Steps to run: 
- From the postgres > docker compose - run the container 
- From the test_connection, check if the postgres is able to connect or not

Sample .env:
```
FIRST_ENV="This is my first env variable"

# POSTGRES CONNECTION VARIABLES: ---------

# you can use IP address instead of in here
POSTGRES_HOST=172.21.0.2
```


Remaining tasks: 
- Mount the data from local static_data folder to 