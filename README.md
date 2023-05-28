# riskthinking
Work Sample for Data Engineer

### Running the project in a Docker container
1- Open CMD window on windows;
2- Move to the git repo folder;
3- Create the image based on the Dockerfile;
	docker build -t riskthinking .
4- Run the image based on the image riskthinking;
	docker run -it --rm -p 8080:8080 riskthinking /bin/bash
5- Wait until it starts the airflow portal
6- Open the airflow link bellow:
	http://localhost:8080/home
	Login: "admin"
	PSW: "admin"
7- Open the dag: flow_riskthinking_stock_market and press run 
