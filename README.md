# riskthinking
Work Sample for Data Engineer

### Running the project in a Docker container
- Open CMD window on windows;
- Move to the git repo folder;
- Create the image based on the Dockerfile;<br />
	docker build -t riskthinking .
- Run the image based on the image riskthinking;<br />
	docker run -it --rm -p 8080:8080 riskthinking /bin/bash
- Wait until it starts the airflow portal
- Open the airflow link bellow:<br />
	http://localhost:8080/home<br />
	Login: "admin"<br />
	PSW: "admin"
- Open the dag: flow_riskthinking_stock_market and press run 
