Semantic Typing Service
=======================

##Software Requirements
* Python 2.7
* Elasticsearch
* MongoDB
* Pyspark
* scikit-learn (maybe?)
* pandas (maybe?)

##Running the service
1. Start MongoDB by running "mongod" in the terminal
2. Start Elasticsearch by running the "elasticsearch" in your main elasticsearch directory
3. Run "server.py"

##Swagger UI
To view all of the documentation for each of the endpoints and try it out with data, go to [http://petstore.swagger.io/](http://petstore.swagger.io/) and paste the following into the address bar the top of the page and click explore: <pre>http://localhost:5000/api/spec.json</pre>  For some reason it always starts with all of the endpoints hidden, so don't forget to click on "Show/Hide" or "List Operations".
