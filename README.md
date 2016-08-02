#Semantic Typing Service
Use this service to predict types of data after giving training data, see "Using the service" for details.

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

##Using the service
Before you can predict what kind of data something is you have to create semantic types and columns with data in the semantic types.  The following diagram represents the relationship of the semantic types and columns in the service:
<center><img src="readme_data/structure.pdf" alt="Data Structure" style="width: 85%"/></center>
I reccomend using swagger when you are trying the steps below for the first time since you don't have to worry about 

1. Create semantic types by using the `POST /semantic_types` with the class and property you want for the semantic type, just note that the class must be a valid URL which also has a valid (namespace) parent URL.  If you don't have any particular URL you want to use just make one up.
2. Create at least one column for each of the semantic types using the `POST /semantic_types/{type_id}` endpoint.  Keep in mind that you will need the semantic type's id for this; the id of the semantic type is returned when you 

###Swagger
To view documentation for each of the endpoints and try it out with data, go to [http://petstore.swagger.io/](http://petstore.swagger.io/) and paste `http://localhost:5000/api/spec.json` into the address bar the top of the page and click explore.  For some reason it always starts with all of the endpoints hidden, so don't forget to click on "Show/Hide" or "List Operations".
