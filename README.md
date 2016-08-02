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
<center><img src="/readme_data/structure.pdf" alt="Data Structure" style="width: 85%"/></center>
I reccomend using swagger when you are trying the steps below for the first time since you don't have to worry about 

1. Create semantic types by using the `POST /semantic_types` with the class and property you want for the semantic type, just note that the class must be a valid URL which also has a valid (namespace) parent URL.  If you don't have any particular URL you want to use just make one up.
2. Create at least one column for each of the semantic types using the `POST /semantic_types/{type_id}` endpoint.  Keep in mind that you will need the semantic type's id for this; the id of the semantic type is returned when you create the type but you can also get them using the `GET /semantic_types` endpoint.  Even though you can create as many columns in a semantic type as you want, when you are predicting the service will only return the semantic type it thinks the data belongs to and no details about the column.  When you're first creating the column you do not have to add data, but you do need to add data before predicting.  If you decide to create the column and add the data separately you can use the `POST /semanti_types/{column_id}` endpoint.  When you give data to the service, remember that each line is taken as a value, including blank lines.  Below is an exaple of a message body for a Silicon Valley Company Names column:<pre>
Google
Apple
eBay
Cisco
Adobe</pre>
3. Now that you have semantic types and columns with data, use can use the `POST /predict` endpoint to predict the semantic type of the data.  When you predict data do it in the same format as adding the data, and the more you provide the better.  The data you will get back from the service will contain the id semantic type and the how confident the semantic labeler is that the specific semantic type is the correct one for the given data, which ranges from 0 to 1.

###Swagger
To view documentation for each of the endpoints and try it out with data, go to [http://petstore.swagger.io/](http://petstore.swagger.io/) and paste `http://localhost:5000/api/spec.json` into the address bar the top of the page and click explore.  For some reason it always starts with all of the endpoints hidden, so don't forget to click on "Show/Hide" or "List Operations".
