(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();
    var cols = [];    

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {

        var cosmosConnectionInfo = JSON.parse(tableau.connectionData),
        account = cosmosConnectionInfo.account,
        key = cosmosConnectionInfo.key,
        database = cosmosConnectionInfo.database,
        collection = cosmosConnectionInfo.collection,
        query = "SELECT TOP 1 * from c";

        var queryInfo = {
            QueryText : query,
            AccountUri: account,
            Key : key,
            Database: database,
            Collection: collection
        };

        var data = JSON.stringify(queryInfo);

        $.post("https://cosmosdbquery.azurewebsites.net/api/CosmosDBQuery?code=b0dC8AhGXaqOhRIrOOMXGGlnd0fcOGnekGEXd8MkKx10cVdXQMoCnQ==", data, function(resp) {
            tableData = [];
            console.log("Cosmos db result is " + resp);            
            var respObj = JSON.parse(resp);
            var colNames = Object.keys(respObj[0]);
            for (var i = 0, len = colNames.length; i < len; i++) {
                if(colNames[i] != ',')
                {
                    cols.push({
                        id : colNames[i],
                        dataType: tableau.dataTypeEnum.string
                    }); 
                }        

            };
            var tableSchema = {
                id: "CosmosDbData",
                alias: "Data loaded from Cosmos DB",
                columns: cols
            };
    
            schemaCallback([tableSchema]);
        }, "json");
    };


    // Download the data
    myConnector.getData = function(table, doneCallback) {

        var cosmosConnectionInfo = JSON.parse(tableau.connectionData),
            account = cosmosConnectionInfo.account,
            key = cosmosConnectionInfo.key,
            database = cosmosConnectionInfo.database,
            collection = cosmosConnectionInfo.collection,
            query = cosmosConnectionInfo.query;

        var queryInfo = {
            QueryText : query,
            AccountUri: account,
           Key : key,
           Database: database,
           Collection: collection
        };

        var data = JSON.stringify(queryInfo);

        $.post("https://cosmosdbquery.azurewebsites.net/api/CosmosDBQuery?code=b0dC8AhGXaqOhRIrOOMXGGlnd0fcOGnekGEXd8MkKx10cVdXQMoCnQ==", data, function(resp) {
            tableData = [];
            console.log("Cosmos db result is " + resp);
            
            var respObj = JSON.parse(resp);
            // Iterate over the JSON object
            var tableData = [];
            

            for (var i = 0, len_i = respObj.length; i < len_i; i++) {
                var tableObj = {};
                for (var j = 0, len_j = cols.length; j < len_j; j++) {
                    tableObj[cols[j].id] = respObj[i][cols[j].id];
                    console.log(tableObj);                    
                }
                tableData.push(tableObj);
            }
        table.appendRows(tableData);
        doneCallback();
        }, "json");
    };

    tableau.registerConnector(myConnector);

     // Create event listeners for when the user submits the form
     $(document).ready(function() {
        $("#submitButton").click(function() {
            var cosmosConnectionInfo = {
                account : $("#account-uri").val().trim(),
                key: $("#account-key").val().trim(),
                database: $("#database").val().trim(),
                collection: $("#collection").val().trim(),
                query: $("#custom-query").val().trim(),
            }

            tableau.connectionData = JSON.stringify(cosmosConnectionInfo);
            tableau.connectionName = "Cosmos DB Reader"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
})();
