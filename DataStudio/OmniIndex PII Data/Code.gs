/**
* This file holds the specific PII  Data Source schema and query for the connector.
* It must be used in conjunction with OIDXBase.gs which holds the common (Across OmniIindex Connectors) code base.
* @Copyright OmniIndex Inc. 2021
* @Author Simon Bain
* sibain@omniindex.io
* April 2021
*/

/**
* https://developers.google.com/datastudio/connector/reference#getconfig
*/
function getConfig(request) {

  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('This Connector will retrieve data from your OmniIndex Platform or SaaS version. To continue please type in the URL of your OmniIndex API Server, username and password.');

  config.setDateRangeRequired(true);
  return config.build();
}

/** 
* Inbuilt function to create the Schema that is displayed within Data Studio to alloww the user to choose which fields to include within their report.
*/
function getFields(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields.newMetric()
    .setId('content_id')
    .setName('ID')
    .setDescription('The OmniIndex ID.')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT); 

  fields.newMetric()
    .setId('type')
    .setName('PII Type')
    .setDescription('The type of PII Suspected breach (Email, Telephone, SSN etc.).')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);   
   
  return fields;
}

/**
* This function will build the query that is sent to the platform
* @param request
* @return query
*/
function buildQuery(request) {
  let sql = 'SELECT content_id, type FROM specific_classifications_meta';
  return sql;
}

/**
* This function will take the schema json of the current request and the result set JSON 
* and use them to buils a json array of results.
* @param fields
* @param records
* @return rows
*/
function responseToRows(fields, records) {
  // Transform parsed data and filter for requested fields
  try {
    return records.map (function(data) {
      var row = [];
      fields.asArray().forEach(function (field) {
        switch ( field.getId() )  {
          case "content_id" :
            return row.push(data.content_id);
            break;
          case "type" :
            return row.push(data.type);
            break;
          default: return row.push('');  
        }
      });
      return { values: row };
    });
  } catch ( e ) {
   Logger.log(e); 
  }
}