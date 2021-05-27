/**
* This file holds the specific Meta Data Source schema and query for the connector.
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
    .setId('authors')
    .setName('Authors / From')
    .setDescription('The authors of your emails.')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT); 

   fields.newMetric()
    .setId('store')
    .setName('Store')
    .setDescription('Where the item is stored (saved to).')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);

     fields.newMetric()
    .setId('file_size')
    .setName('Size')
    .setDescription('The size of the item within the store.')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);     
  
  fields.newDimension()
    .setId('created_date')
    .setName('Sent / Recieved')
    .setDescription('The time the email was sent or received') 
    .setType(types.YEAR_MONTH_DAY_MINUTE)

  fields.newDimension()
    .setId('modified_date')
    .setName('Modified Date')
    .setDescription('The time the item was last modified') 
    .setType(types.YEAR_MONTH_DAY_MINUTE)    
  
  fields.newMetric()
    .setId('name')
    .setName('Name/Subject')
    .setType(types.TEXT)
    .setDescription('The Subject of the email.')
    .setAggregation(aggregations.COUNT);   
   
  return fields;
}

/**
* This function will build the query that is sent to the platform
* @param request
* @return query
*/
function buildQuery(request) {
  let sql = 'SELECT content_id, authors, name, created_date, modified_date, store, file_size FROM content_meta';
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
          case "authors" :
            return row.push(data.authors);
            break;
          case "name" :
            return row.push(data.name);
            break;
          case "content_id" :
            return row.push(data.content_id);
            break;
          case "created_date" :
            return row.push(data.created_date);
            break;
          case "modified_date" :
            return row.push(data.modified_date);
            break;
          case "file_size" :
            return row.push(data.file_size);
            break;
          case "store" :
            return row.push(data.store);
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