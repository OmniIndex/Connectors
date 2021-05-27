/**
* This file holds the specific Email Data Source schema and query for teh connector.
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
    .setId('email_to_list')
    .setName('To')
    .setDescription('The Recipients of your emails.')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT); 

   fields.newMetric()
    .setId('email_cc_list')
    .setName('CC')
    .setDescription('Who is on the emails CC list.')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);

     fields.newMetric()
    .setId('email_bcc_list')
    .setName('BCC')
    .setDescription('Who is on the emails BCC list.')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);     
  
  fields.newDimension()
    .setId('email_attachment_list')
    .setName('Attachments')
    .setDescription('File names of attached items') 
    .setType(types.TEXT)
  
   
  return fields;
}

/**
* This function will build the query that is sent to the platform
* @param request
* @return query
*/
function buildQuery(request) {
  let sql = 'SELECT content_id, email_to_list, email_cc_list, email_bcc_list, email_attachment_list FROM content_emails';
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
          case "email_to_list" :
            return row.push(data.email_to_list);
            break;
          case "email_cc_list" :
            return row.push(data.email_cc_list);
            break;
          case "email_bcc_list" :
            return row.push(data.email_bcc_list);
            break;
          case "email_attachment_list" :
            return row.push(data.email_attachment_list);
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