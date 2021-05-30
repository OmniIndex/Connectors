/**
* This file holds the specific Email Data Source schema and query for teh connector.
* It must be used in conjunction with OIDXBase.gs which holds the common (Across OmniIindex Connectors) code base.
* @Copyright OmniIndex Inc. 2021
* @Author Simon Bain
* sibain@omniindex.io
* April 2021
*/

/**Start off with a variable that is used by the getData function. It is here so that we can leave teh oidxbase file untouched, and just modify
* This file with each new connector.
* This array holds the field list that after all queries have been run we will be working with. It must be the same as the ones in the getFields function.
* If a field is not in this list, then it will not be added to the output.
**/
var FieldList = ['content_id', 'authors', 'modified_date', 'name', 'file_size', 'store', 'created_date', 'email_to_list', 'email_cc_list', 'email_bcc_list', 'email_attachment_list', 'classification_type', 'classification_type_2', 'classification_type_3', 'emotion', 'emotion_2', 'type' ];

/**
* https://developers.google.com/datastudio/connector/reference#getconfig
*/
function getConfig(request) {

  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('This Connector will retrieve analysis data from your OmniIndex Platform or SaaS version. To continue please type in the URL of your OmniIndex API Server, username and password.');

  config.setDateRangeRequired(true);
  return config.build();
}

/** 
* Inbuilt function to create the Schema that is displayed within Data Studio to alloww the user to choose which fields to include within their report.
*/
function getFields(request) {
  //I have set all the main fields as newMetric and not newDimension. The reason for this is so that a default count can 
  //be added to the filed. This enables easier pie charts on things like how what % of emails are from Bob
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
    .setName('Authored / From')
    .setDescription('The Author/s of the Content.')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT); 

  fields.newMetric()
    .setId('modified_date')
    .setName('Modified Date')
    .setDescription('Date the content was last mofified.')
    .setType(types.YEAR_MONTH_DAY_MINUTE)
    .setAggregation(aggregations.COUNT); 

  fields.newMetric()
    .setId('name')
    .setName('Subject / File Name')
    .setDescription('The Subject or File Name of the Content.')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);     
  
  fields.newMetric()
    .setId('file_size')
    .setName('Size')
    .setDescription('Content Size') 
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  fields.newMetric()
    .setId('store')
    .setName('Content Store')
    .setDescription('Where the content has been stored (saved to)') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT);

  fields.newMetric()
    .setId('created_date')
    .setName('Received / Created Date')
    .setDescription('Date the content was received and or created.')
    .setType(types.YEAR_MONTH_DAY_MINUTE)
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
  
  fields.newMetric()
    .setId('email_attachment_list')
    .setName('Attachments')
    .setDescription('File names of attached items') 
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);

  fields.newMetric()
    .setId('classification_type')
    .setName('Content Classification')
    .setDescription('Classification of the contents') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT);

  fields.newMetric()
    .setId('classification_type_1')
    .setName('First additional Content Classification 1')
    .setDescription('Classification 1 of the contents') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT);

  fields.newMetric()
    .setId('classification_type_2')
    .setName('Content Classification 2')
    .setDescription('Second additional Classification of the contents') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT);           

  fields.newMetric()
    .setId('classification_type_3')
    .setName('Content Classification 3')
    .setDescription('Third additional Classification of the contents') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT); 

  fields.newMetric()
    .setId('emotion')
    .setName('Main Content Emotion')
    .setDescription('Main emotion of the content') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT);

  fields.newMetric()
    .setId('emotion_2')
    .setName('Second Content Emotion')
    .setDescription('Second emotion of the content') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT);  

  fields.newMetric()
    .setId('type')
    .setName('PII Type')
    .setDescription('Suspected PII Breach type') 
    .setType(types.TEXT)  
    .setAggregation(aggregations.COUNT);             

  return fields;
}

/**
* This function will build the query that is sent to the platform
* @param request
* @return query
*/
function buildQuery(request, count) {
  let sql = null;
  switch (count) {
    case 0: sql = 'SELECT content_id, authors, modified_date, name, file_size, store, created_date FROM content_meta';
    break;  
    case 1: sql = 'SELECT content_id, email_to_list, email_cc_list, email_bcc_list, email_attachment_list FROM content_emails';
    break;
    case 2: sql = 'SELECT content_id, classification_type, classification_type_1, classification_type_2, classification_type_3 FROM content_classifications';
    break;
    case 3: sql = 'SELECT content_id, emotion, emotion_2 FROM emotions';
    break;
    case 4: sql = 'SELECT content_id, type FROM specific_classifications_meta';
    break;               
  }
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
          case "classification_type" :
            return row.push(data.classification_type);
            break; 
          case "classification_type_1" :
            return row.push(data.classification_type_1);
            break; 
          case "classification_type_2" :
            return row.push(data.classification_type_2);
            break; 
          case "classification_type_3" :
            return row.push(data.classification_type_3);
            break;
          case "emotion" :
            return row.push(data.emotion);
            break; 
          case "emotion_2" :
            return row.push(data.emotion_2);
            break;
          case "type" :
            return row.push(data.type);
            break; 
          case "authors" :
            return row.push(data.authors);
            break;
          case "modified_date" :
            return row.push(data.modified_date);
            break;
          case "name" :
            return row.push(data.name);
            break;
          case "file_size" :
            return row.push(data.file_size);
            break; 
          case "store" :
            return row.push(data.store);
            break;
          case "created_date" :
            return row.push(data.created_date);
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