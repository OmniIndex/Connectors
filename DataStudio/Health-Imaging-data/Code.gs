/**
* This file holds the specific Health Data Source schema and query for teh connector.
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
    .setText('This Connector will retrieve data from your OmniIndex Health Platform. To continue please type in the URL of your OmniIndex API Server.');
  config.newTextInput()
      .setId('patient_name')
      .setName('Patient Name')
      .setPlaceholder('Patient Name (Optional)')
      .setIsDynamic(true)
      .setHelpText('Type in a specific Patient Name (Optional)');
  config.newTextInput()
      .setId('patient_address')
      .setName('Patient Address')
      .setPlaceholder('Patient Address (Optional - Used with Patient Name)')
      .setIsDynamic(true)
      .setHelpText('Type in a specific Patient Address (Optional - Used with Patient Name)');
  config.newTextInput()
      .setId('patient_number')
      .setName('Patient Number')
      .setPlaceholder('Patient Number - (Optional)')
      .setIsDynamic(true)
      .setHelpText('Type in a patient number (Optional - Not required if Patien name and address have been given)');  
  
  config.setDateRangeRequired(true);
  return config.build();
}

/** 
* Inbilt function to create the Schema that is displayed within Data Studio to alloww the user to choose which fields to include within their report.
*/
function getFields(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  fields.newMetric()
    .setId('Scanner')
    .setType(types.TEXT)
    .setDescription('The Scanner name. This is also set as a Count field for metric data requirements.')
    .setAggregation(aggregations.COUNT);
  
  fields.newDimension()
    .setId('PatientSide')
    .setDescription('Side of the item that has been imaged (left/right)') 
    .setType(types.TEXT)
  
  fields.newMetric()
    .setId('ScanType')
    .setType(types.TEXT)
    .setDescription('The Scan Type name. This is also set as a Count field for metric data requirements.')
    .setAggregation(aggregations.COUNT);   
  
  fields.newMetric()
    .setId('content_id')
    .setType(types.TEXT)
    .setDescription('The ID of the content within the system. This is also set as a Count field for metric data requirements.')
    .setAggregation(aggregations.COUNT);
 
 fields.newDimension()
    .setId('group_id')
    .setDescription('The Group ID that this item belongs too.')
    .setType(types.TEXT);  
  
  fields.newDimension()
    .setId('ScanOf')
    .setDescription('The item that has been scanned.')
    .setType(types.TEXT) 
 
  fields.newDimension()
    .setId('created_date')
    .setDescription('The date teh scan was taken.')
    .setType(types.YEAR_MONTH_DAY);
  
   fields.newDimension()    
    .setId('authors')
    .setDescription('The person who took the scan.')
    .setType(types.TEXT);
   
  return fields;
}

/**
* This function will build the query that is sent to the platform
* @param request
* @return query
*/
function buildQuery(request) {
  query = 'SELECT DISTINCT content_id, Scanner,PatientSide,ScanType,group_id,ScanOf,created_date,authors FROM full_imaging';
  var configParams = request.configParams;
  try {
    if ( configParams.patient_name != undefined ) {
      query +=  " WHERE indexed_data LIKE '%25" + patientName + "%25'";
      if ( configParams.patient_address === undefined ) {
        sendUserError('Please provide a patient address');
      } else {
        query += " AND index_data LIKE '%25" + configParams.patient_address + "%25'";
      }
    }
  } catch ( e ) {
    //sendUserError('Error in Connector. The error message is: ' + e ); 
  }
  if ( RUN_TIME_DEBUG) {
    Logger.log(query); 
  }
  return query;
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
          case "Scanner" :
            return row.push(data.Scanner);
            break;
          case "PatientSide" :
            return row.push(data.PatientSide);
            break;
          case "ScanType" :
            return row.push(data.ScanType);
            break;
          case "content_id" :
            return row.push(data.content_id);
            break;
          case "group_id" :
            return row.push(data.group_id);
            break;
          case "ScanOf" :
            return row.push(data.ScanOf);
            break;
          case "created_date" :
            //We will normalize this
            let created_date = data.created_date;
            if ( RUN_TIME_DEBUG ) {
              Logger.log(created_date); 
            }
            created_date = created_date.replace(/-/g, '');
            created_date = created_date.replace(/:/g, '');
            created_date = created_date.replace(/ /g, '');
            if ( RUN_TIME_DEBUG ) {
              Logger.log(created_date); 
            }            
            return row.push(created_date);
            break;
          case "authors" :
            return row.push(data.authors);
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

