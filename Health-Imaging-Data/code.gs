var cc = DataStudioApp.createCommunityConnector();
//This is used only when debugging within the App Script editor.
var DEBUG = false;
//This is used for runtime logging
var RUNTIME_DEBUG = true;

/**
* This is the inbuilt function that will check to see if the user is a valid one and has been authorized
* https://developers.google.com/datastudio/connector/auth#isauthvalid
* @return bool
*/

function isAuthValid() {
  //Here we check whether the user credentials have been authorized against the OmniIndex Platform.
  //If they have then the authorized property will have been set to true.
  var properties = PropertiesService.getScriptProperties();
  var auth = properties.getProperty('dscc.authorized');
  Logger.log (auth);
  if ( auth == 'true' ) {
    return true;
  } else {
    if ( RUNTIME_DEBUG ) {
      Logger.log ('dscc.autorization, returned: ' + auth);
    }
    return false; 
  }
}

/**
* This function is here to set the user creds within Data Studio so that it can get data from the connection.
* It is a built in function. In here we pass teh user supplied creds to teh API server for checking.
* https://developers.google.com/datastudio/connector/auth#setcredentials
* @return errorCode
*/
function setCredentials(request) {
  //Send the user creds to the API to see if they are correct
  var url = request.configParams.server_address;
  if ( url == null ) {
      DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('You must provide a valid OmniIndex API Server.')
      .setText('To access the data, you must provide a valid OmniIndex Health API URL.')
      .throwException();
  } else {
    url +=  + '/selectcsv?';
  }
  if (url.indexOf("http://") == 0 || url.indexOf("https://") == 0) {
      DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('You must provide a valid OmniIndex API Server.')
      .setText('To access the data, you must provide a valid OmniIndex Health API URL.')
      .throwException();
  }
  const isCredentialsValid = false;
  try {
    isCredentialsValid = checkForValidCreds(request.userPass.username,request.userPass.password, url);
  } catch ( e ) {
      DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('Error validating your credentials. The error is: ' + e)
      .setText('There was an error communicating with the service. Try again later, or file an issue if this error persists.')
      .throwException();
  }
  if (!isCredentialsValid) {
    return {
      //Return an error to allow the user to know
      errorCode: "INVALID_CREDENTIALS"
    };
  }
  //Yes yes we should be using getUserProperties. But it does not work. And not just for me!
  PropertiesService.getScriptProperties()
    .setProperty('dscc.username', request.userPass.username)
    .setProperty('dscc.password', request.userPass.password);
  return {
    errorCode: "NONE"
  };
}

/**
* An inbuilt function that sets how Data Studio will handle the user authentication.
* We are using USER.PASS which will create a form asking for usernamd and password
* https://developers.google.com/datastudio/connector/auth#getauthtype
*/
function getAuthType() {
  //I would like to reset with each new load, however, I am unable to do this but
  //will leave this in gere to remind me to keep looking and reading!
  //resetAuth();
  //Tell Data Studio to show a user cred form if the username and passweord are not available
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.USER_PASS)
    .setHelpUrl('https://www.omniindex.io/datastudio')
    .build();
}

/** 
* https://developers.google.com/datastudio/connector/auth#resetauth
*/
function resetAuth() {
  var properties = PropertiesService.getScriptProperties();
  properties.setProperty('dscc.authorized', 'false');
  properties.deleteProperty('dscc.username');
  properties.deleteProperty('dscc.password');
}

/**
* This function will check the passed credentilas with teh API server.
* @param username
* @param password
* @param server
*/
function checkForValidCreds(username, password, server) { 
  //Here we pass the credentials that the user has given us
  //to the OmniIndex API to see if they are valid.
  var url = [
    server,
    'username=',
     username,
    '&password=',
     password
  ];  
  var response = UrlFetchApp.fetch(url.join(''));
  Logger.log(response.getContentText());
  //The server passes back a JSON string with a 'success' set. This is set to true or fail
  var allGoodToGo = JSON.parse(response.getContentText()).success;
  var properties = PropertiesService.getScriptProperties();
  if ( allGoodToGo == 'true' ) {
    properties.setProperty('dscc.authorized', 'true');
    properties.setProperty('dscc.username', username)
    properties.setProperty('dscc.password', password);
    if ( RuntimeDebug ) {
      Logger.log(properties.getProperty('dscc.username'));
      Logger.log(properties.getProperty('dscc.authorized'));
    }
    return true; 
  } else {
    properties.setProperty('dscc.authorized', 'false');
    return false;
  }
}

/**
* https://developers.google.com/datastudio/connector/reference#isadminuser
*/
function isAdminUser() {
  return true;
}

/**
* https://developers.google.com/datastudio/connector/reference#getconfig
*/
function getConfig(request) {

  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('This Connector will retrieve data from your OmniIndex Health Platform. To continue please type in the URL of your OmniIndex API Server.');
  //Used to get the API server URL that the user is connecting too
  config.newTextInput()
      .setId('server_address')
      .setName('OmniIndex API Server')
      .setHelpText('Type in the url of the OmniIndex API Server that you wish to access. (Required)');
  
  config.newTextInput()
      .setId('patient_name')
      .setName('Patient Name')
      .setHelpText('Type in a specific Patient Name (Optional)');
  config.newTextInput()
      .setId('patient_address')
      .setName('Patient Address')
      .setHelpText('Type in a specific Patient Address (Optional - Unless a Patient Name has been given)');
  config.newTextInput()
      .setId('patient_number')
      .setName('Patient Number')
      .setHelpText('Type in a patient number (Optional)');  
  
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
    .setAggregation(aggregations.COUNT);
  
  fields.newDimension()
    .setId('PatientSide')
    .setType(types.TEXT)
  
  fields.newMetric()
    .setId('ScanType')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);   
  
  fields.newMetric()
    .setId('content_id')
    .setType(types.TEXT)
    .setAggregation(aggregations.COUNT);
 
 fields.newDimension()
    .setId('group_id')
    .setType(types.TEXT);  
  
  fields.newDimension()
    .setId('ScanOf')
    .setType(types.TEXT) 
 
  fields.newDimension()
    .setId('created_date')
    .setType(types.YEAR_MONTH_DAY);
  
   fields.newDimension()    
    .setId('authors')
    .setType(types.TEXT);
   
  return fields;
}

/**
* Inbuilt function that Dat Studio uses to get the schema for the currently displayed report fields.
* Point to note - A user may ask for 1 or 2 of the field values to be displayed. In this case Data Studio
* will only get those fields fro the schema.
* https://developers.google.com/datastudio/connector/reference#getschema
* @return schema
*/
function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}

/** 
* This function will make the call to teh API server
* and [ass the results to Data Studio for display
* @param request
* @return schems
* @return rows
*/
function getData(request) {
  var requestedFieldIds;
  if ( !DEBUG ) {
    requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  }
  var requestedFields;
  if ( !DEBUG ) {
    requestedFields = getFields().forIds(requestedFieldIds);
  } else {
    requestedFields = '[{dataType=STRING, name=ScanOf, semantics={semanticType=TEXT, conceptType=DIMENSION}}, {name=Scanner, dataType=STRING, semantics={conceptType=METRIC, semanticType=TEXT, isReaggregatable=true}, defaultAggregationType=SUM}, {name=content_id, semantics={conceptType=DIMENSION, semanticType=TEXT}, dataType=STRING}]'; 
  }
  
  const properties = PropertiesService.getScriptProperties();
  const username = properties.getProperty('dscc.username');
  const password = properties.getProperty('dscc.password');
  if ( RUNTIME_DEBUG ) {
    Logger.log('username: ' + username);
    Logger.log('password: ' + password);
    Logger.log('username: ' + properties.getProperty('dscc.username'));
    Logger.log('password: ' + properties.getProperty('dscc.password'));
  }
  //This is the query that we are passing to the API
  var sql = buildQuery(request);
  // Fetch and parse data from API
  var server = request.configParams.server_address + '/selectcsv?';
  var url = [
    server,
    'username=',
     properties.getProperty('dscc.username'),
    '&password=',
     properties.getProperty('dscc.password'),
    '&query=' + sql
  ];
  var response = UrlFetchApp.fetch(url.join(''));
  if ( RUNTIME_DEBUG ) {
    Logger.log ( response ) ; 
  }
  var records = formatData(response); 
  if ( RUNTIME_DEBUG ) {
    Logger.log ( records ) ; 
  }  
  var stuff;
  try {
    stuff = JSON.parse(records).results;
  } catch ( e ) {
      DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('The results were unable to be parsed by the application. The system error is: ' + e)
      .setText('There was an error parsing the result set. Try again later, or file an issue if this error persists.')
      .throwException();
  }
  if ( RUNTIME_DEBUG ) {
    Logger.log ( stuff ) ; 
  }  
  var rows = responseToRows(requestedFields, stuff);
  //Used if debugging in console
  if ( !DEBUG ) {
    return {
      schema: requestedFields.build(),
      rows: rows
    };
  }
}

/**
* This function will build the query that is sent to the platform
* @param request
* @return query
*/
function buildQuery(request) {
  query = 'SELECT DISTINCT content_id, Scanner,PatientSide,ScanType,group_id,ScanOf,created_date,authors FROM full_imaging';
  var patientName = request.configParams.patient_name;
  var patientAddress = request.configParams.patient_address;
  var patientNumber = request.configParams.patient_number;
  if ( patientName != null ) {
    if (patientAddress == null) {
       query = 'SELECT DISTINCT TOP 0 content_id, Scanner,PatientSide,ScanType,group_id,ScanOf,created_date,authors FROM full_imaging'
    } else {
      //Please note the %25. This is a bug within the API and GoLang that is being fixed.
      patientAddress = patientAddress.replace(/ /g, '%25');
      query += " WHERE indexed_data LIKE '%25" + patientName + "%25' AND indexed_data LIKE '%25" + patientAddress + "%25'";
    }
  } else if ( patientNumber != null ) {
      query += " WHERE indexed_data LIKE '%25" + patientNumber + "%25'";
  }
  if ( RUNTIME_DEBUG) {
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
            if ( RUNTIME_DEBUG ) {
              Logger.log(created_date); 
            }
            created_date = created_date.replace(/-/g, '');
            created_date = created_date.replace(/:/g, '');
            created_date = created_date.replace(/ /g, '');
            if ( RUNTIME_DEBUG ) {
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

/** 
* This function will take teh CSV from teh API server and convert it in to a 
* JSON string of results.
* @param response
* @return results
*/
function formatData(response) {
  var resultSet = '{\n\t"results": [';
  var dataMap = new Map();
  var records = new Map();
  var fieldNames = [];
  var fieldValues = [];
  var hasResults = false;
  var oResults = response.getContentText();
  var found = oResults.indexOf('\n');
  if ( found > 0 ) {
     let headers = oResults.substring(0, found);
     headers += ",";
     oResults = oResults.substring(found + 1);
     oResults += "\n";
     for ( let it = headers.indexOf(","); it >0; it = headers.indexOf(",") ) {
         fieldNames.push(headers.substring(0, it));
         headers = headers.substring(it + 1);
     }
   }  
  var current = -1;
  var isFirst = true;
  var currentRecord = 0;
  for ( var arr = oResults.indexOf('\n'); arr >0; arr = oResults.indexOf('\n') ) { 
    var result = oResults.substring(0, arr +1);
    result += ",";
    oResults = oResults.substring(arr + 1);
    oResults+= ",";
    current = -1;
    for ( let it = result.indexOf(","); it >0; it = result.indexOf(",") ) {
      current++;
      let fieldName = fieldNames[current];
      let fieldValue = result.substring(0, it);
      result = result.substring(it + 1);
      if ( fieldValue == null ) {
        fieldValue = ''   
      }
      fieldValue = fieldValue.replace('\n', '');
      fieldValue = fieldValue.replace('\r', '');
      fieldValues.push(fieldValue);
      dataMap.set(fieldName, fieldValue);
    }
    isFirst = false;
    fieldValues = [];
    records.set(currentRecord, dataMap);
    var numeroUno = true;
    resultSet += '{\n\t';
    if ( dataMap != null ) {
      dataMap.forEach(function(value, key) {
        hasResults = true;
        if ( numeroUno ) {
          resultSet += '\n"' + key + '": "' + value + '"';
          numeroUno = false;
        } else {
          resultSet += ',\n"' + key + '": "' + value + '"';
        }
      });
    }
    resultSet += '\n},';
    dataMap = new Map();
  }//end for loop
  //remove the trailing comma
  if ( hasResults ) {//maybe
    resultSet = resultSet.substring(0, resultSet.length -1);
  }
  resultSet += '\n]}';
  Logger.log('Results: ' + resultSet)
  return resultSet;
}
