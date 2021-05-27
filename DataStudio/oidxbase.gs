/**
* This file holds the conection and data source building code. It can be used unmodified with any OmniIndex API call that outputs a CSV stream.
* It can be used in conjunction with the OmniIndex Code.gs files. Each of which holds specific data source schem and query configurations..
* @Copyright OmniIndex Inc. 2021
* @Author Simon Bain
* sibain@omniindex.io
* April 2021
*/

/**
 * NOTE 
 * THERE MUST BE NO LOGGER OUTPUTS EXCEPT IF AN ERROR ARISES WHEN THE CONNECTOR IS IN DEPLOYMENT.
 * PLEASE MAKE SURE THAT DEBUG AND RUN_TIME_DEBUG ARE SET TO FALSE AND THAT THERE ARE NO HANGOVERS IN THE CODE.
 */

var cc = DataStudioApp.createCommunityConnector();
//This is used only when debugging within the App Script editor. Outside of the UI it will break!
var DEBUG = false;
//This is used for runtime logging. DO NOT USE IN PRODUCTION. Debug testing inside Data Studio only
var RUN_TIME_DEBUG = false;

function sendUserError(message) {
  cc.newUserError()
    .setText(message)
    .throwException();
}

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
    if ( RUN_TIME_DEBUG ) {
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
  var creds = request.pathUserPass;
  var path = creds.path;
  var username = creds.username;
  var password = creds.password;
  
    if ( RUN_TIME_DEBUG ) {
      Logger.log (path + ' ' + username + ' ' + password);
    }  
  
  if ( path == null ) {
      sendUserError('To access the data, you must provide a valid OmniIndex Health API URL.');
  } else {
    if ( RUN_TIME_DEBUG ) {
      Logger.log ('URL: ' + path);
    }  
  }
  var wtf = path.indexOf("https://") 
  if (path.indexOf("https://") != 0) {
        if ( RUN_TIME_DEBUG ) {
      Logger.log ('https/s check failed on: ' + wtf + ' ' + path);
    } 
      sendUserError('To access the data, you must provide a valid OmniIndex Health API URL.');
  }
  var isCredentialsValid = false;
    if ( RUN_TIME_DEBUG ) {
      Logger.log ('HERE: ' + isCredentialsValid);
    }    
  try {
    isCredentialsValid = checkForValidCreds(username,password, path);
  } catch ( e ) {
    if ( RUN_TIME_DEBUG ) {
      Logger.log ('ERROR: ' + e);
    }  
      sendUserError('There was an error communicating with the service. Try again later, or file an issue if this error persists.');
  }
  if (!isCredentialsValid) {
    return {
      //Return an error to allow the user to know
      errorCode: "INVALID_CREDENTIALS"
    };
  } else {
    return {
      errorCode: "NONE"
    };
  }
}

/**
* An inbuilt function that sets how Data Studio will handle the user authentication.
* We are using USER.PASS which will create a form asking for usernamd and password
* https://developers.google.com/datastudio/connector/auth#getauthtype
*/
function getAuthType() {
  //resetAuth();
  //I would like to reset with each new load, however, I am unable to do this but
  //will leave this in gere to remind me to keep looking and reading!
  //resetAuth();
  //Tell Data Studio to show a user cred form if the username and passweord are not available
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.PATH_USER_PASS)
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
  properties.deleteProperty('dscc.path');
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
  if ( RUN_TIME_DEBUG ) {
    Logger.log('username: ' + username);
    Logger.log('password: ' + password);
    Logger.log('Server" ' + server);
  }  
  
  var url = [
    server,
    '/selectcsv?username=',
     username,
    '&password=',
     password,
    '&query=SELECT COUNT(name) FROM content_meta' 
  ];  
  var response = UrlFetchApp.fetch(url.join(''));

  var allGoodToGo = false;
  var properties = PropertiesService.getScriptProperties();
  try{
    let content = response.getContentText();
    if ( content.indexOf('{"Response" : "Fail"') >= 0 ) {
      properties.setProperty('dscc.authorized', 'false');
      return false;
    }
    allGoodToGo = 'true';
  } catch ( e ) {}
  
  if ( allGoodToGo == 'true' ) {
    properties.setProperty('dscc.authorized', 'true');
    properties.setProperty('dscc.username', username)
    properties.setProperty('dscc.password', password);
    properties.setProperty('dscc.path', server);
    if ( RUN_TIME_DEBUG ) {
      Logger.log('username: ' + properties.getProperty('dscc.username'));
      Logger.log('Authorized: ' + properties.getProperty('dscc.authorized'));
      Logger.log('Server" ' + properties.getProperty('dscc.path'));
    }

    return true; 
  } else {
    if ( RUN_TIME_DEBUG ) {
      Logger.log('username: ' + properties.getProperty('dscc.username'));
      Logger.log('Authorized: ' + properties.getProperty('dscc.authorized'));
      Logger.log('Server" ' + properties.getProperty('dscc.path'));
    }    
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
  var path = properties.getProperty('dscc.path');
  if ( RUN_TIME_DEBUG ) {
    Logger.log('username: ' + username);
    Logger.log('password: ' + password);
    Logger.log('path: ' + path);
    Logger.log('username: ' + properties.getProperty('dscc.username'));
    Logger.log('password: ' + properties.getProperty('dscc.password'));
  }
  //This is the query that we are passing to the API
  var sql = buildQuery(request);
  if ( RUN_TIME_DEBUG ) {
    Logger.log ( sql ); 
  }
  // Fetch and parse data from API
  path += '/selectcsv?';
  var url = [
    path,
    'username=',
     properties.getProperty('dscc.username'),
    '&password=',
     properties.getProperty('dscc.password'),
    '&query=' + sql
  ];
  var response = UrlFetchApp.fetch(url.join(''));
   if ( RUN_TIME_DEBUG ) {
    Logger.log ( response ); 
  } 
  
  var records = formatData(response);
   if ( RUN_TIME_DEBUG ) {
    Logger.log ( records ); 
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
        var clean_value = null;
        clean_value = value.replace(/\\n/g, "")
        .replace(/\'/g, "")
        .replace(/\\"/g, "")
        .replace(/\\&/g, "")
        .replace(/\\r/g, "")
        .replace(/\\t/g, "")
        .replace(/\\b/g, "")
        .replace(/\"/g, "")
        .replace(/\\/g, "")
        .replace(/\\f/g, "");

        //Date format - we do this elsewhere also, but somewhere it is missed!
        if ( key == 'created_date' || key =='modified_date') {

          let dateString = new Date(clean_value);
          var formattedDate = Utilities.formatDate(dateString, 'GMT', "YYYYMMDDHH");
          if ( clean_value.indexOf(' ') > 0 ){
            clean_value = clean_value.substring(0, clean_value.indexOf(' '));
          }
          clean_value = clean_value.replace(/-/g, '');
          clean_value = clean_value.replace(/:/g, '');
          clean_value = clean_value.replace(/ /g, '');
          clean_value = formattedDate;
        }

        if ( numeroUno ) {
          resultSet += '\n"' + key + '": "' + clean_value + '"';
          numeroUno = false;
        } else {
          resultSet += ',\n"' + key + '": "' + clean_value + '"';
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
  return resultSet;
}
