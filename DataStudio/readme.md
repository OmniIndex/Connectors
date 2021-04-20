This folder holds the Google Data Studio Comunnity Connector Code.
The top level holds the OIDXBase.gs file, and the individual connector folders.
Then the child folders hold their own specific code files allong with their appscript.jason configuration file.

eg:
/OIDXBase.gs
/Health-Imaging-data/Code.gs
/Health-Imaging-data/appscript.js

The OIDXBase.gs holds the code that is used by all of teh OmniIndex Connectors. This includes the following function implementations.

function sendUserError(message)
function isAuthValid()
function setCredentials(request)
function getAuthType()
function resetAuth()
function checkForValidCreds(username, password, server)
function isAdminUser()
function getSchema(request)
function getData(request)
function formatData(response)

When Google Scripting engine compilese this and the associated files it creates a single code block. This means that any globals in one are automatically passed in to the others:
var cc = DataStudioApp.createCommunityConnector();
//This is used only when debugging within the App Script editor.
var DEBUG = false;
//This is used for runtime logging
var RUN_TIME_DEBUG = true;
