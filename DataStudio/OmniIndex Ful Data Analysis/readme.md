This folder contains the full meta table queries, made up from 5 seperate sql queries and brought together 
within the oidxbase.gs/getData function. It uses the FieldList array that is set at a global vele within Code.gs.

getData loops through the queries in Code.gs/buildQuery adn builds a single output based on content_d and the list of
fields within Code.gs/FieldList

The oidxbase.gs file within this folder will become the master when teh other 5 connectors are updated.