/*
 *
 *  Plaza JS client library and utilities
 *  @author Antonio Garrote
 *  @date   23.06.2010
 *
 */

 Plaza = {};

 /* Utils */
 Plaza.Utils = {

     /**
      *  Registers a new namespace in the javascript
      *  runtime.
      */
     registerNamespace = function() {
         var nsPath = "";
         for (var i=0; i<arguments.length; i++) {
             var ns = arguments[i];
             if(nsPath != "") {
                 nsPath = nsPath + ".";
             }
             var nsPath = nsPath + ns;
             try {
                 var res = eval(nsPath);
                 if(res == null) {
                     throw "Non existant path";
                 }
             } catch(e) {
                 eval(nsPath + " = {};");
             }
         }
     },

     // Extracts the QLocal part from an URI:
     // * http://test.com/something -> something
     // * http://test.com/something/else -> else
     // * http://test.com/something#more -> more
     extractQLocal: function(uri) {
         if(uri.indexOf("#") == -1) {
             var parts = uri.split("#");
             return parts[parts.length - 1];
         } else {
             var parts = uri.split("/");
             return parts[parts.length - 1];
         }
     }

 };

 /* TBox */
 Plaza.TBox = {

     // Map URI -> alias
     classesMap: {},

     // Map alias -> URI
     classesRegistry: {},

     // Map URI -> definition
     propertiesMap: {},

     // Map alias -> URI
     propertiesRegistry: {},

     // Retrieves a service in JSON format provided the URI
     _retrieveSchema: function(uri, callback) {
         jQuery.ajax({
             url: uri,
             dataType: 'json',
             success: function(data) {
                 callback(data);
             }
         })
     },

     // Process a Class definition
     _processSchema(clsDef) {
         var uri=clsDef.uri;
         if(uri != null) {

         }
     },

     // Process a Property definition
     _processSchema(propDef) {
         var uri= propDef.uri;
         if(uri != null) {

         }
     },

     // Registers a schema using an associated URI
     registerSchema: function(uri) {
         this._retrieveSchema(uri, function(schema) {
             for(var i in schema) {
                 if(schema[i].type != null) {
                     this._processSchema(schema[i]);
                 } else {
                     this._processProperty(schema[i]);
                 }
             }
         });
     },

 };

 /* Services */
 Plaza.Services = {
     // Map URI -> alias
     servicesMap: {},

     // Map alias -> URI
     servicesRegistry: {},

     // Retrieves a service in JSON format provided the URI
     _retrieveService: function(uri, callback) {
         jQuery.ajax({
             url: uri,
             dataType: 'json',
             success: function(data) {
                 callback(data);
             }
         })
     },

     // Registers a service using an associated URI
     registerService: function(alias, uri) {
         this._retrieveService(uri, function(srv) {
             Plaza.Services.servicesMap[uri] = srv;
             Plaza.Services.servicesRegistry[alias] = uri;
         });
     },

     // Retrieves a service from the provided alias
     findByAlias: function(alias) {
         return Plaza.Services.servicesMap[Plaza.Services.servicesRegistry[alias]];
     },

     // Retrieves a service from the provided uri
     findByUri: function(uri) {
         return Plaza.Services.servicesMap[uri];
     }
 };
