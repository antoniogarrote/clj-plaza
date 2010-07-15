/*
 *
 *  Plaza JS client library and utilities
 *  @author Antonio Garrote
 *  @date   23.06.2010
 *
 */

Plaza = {

    // Sequences a list of currified functions
    setup: function() {
        var fns = arguments;
        var cdr = [];
        var car = null

        var fnsLength = fns.length;

        for(var i=0; i<fnsLength; i++) {
            if(i==0) {
                car = fns[i];
            } else {
                cdr.push(fns[i]);
            }
        }


        if(cdr.length == 0) {
            car();
        } else {
            car(function() {
                Plaza.setup.apply(this,cdr);
            });
        }
    }

};

/* XSD Datatypes */

Plaza.XSD = {
    DATATYPES: {
        datatype:"http://www.w3.org/2000/01/rdf-schema#Datatype",
        string:"http://www.w3.org/2001/XMLSchema#string",
        boolean:"http://www.w3.org/2001/XMLSchema#boolean",
        decimal:"http://www.w3.org/2001/XMLSchema#decimal",
        float:"http://www.w3.org/2001/XMLSchema#float",
        double:"http://www.w3.org/2001/XMLSchema#double",
        dateTime:"http://www.w3.org/2001/XMLSchema#dateTime",
        time:"http://www.w3.org/2001/XMLSchema#time",
        date:"http://www.w3.org/2001/XMLSchema#date",
        gYearMonth:"http://www.w3.org/2001/XMLSchema#gYearMonth",
        gYear:"http://www.w3.org/2001/XMLSchema#gYear",
        gMonthDay:"http://www.w3.org/2001/XMLSchema#gMonthDay",
        gDay:"http://www.w3.org/2001/XMLSchema#gDay",
        gMonth:"http://www.w3.org/2001/XMLSchema#gMonth",
        hexBinary:"http://www.w3.org/2001/XMLSchema#hexBinary",
        base64Binary:"http://www.w3.org/2001/XMLSchema#base64Binary",
        anyURI:"http://www.w3.org/2001/XMLSchema#anyURI",
        normalizedString:"http://www.w3.org/2001/XMLSchema#normalizedString",
        token:"http://www.w3.org/2001/XMLSchema#token",
        language:"http://www.w3.org/2001/XMLSchema#language",
        NMTOKEN:"http://www.w3.org/2001/XMLSchema#NMTOKEN",
        Name:"http://www.w3.org/2001/XMLSchema#Name",
        NCName:"http://www.w3.org/2001/XMLSchema#NCName",
        integer:"http://www.w3.org/2001/XMLSchema#integer",
        nonPositiveInteger:"http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
        negativeInteger:"http://www.w3.org/2001/XMLSchema#negativeInteger",
        long:"http://www.w3.org/2001/XMLSchema#long",
        int:"http://www.w3.org/2001/XMLSchema#int",
        short:"http://www.w3.org/2001/XMLSchema#short",
        byte:"http://www.w3.org/2001/XMLSchema#byte",
        nonNegativeInteger:"http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
        unsignedLong:"http://www.w3.org/2001/XMLSchema#unsignedLong",
        unsignedInt:"http://www.w3.org/2001/XMLSchema#unsignedInt",
        unsignedShort:"http://www.w3.org/2001/XMLSchema#unsignedShort",
        unsignedByte:"http://www.w3.org/2001/XMLSchema#unsignedByte",
        positiveInteger:"http://www.w3.org/2001/XMLSchema#positiveInteger"
    },

    DATATYPES_PARSERS: {
        datatype: function(obj) { return obj.value },
        string: function(obj) { return obj.value },
        boolean: function(obj) { return eval(obj.value) },
        decimal: function(obj) { return parseInt(obj.value) },
        float: function(obj) { return parseFloat(obj.value)},
        double: function(obj) { return parseInt(obj.value) },
        dateTime: function(obj) { return new Date(obj.value) },
        time: function(obj) { return obj.value },
        date: function(obj) { return new Date(obj.value) },
        gYearMonth: function(obj) { return obj.value },
        gYear: function(obj) {return obj.value } ,
        gMonthDay: function(obj) {return obj.value },
        gDay: function(obj) {return obj.value },
        gMonth: function(obj) {return obj.value },
        hexBinary: function(obj) {return obj.value },
        base64Binary: function(obj) {return obj.value },
        anyURI: function(obj) {return obj.value },
        normalizedString: function(obj) {return obj.value },
        token: function(obj) {return obj.value },
        language: function(obj) {return obj.value },
        NMTOKEN: function(obj) {return obj.value },
        Name: function(obj) {return obj.value },
        NCName: function(obj) {return obj.value },
        integer: function(obj) { return parseInt(obj.value) },
        nonPositiveInteger: function(obj) { return parseInt(obj.value) },
        negativeInteger: function(obj) { return parseInt(obj.value) },
        long: function(obj) { return parseInt(obj.value) },
        int: function(obj) { return parseInt(obj.value) },
        short: function(obj) {return parseInt(obj.value) },
        byte: function(obj) { return parseInt(obj.value) },
        nonNegativeInteger: function(obj) { return parseInt(obj.value) },
        unsignedLong: function(obj) { return parseInt(obj.value) },
        unsignedInt: function(obj) { return parseInt(obj.value) },
        unsignedShort: function(obj) { return parseInt(obj.value) },
        unsignedByte: function(obj) { return parseInt(obj.value) },
        positiveInteger: function(obj) { return parseInt(obj.value) }
    }
};


Plaza.XSD.DATATYPES_INV = {};
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2000/01/rdf-schema#Datatype"] = "datatype";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#string"] = "string";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#boolean"]= "boolean";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#decimal"] = "decimal";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#float"] = "float";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#double"] = "double";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#dateTime"] = "dateTime";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#time"] = "time";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#date"] = "date";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#gYearMonth"] = "gYearMonth";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#gYear"] = "gYear";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#gMonthDay"] = "gMonthDay";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#gDay"] = "gDay";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#gMonth"] = "gMonth";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#hexBinary"] = "hexBinary";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#base64Binary"] = "base64Binary";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#anyURI"] = "anyURI";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#normalizedString"] = "normalizedString";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#token"] = "token";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#language"] = "language";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#NMTOKEN"] = "NMTOKEN";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#Name"] = "Name";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#NCName"] = "NCName";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#integer"] = "integer";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#nonPositiveInteger"] = "nonPositiveInteger";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#negativeInteger"] = "negativeInteger";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#long"] = "long";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#int"] = "int";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#short"] = "short";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#byte"] = "byte";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#nonNegativeInteger"] = "nonNegativeInteger";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#unsignedLong"] = "unsignedLong";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#unsignedInt"] = "unsignedInt";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#unsignedShort"] = "unsignedShort";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#unsignedByte"] = "unsignedByte";
Plaza.XSD.DATATYPES_INV["http://www.w3.org/2001/XMLSchema#positiveInteger" ] = "positiveInteger";

Plaza.XSD.isKnownUriDatatype = function(uri) {
    for(var du in Plaza.XSD.DATATYPES_INV) {
        if(du == uri) {
            return true;
        }
    }
    return false;
}

// Parses a URI datatype string or a Datatype object to the JS value
// {"value":"test","datatype":"http://www.w3.org/2001/XMLSchema#string"} -> "test"
// {"value":"25","datatype":"http://www.w3.org/2001/XMLSchema#int"} -> 25
// "25^^http://www.w3.org/2001/XMLSchema#int"} -> 25
Plaza.XSD.parseType= function(uri) {
    var uriObj = null;

    if(typeof(uri) == "string") {
        var parts = uri.split("^^");
        uriObj = { value: parts[0], datatype: parts[1] };
    } else {
        uriObj = uri;
    }

    var alias = Plaza.XSD.DATATYPES_INV[uriObj.datatype];
    return Plaza.XSD.DATATYPES_PARSERS[alias](uriObj);
}


/* Utils */
Plaza.Utils = {

    /**
     * TypeOf capable of detecting arrays and nulls
     */
    typeOf: function(obj) {
        if ( typeof(obj) == 'object' ) {
            if (obj.length) {
                return 'array';
            } else {
                return 'object';
            }
        } else {
            return typeof(obj);
        }
    },

    /**
     *  Registers a new namespace in the javascript
     *  runtime.
     */
    registerNamespace: function() {
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

    /**
     * Extracts the QLocal part from an URI:
     * - http://test.com/something -> something
     * - http://test.com/something/else -> else
     * - http://test.com/something/else/ -> else
     * - http://test.com/something#more -> more
     */
    extractQLocal: function(uri) {
        if(uri.indexOf("#") != -1) {
            var parts = uri.split("#");
            return parts[parts.length - 1];
        } else {
            var parts = uri.split("/");
            if(parts[parts.length - 1] == "") {
                return parts[parts.length - 2];
            } else {
                return parts[parts.length - 1];
            }
        }
    },

    cleanTypedLiteral: function(typed) {
        var parts = typed.split("^^");
        var content = parts[0];
        var datatype = parts[1];

        if(content[0] == '"' && content[content.length-1] == '"') {
            content = content.substring(1, content.length-1);
        }
        if(datatype[0] == '<' && datatype[datatype.length-1] == '>') {
            datatype = datatype.substring(1, datatype.length-1);
        }

        return {"value": content, "datatype": datatype};
    },

    isoDate: function(date) {
        var year = 1900+date.getYear();
        var month = date.getMonth() + 1;
        var day = date.getDate();

        var isoDate = ""+year;

        isoDate = isoDate + "-";

        if(month < 10) {
            isoDate = isoDate + "0" + month;
        } else {
            isoDate = isoDate + month;
        }

        isoDate = isoDate + "-";

        if(day<10) {
            isoDate = isoDate + "0" + day;
        } else {
            isoDate = isoDate + day;
        }

        return isoDate;
    },

    _letters: function(str) {
        var acum = []
        for(var i in str) {
            acum.push(str[i]);
        }

        return acum;
    },

    _isUpcase: function(letter) {
        if(letter.toUpperCase() != letter) {
            return false;
        } else {
            return true;
        }
    },

    humanize: function(str) {
        var lets = Plaza.Utils._letters(str);
        var acum = [];
        for(var i in lets) {
            if(lets[i] == "-" || lets[i] == "_") {
                if(i != 0) {
                    acum.push(" ")
                }
            } else {
                if(Plaza.Utils._isUpcase(lets[i])) {
                    if(i !=0) {
                        acum.push(" ")
                    }
                    acum.push(lets[i].toLowerCase());
                } else {
                    acum.push(lets[i]);
                }
            }
        }
        return String.concat.apply("",acum);
    },

    includes: function(array, val) {
        for(var i in array) {
            var valArray = array[i];
            if(valArray == val) {
                return true;
            }
        }
        return false;
    },

    keysIncludes: function(map,val) {
        for(var p in map) {
            if(p == val) {
                return true;
            }
        }
        return false;
    }
};

/* TBox */
Plaza.TBox = {

    // List with all the schemas loaded
    schemaList: [],

    // Map URI -> alias
    classesMap: {},

    // Map alias -> URI
    classesRegistry: {},

    // Map URI -> definition
    propertiesMap: {"http://www.w3.org/1999/02/22-rdf-syntax-ns#type": {"uri": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                                                        "domain": ["http://www.w3.org/2000/01/rdf-schema#Resource"],
                                                                        "range": ["http://www.w3.org/2000/01/rdf-schema#Resource"] },

                    "http://plaza.org/vocabularies/restResourceId": {"uri": "http://plaza.org/vocabularies/restResourceId",
                                                                     "domain": ["http://www.w3.org/2000/01/rdf-schema#Resource"],
                                                                     "range": ["http://www.w3.org/2000/01/rdf-schema#Resource"]}},

    // Map alias -> URI
    propertiesRegistry: {"rdf_type": ["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"],
                         "restResourceId": ["http://plaza.org/vocabularies/restResourceId"] },

    // Map URI -> alias
    propertiesInvRegistry: {"http://www.w3.org/1999/02/22-rdf-syntax-ns#type": ["rdf_type"],
                            "http://plaza.org/vocabularies/restResourceId": ["restResourceId"] },

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
    _processSchema: function(clsDef) {
        var uri=clsDef.uri;
        if(uri != null) {
            var alias = Plaza.Utils.extractQLocal(uri);
            Plaza.TBox.classesMap[uri] = alias;

            if(null == Plaza.TBox.classesRegistry[alias]) {
                Plaza.TBox.classesRegistry[alias] = [uri];
            } else {
                if(!Plaza.Utils.includes(Plaza.TBox.classesRegistry[alias], uri)) {
                    Plaza.TBox.classesRegistry[alias].push(uri);
                }
            }
        }
    },

    // Process a Property definition
    _processProperty: function(propDef) {
        var uri= propDef.uri;
        if(uri != null) {
            var alias = Plaza.Utils.extractQLocal(uri);
            if(Plaza.TBox.propertiesMap[uri] == null) {
                Plaza.TBox.propertiesMap[uri] = propDef;
                Plaza.TBox.propertiesMap[uri].domain = [propDef.domain];
                Plaza.TBox.propertiesMap[uri].range = [propDef.range];
                if(Plaza.TBox.propertiesInvRegistry[uri]==null) {
                    Plaza.TBox.propertiesInvRegistry[uri] = [alias];
                } else {
                    Plaza.TBox.propertiesInvRegistry[uri].push(alias);
                }
            } else {
                Plaza.TBox.propertiesMap[uri].domain.push(propDef.domain);
                Plaza.TBox.propertiesMap[uri].range.push(propDef.range);
            }

            if(Plaza.TBox.propertiesRegistry[alias] == null) {
                Plaza.TBox.propertiesRegistry[alias] = [uri];
            } else {
                if(!Plaza.Utils.includes(Plaza.TBox.propertiesRegistry[alias], uri)) {
                    Plaza.TBox.propertiesRegistry[alias].push(uri);
                }
            }
        }
    },

    // Registers a schema using an associated URI
    // * arguments:
    //   - uri : URI of the schema
    //   - callback: Optional callback function that will be notified whe
    //               when the registration is successfull
    // If no callback is provided a currified version of the function will be
    // returned.
    registerSchema: function() {
        var uri = arguments[0];
        var clbk = arguments[1];

        var cont =  function(callback) {
            Plaza.TBox._retrieveSchema(uri, function(schema) {
                if(schema.length > 0) {
                    Plaza.TBox.schemaList.push(uri);
                    for(var i in schema) {
                        if(schema[i].type != null) {
                            Plaza.TBox._processSchema(schema[i]);
                        } else {
                            Plaza.TBox._processProperty(schema[i]);
                        }
                    }
                    callback(uri);
                } else {
                    callback(null);
                }
            });
        };

        if(clbk == null) {
            return cont;
        } else {
            cont(clbk);
        }
    },

    // Finds the alias registered for an URI
    findPropertyAlias: function(uri) {
        return Plaza.TBox.propertiesInvRegistry[uri];
    },

    // Finds the URI registered for an alias
    findPropertyUri: function(alias) {
        var choices = Plaza.TBox.propertiesRegistry[alias];
        for(choice in choices) {
            if(choice.indexOf(alias) == 0) {
                return [choice];
            }
        }
        if(choices == undefined) {
            choices = [undefined];
        }
        return choices;
    }


};

/* ABbox */
Plaza.ABox = {

    // Default events
    EVENTS: {"CREATED": 0, "DESTROYED": 1, "UPDATED": 2},

    // Mapping URI -> Entity
    entitiesRegistry: {},

    // Mapping space -> triples
    spacesRegistry: {},

    // Registers an observer for an entity
    startObservingEntity: function(uri,event,observer,callback) {
        var entity = Plaza.ABox.entitiesRegistry[uri];
        if(entity != null) {
            var observers = entity.observers[event];
            if(observers == null) {
                observers = []
                entity.observers[event] = observers;
            }
            observers.push({"observer": observer, "callback": callback});
        }
    },

    // Unregisters an observer in an entity
    stopObservingEntity: function(uri, event, observer) {
        var entity = Plaza.ABox.entitiesRegistry[uri];
        if(entity != null) {
            var observers = entity.observers[event];
            if(observers != null) {
                var observersMod = []
                for(var i in observers) {
                    var o = observers[i];
                    if(o.observer != observer) {
                        observersMod.push(o)
                    }
                }
                entity.observers = observersMod;
            }
        }
    },

    // Sends a notification to the observers of an event on a an entity
    notifyEntityObservers: function(entityUri, event, data) {
        var entity = Plaza.ABox.entitiesRegistry[entityUri];
        for(var i in entity.observers[event]) {
            var o = entity.observers[event][i];

            o.callback.apply(o.observer,[entity.uri, event, data]);
        }
        Plaza.ABox.notifySpaceObservers(entity.space.id, event, data);
    },

    // Registers an observer for a space
    startObservingSpace: function(spaceId, event, observer, callback) {
        var space = Plaza.ABox.spacesRegistry[spaceId];
        if(space != null) {
            var observers = space.observers[event];
            if(observers == null) {
                observers = []
                space.observers[event] = observers;
            }
            observers.push({"observer": observer, "callback": callback});
        }
    },

    // Unregisters an observer in an space
    stopObservingSpace: function(spaceId, event, observer) {
        var space = Plaza.ABox.spacesRegistry[spaceId];
        if(space != null) {
            var observers = space.observers[event];
            if(observers != null) {
                var observersMod = []
                for(var i in observers) {
                    var o = observers[i];
                    if(o.observer != observer) {
                        observersMod.push(o)
                    }
                }
                space.observers = observersMod;
            }
        }
    },

    // Sends a notification to the observers of an event on a space
    notifySpaceObservers: function(spaceId, event, data) {
        var space = Plaza.ABox.spacesRegistry[spaceId];
        if(space != null) {
            for(var i in space.observers[event]) {
                var o = space.observers[event][i];
                o.callback.apply(o.observer,[space.id, event, data]);
            }
        }
    },

    // Initalizes a new ABox entity setting some meta-data for the entity
    makeEntityMap: function(uri, triples) {
        return { "uri": uri, "dirty": false, "value": triples, "observers": {}, "space": null };
    },

    // Registers a new triple space for handling entitities
    // * spaceId: Identifier of the space
    // * callback: function where life cycle notifications of the entity will be notified
    registerSpace: function(spaceId, callback) {
        if(Plaza.ABox.spacesRegistry[spaceId] != null) {
            throw "Space already registered:" + spaceId;
        }

        Plaza.ABox.spacesRegistry[spaceId] = { "id": spaceId, "entities": [], "callback": callback, "observers":{} };
    },

    // Registers some triples in the ABox
    // * URI: uri of the new Entity
    // * triples: value of the entity
    // * spaceId: identifier of the space managing the entity (e.g. service URI)
    registerEntity: function(uri, triples, spaceId) {
        // Entity creation
        var entity = Plaza.ABox.makeEntityMap(uri, triples);
        entity.space = Plaza.ABox.spacesRegistry[spaceId];

        // Registering the entity
        if(Plaza.ABox.spacesRegistry[spaceId] == null) {
            // @todo What should we do here?
            throw "error, unknown space:"+ spaceId;
        }
        Plaza.ABox.spacesRegistry[spaceId].entities.push(entity);

        if(Plaza.ABox.entitiesRegistry[uri] != null) {
            //@todo What should we do here?
            throw "error, entity already registered:"+ uri;
        }
        Plaza.ABox.entitiesRegistry[uri] = entity;

        // Creation notifications
        Plaza.ABox.notifySpaceObservers(spaceId, Plaza.ABox.EVENTS.CREATED, entity.value);
    },

    // Finds a space provided its identifier
    findSpace: function(spaceId) {
        return Plaza.ABox.spacesRegistry[spaceId];
    },

    // Returns all the entities in a space provided its identifier
    spaceEntities: function(spaceId) {
        var entities = Plaza.ABox.findSpace(spaceId).entities;
        var vals = [];
        for (var i in entities) {
            var entity = entities[i];
            vals.push(entity.value);
        }

        return vals;
    },

    updateEntity: function(uri, triples) {
        var entity = Plaza.ABox.entitiesRegistry[uri];

        if(entity != null) {
            entity.dirty = true;
            entity.value = triples;
            entity.value["_uri"] = uri
            entity.space.callback(Plaza.ABox.EVENTS.UPDATED, uri, entity);

            Plaza.ABox.notifyEntityObservers(uri, Plaza.ABox.EVENTS.UPDATED, entity.value);
        }
    },

    destroyEntity: function(uri) {
        var entity = Plaza.ABox.entitiesRegistry[uri];

        if(entity != null) {
            entity.space.callback(Plaza.ABox.EVENTS.DESTROYED, uri, entity);
            Plaza.ABox.notifyEntityObservers(uri, Plaza.ABox.EVENTS.DESTROYED, entity.value);

            // we remove the entity
            var entities = entity.space.entities;
            var entitiesMod = []
            for(var i in entities) {
                if(entities[i].uri != uri) {
                    entitiesMod.push(entities[i]);
                }
            }
            entity.space.entities = entitiesMod;

            delete Plaza.ABox.entitiesRegistry[uri];
        }
    },

    findEntityByURI: function(uri) {
        return Plaza.ABox.entitiesRegistry[uri].value;
    }
};

/* JSON */
Plaza.JSON = {

    // Builds a JSON object retrieving the alias for the properties from
    // the TBox
    fromTriples: function(triples) {
        var acum = {}
        for(var i in triples) {
            var obj = null;
            var triple = triples[i];
            var subject = triple[0];
            var predicate = triple[1];
            var object = triple[2];
            //@todo why first?
            // We are using this function when retrieving data
            // from some service and transforming it into a JSON
            // object
            // Maybe we can take a look at the service and see
            // what alias does it define for the property URI...
            var alias = Plaza.TBox.findPropertyAlias(predicate)[0];

            if(acum[subject] == null) {
                obj = {"_uri": subject};
                acum[subject] = obj;
            } else {
                obj = acum[subject];
            }

            if(alias != null) {
                if(typeof(object) == "object" || (typeof(object) == "string" && object.indexOf("http://") != 0)) {
                    obj[alias] = Plaza.XSD.parseType(object);
                } else {
                    if(Plaza.Utils.typeOf(obj[alias]) === "array") {
                        obj[alias].push(object);
                    } else if(obj[alias] == null) {
                        obj[alias] = object;
                    } else {
                        var oldVal = obj[alias];
                        obj[alias] = [oldVal];
                        obj[alias].push(object);
                    }
                }
            }
        }

        var objs = [];
        for(var i in acum) {
            objs.push(acum[i])
        }
        return objs;
    }

}

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
    registerService: function(alias, uri, callback) {
        this._retrieveService(uri, function(srv) {
            Plaza.Services.servicesMap[uri] = srv;
            Plaza.Services.servicesRegistry[alias] = uri;
            callback(alias);
        });
    },

    // Retrieves a service from the provided alias
    findByAlias: function(alias) {
        return Plaza.Services.servicesMap[Plaza.Services.servicesRegistry[alias]];
    },

    // Retrieves a service from the provided uri
    findByUri: function(uri) {
        return Plaza.Services.servicesMap[uri];
    },

    // Creates an object with alias -> uri for all the properties in messages
    // of one service operation
    inputMessagesMap: function(alias, method) {
        var service = Plaza.Services.findByAlias(alias);
        if(service == null) {
            throw("Cannot find service to consume for alias: " + alias);
        }

        var operation = null;
        for (var i in service.operations) {
            var op = service.operations[i]
            if(op.method.toLowerCase() == method.toLowerCase()) {
                operation = op;
            }
        }
        if(operation == null) {
            throw("Cannot find operation for service " + alias +" and method " + method);
        }

        var messages = operation.inputMessages;

        var mapping = {};

        for (var i in messages) {
            var msg = messages[i];
            var modelReference = msg.modelReference;
            // @todo here we choosing the first... problem
            var modelReferenceAlias = Plaza.TBox.findPropertyAlias(modelReference)[0];

            if(modelReferenceAlias != null) {
                mapping[modelReferenceAlias] = modelReference
            } else {
                mapping[modelReference] = modelReference
            }
        }

        return mapping;
    },

    // Creates a map with urlReplacements and values extracted from the data
    // passed as arguments
    _computeReplacements: function(data, messages) {
        var mapping = {};

        for (var i in messages) {
            var msg = messages[i];
            var modelReference = msg.modelReference;
            var urlReplacement = Plaza.Utils.cleanTypedLiteral(msg.urlReplacement).value;

            var modelReferenceAliases = Plaza.TBox.findPropertyAlias(modelReference);
            modelReferenceAliases.push(urlReplacement);

            var value = null;
            for(var modelReferenceAlias in modelReferenceAliases) {
                if(value == null) {
                    value = data[modelReferenceAliases[modelReferenceAlias]];
                }
            }
            if(value != null) {
                mapping[urlReplacement] = value;
            }
        }

        return mapping;
    },

    // Provided a map of URL replacements and a URI template, returns
    // a URI and a map of parameters
    _urlAndParameters: function(urlTemplate, replacements) {
        var mapping = {};

        for (var replacement in replacements) {
            var rplc = "{" + replacement + "}";
            var value = replacements[replacement];

            if(urlTemplate.indexOf(rplc) != -1) {
                urlTemplate = urlTemplate.replace(rplc, escape(value));
            } else {
                mapping[replacement] = value;
            }
        }

        return {"url": urlTemplate, "parameters": mapping};
    },

    // Consumes a services
    consume: function(alias, method, data, callback) {
        var service = Plaza.Services.findByAlias(alias);
        if(service == null) {
            throw("Cannot find service to consume for alias: " + alias);
        }

        var operation = null;
        for (var i in service.operations) {
            var op = service.operations[i]
            if(op.method.toLowerCase() == method.toLowerCase()) {
                operation = op;
            }
        }
        if(operation == null) {
            throw("Cannot find operation for service " + alias +" and method " + method);
        }

        var replacements = Plaza.Services._computeReplacements(data, operation.inputMessages);

        var urlAndParameters = Plaza.Services._urlAndParameters(operation.addressTemplate, replacements);
        var url = urlAndParameters.url + ".js3";
        var parameters = urlAndParameters.parameters;

        var realMethod = operation.method;
        if(operation.method.toLowerCase() == "put") {
            parameters["_method"] = "put";
            realMethod = "post";
        } else if(operation.method.toLowerCase() == "delete") {
            realMethod = "get";
            parameters["_method"] = "delete";
        }

        jQuery.ajax({
            url:url,
            type: realMethod.toUpperCase(),
            dataType: 'json',
            data: parameters,
            traditional: true,
            success: function(triples) {
                callback({"alias": alias, "method": method, "triples":triples, "kind":"success"});
            },
            error: function(e) {
                console.log("Error consuming service: " + url + " method: " + method + " parameters: " + parameters + " -> " + e);
                callback({"kind":"error", "error":e });
            }
        });

    }
};


/* Triple Sapces */
Plaza.ABox.TripleSpace = {

    _singleResourceName: function(spaceName) {
        return spaceName + "-single";
    },

    _collectionResourceName: function(spaceName) {
        return spaceName + "-collection";
    },

    //creates a new triple space from a certain number of services
    connect: function() {
        var name = arguments[0];
        var opts = arguments[1];
        var clbk = arguments[2];

        var singleResourceServiceUri = opts.singleResource;
        var collectionResourceServiceUri = opts.collectionResource;

        var toCall = function(callback) {
            // Registration of the Triple Space service
            var tripleSpaceRegistrationFn = function() {

                // Function for provided as callback for managing engities
                Plaza.ABox.registerSpace(name,function(event, uri, entity) {

                    // Updated
                    if(event == Plaza.ABox.EVENTS.UPDATED) {
                        Plaza.Services.consume(Plaza.ABox.TripleSpace._singleResourceName(name), "put", entity.value, function(evt){
                            entity.dirty = false;
                        });
                    }

                    // Destroyed
                    if(event == Plaza.ABox.EVENTS.DESTROYED) {
                        Plaza.Services.consume(Plaza.ABox.TripleSpace._singleResourceName(name), "delete", entity.value, function(evt){});
                    }

                });

                var space = Plaza.ABox.spacesRegistry[name];

                space["singleResource"] = Plaza.ABox.TripleSpace._singleResourceName(name);
                space["collectionResource"] = Plaza.ABox.TripleSpace._collectionResourceName(name);

                callback(name);
            };

            // Registration of services
            if(singleResourceServiceUri != null) {
                Plaza.Services.registerService(Plaza.ABox.TripleSpace._singleResourceName(name), singleResourceServiceUri, function(_alias) {
                    if(collectionResourceServiceUri != null) {
                        Plaza.Services.registerService(Plaza.ABox.TripleSpace._collectionResourceName(name), collectionResourceServiceUri, function(_alias){
                            tripleSpaceRegistrationFn();
                        });
                    } else {
                        tripleSpaceRegistrationFn();
                    }
                });
            } else {
                Plaza.Services.registerService(Plaza.ABox.TripleSpace._collectionResourceName(name), collectionResourceServiceUri, function(_alias){
                    tripleSpaceRegistrationFn();
                });
            }
        };

        if (clbk == null) {
            return toCall;
        } else {
            toCall(clbk);
        }
    }
};

// Load a set of instances
Plaza.ABox.loadInstances = function(){
    var spaceId = arguments[0];
    var data = arguments[1];
    var clbk = arguments[2]; //optional


    var space = Plaza.ABox.spacesRegistry[spaceId];

    var collectionResource = space["collectionResource"];
    if(collectionResource != null) {
        Plaza.Services.consume(collectionResource, "get", data, function(evt){
            if(evt.kind == "error") {
                if(clbk != null) {
                    clbk(evt);
                }
            } else {
                var alias = evt.alias;
                var method = evt.method;
                var triples = evt.triples;
                var uris = [];

                var results = Plaza.JSON.fromTriples(triples);
                for (var i in results) {
                    var entity = results[i];
                    uris.push(entity._uri);
                    var found = Plaza.ABox.entitiesRegistry[entity._uri];
                    if(found != null) {
                        if(found.dirty == false) {
                            Plaza.ABox.updateEntity(entity._uri, entity);
                        } else {
                            for(var p in entity) {
                                if(found.value[p] == null) {
                                    found.value[p] = entity.p
                                }
                            }
                            Plaza.ABox.updateEntity(found._uri, found.value);
                        }
                    } else {
                        Plaza.ABox.registerEntity(entity._uri, entity, spaceId);
                    }
                }
                if(clbk != null) {
                    clbk({"uris":uris, "kind":"success"});
                }
            }
        });
    } else {
        throw "Uknown triple space:" + spaceId;
    }
};

// Load one instance
Plaza.ABox.loadInstance = function(){
    var spaceId = arguments[0];
    var data = arguments[1];
    var clbk = arguments[2]; //optional


    var space = Plaza.ABox.spacesRegistry[spaceId];

    var singleResource = space["singleResource"];
    if(singleResource != null) {
        Plaza.Services.consume(singleResource, "get", data, function(evt){
            if(evt.kind == "error") {
                if(clbk != null) {
                    clbk(evt);
                }
            } else {
                var alias = evt.alias;
                var method = evt.method;
                var triples = evt.triples;
                var uris = [];

                var results = Plaza.JSON.fromTriples(triples);
                for (var i in results) {
                    var entity = results[i];
                    uris.push(entity._uri);
                    var found = Plaza.ABox.entitiesRegistry[entity._uri];
                    if(found != null) {
                        if(found.dirty == false) {
                            Plaza.ABox.updateEntity(entity._uri, entity);
                        } else {
                            for(var p in entity) {
                                if(found.value[p] == null) {
                                    found.value[p] = entity.p
                                }
                            }
                            Plaza.ABox.updateEntity(found._uri, found.value);
                        }
                    } else {
                        Plaza.ABox.registerEntity(entity._uri, entity, spaceId);
                    }
                }
                if(clbk != null) {
                    clbk({"uris":uris, "kind":"success"});
                }
            }
        });
    } else {
        throw "Uknown triple space:" + spaceId;
    }
};

// Creates a new instance
Plaza.ABox.createEntity = function() {
    var spaceId = arguments[0];
    var data = arguments[1];
    var clbk = arguments[2]; //optional

    var space = Plaza.ABox.spacesRegistry[spaceId];

    var collectionResource = space["collectionResource"];
    if(collectionResource != null) {
        Plaza.Services.consume(collectionResource, "post", data, function(evt){
            if(evt.kind == "success") {
                var alias = evt.alias;
                var method = evt.method;
                var triples = evt.triples;

                var results = Plaza.JSON.fromTriples(triples);
                var entity = results[0];

                Plaza.ABox.registerEntity(entity._uri, entity, spaceId);
                if(clbk != null) {
                    clbk({"kind":"success", "uris":[entity._uri]});
                }
            } else {
                if(clbk != null) {
                    clbk(evt);
                }
            }
        });
    } else {
        throw "Uknown triple space:" + spaceId;
    }
}
