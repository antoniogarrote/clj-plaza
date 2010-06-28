/**
 *
 *  Plaza JS UI library
 *  @author Antonio Garrote
 *  @date   26.06.2010
 *
 */

PlazaUI = {};

/**
 * PlazaUI.Base
 */
PlazaUI.Base = function(){
    var base = {
        build: function() {
            this.__plz_id = "plz_"+PlazaUI._widgetCounter++;
        },

        extend: function() {
            var F = function(){};
            F.prototype = this;
            var w = new F();
            w.build();
            return w;
        }
    };

    return base;
}();

// Counter for the widgets built
PlazaUI._widgetCounter = 0;


/**
 * PlazaUI.EntityWidget
 */
PlazaUI.EntityWidget = function(){
    var w = PlazaUI.Base.extend();

    w.build = function() {
        PlazaUI.Base.build.apply(this,[]);

        this.entity = null;
        this.connectedTo = null;
        this.connections = [];
        this.handlers = {}
    };

    w.updatedAdapter = function(uri, event, data){
        var fh = this.handlers["onUpdate"];
        fh.apply(this,[data]);
    };

    w.destroyedAdapter = function(uri, event, data){
        var fh = this.handlers["onDestroy"];
        fh.apply(this,[data]);
        this.entity = null;
    };

    w.setEntity = function(uriOrEntity) {
        var uri = uriOrEntity;
        var ent = uriOrEntity;

        if(typeof(uriOrEntity) == "string") {
            ent = Plaza.ABox.entitiesRegistry[uriOrEntity];
        } else {
            uri = ent._uri;
        }

        if(this.entity != null) {
            Plaza.ABox.stopObservingEntity(this.entity._uri, Plaza.ABox.EVENTS.UPDATED, this);
            Plaza.ABox.stopObservingEntity(this.entity._uri, Plaza.ABox.EVENTS.DESTROYED, this);
        }

        this.entity = ent
        Plaza.ABox.startObservingEntity(uri, Plaza.ABox.EVENTS.UPDATED, this, this.updatedAdapter);
        Plaza.ABox.startObservingEntity(uri, Plaza.ABox.EVENTS.DESTROYED, this, this.destroyedAdapter);

        this.updatedAdapter(this.entity._uri, Plaza.ABox.EVENTS.UPDATED, this.entity);

        this.disconnect();

        for(var i in this.connections) {
            var c = this.connections[i];
            c.connectionChangeAdapter(this.entity);
        }
    };

    w.connectionChangeAdapter = function(entity) {
        if(this.entity != null) {
            Plaza.ABox.stopObservingEntity(this.entity._uri, Plaza.ABox.EVENTS.UPDATED, this);
            Plaza.ABox.stopObservingEntity(this.entity._uri, Plaza.ABox.EVENTS.DESTROYED, this);
        }

        this.entity = entity;
        this.updatedAdapter(this.entity._uri, Plaza.ABox.EVENTS.UPDATED, this.entity);

        if(entity != null) {
            Plaza.ABox.startObservingEntity(entity._uri, Plaza.ABox.EVENTS.UPDATED, this, this.updatedAdapter);
            Plaza.ABox.startObservingEntity(entity._uri, Plaza.ABox.EVENTS.DESTROYED, this, this.destroyedAdapter);
        }

        for(var i in this.connections) {
            var c = this.connections[i];
            c.connectionChangeAdapter(entity);
        }
    };

    w.connect = function(widget) {
        if(this.entity != null) {
            Plaza.ABox.stopObservingEntity(this.entity._uri, Plaza.ABox.EVENTS.UPDATED, this);
            Plaza.ABox.stopObservingEntity(this.entity._uri, Plaza.ABox.EVENTS.DESTROYED, this);
        }

        this.entity = widget.entity;
        this.updatedAdapter(this.entity);

        if(this.entity != null) {
            Plaza.ABox.startObservingEntity(entity._uri, Plaza.ABox.EVENTS.UPDATED, this, this.updatedAdapter);
            Plaza.ABox.startObservingEntity(entity._uri, Plaza.ABox.EVENTS.DESTROYED, this, this.destroyedAdapter);
        }

        for(var i in this.connections) {
            var c = this.connections[i];
            c.connectionChangeAdapter(entity);
        }

        this.disconnect();

        this.connectedTo = widget;
        widget.connections.push(this);
    };

    w.disconnectConnection = function(otherWidget) {
        var newConnections = [];
        for(var i in this.connections) {
            if(this.connections[i] != otherWidget) {
                newConnections.push(this.connections[i]);
            }
        }
        this.connections = newConnections;
    }

    w.disconnect = function() {
        if(this.connectedTo != null) {
            this.connectedTo.disconnectConnection(this);
        }
    };

    return w;
}();


/**
 * PlazaUI.SpaceWidget
 */
PlazaUI.SpaceWidget = function(){
    var w = PlazaUI.Base.extend();

    w.build = function() {
        PlazaUI.Base.build.apply(this,[]);

        this.space = null;
        this.connectedTo = null;
        this.connections = [];
        this.handlers = {}
    };

    w.createdAdapter = function(spaceId, event, data){
        var fh = this.handlers["onCreate"];
        fh.apply(this,[data]);
    };

    w.updatedAdapter = function(spaceId, event, data){
        var fh = this.handlers["onUpdate"];
        fh.apply(this,[data]);
    };

    w.destroyedAdapter = function(spaceId, event, data){
        var fh = this.handlers["onDestroy"];
        fh.apply(this,[data]);
        this.entity = null;
    };

    w.setSpace = function(idOrSpace) {
        var id = idOrSpace;
        var ent = idOrSpace;

        if(typeof(idOrSpace) == "string") {
            ent = Plaza.ABox.spacesRegistry[idOrSpace];
        } else {
            id = ent.id;
        }

        if(this.space != null) {
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.CREATED, this);
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.UPDATED, this);
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.DESTROYED, this);
        }

        this.space = ent
        Plaza.ABox.startObservingSpace(id, Plaza.ABox.EVENTS.CREATED, this, this.createdAdapter);
        Plaza.ABox.startObservingSpace(id, Plaza.ABox.EVENTS.UPDATED, this, this.updatedAdapter);
        Plaza.ABox.startObservingSpace(id, Plaza.ABox.EVENTS.DESTROYED, this, this.destroyedAdapter);

        if (this.space != null) {
            for(var i in this.space.entities) {
                var e = this.space.entities[i];
                this.createdAdapter(this.space.id, Plaza.ABox.EVENTS.CREATED, e.value);
            }
        }

        this.disconnect();

        for(var i in this.connections) {
            var c = this.connections[i];
            c.connectionChangeAdapter(this.space);
        }
    };

    w.connectionChangeAdapter = function(space) {
        if(this.space != null) {
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.CREATED, this);
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.UPDATED, this);
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.DESTROYED, this);
        }

        if(this.space != null) {
            for(var i in this.space.entities) {
                var e = this.space.entities[i];
                this.destroyedAdapter(space.id, Plaza.ABox.EVENTS.DESTROYED, e.value);
            }
        }

        this.space = space;

        if(space != null) {

            for(var i in this.space.entities) {
                var e = this.space.entities[i];
                this.createdAdapter(this.space.id, Plaza.ABox.EVENTS.CREATED, e.value);
            }

            Plaza.ABox.startObservingSpace(this.space.id, Plaza.ABox.EVENTS.CREATED, this, this.createdAdapter);
            Plaza.ABox.startObservingSpace(this.space.id, Plaza.ABox.EVENTS.UPDATED, this, this.updatedAdapter);
            Plaza.ABox.startObservingSpace(this.space.id, Plaza.ABox.EVENTS.DESTROYED, this, this.destroyedAdapter);
        }

        for(var i in this.connections) {
            var c = this.connections[i];
            c.connectionChangeAdapter(this.space);
        }
    };

    w.connect = function(widget) {
        if(this.space != null) {
            for(var i in this.space.entities) {
                var e = this.space.entities[i];
                this.destroyedAdapter(this.space.id, Plaza.ABox.EVENTS.DESTROYED, e.value);
            }

            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.CREATED, this);
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.UPDATED, this);
            Plaza.ABox.stopObservingSpace(this.space.id, Plaza.ABox.EVENTS.DESTROYED, this);
        }

        this.space = widget.space;
        this.updatedAdapter(this.space);

        if(this.space != null) {
            for(var i in this.space.entities) {
                var e = this.space.entities[i];
                this.createdAdapter(this.space.id, Plaza.ABox.EVENTS.CREATED, e.value);
            }

            Plaza.ABox.startObservingSpace(this.space.id, Plaza.ABox.EVENTS.CREATED, this, this.createdAdapter);
            Plaza.ABox.startObservingSpace(space.id, Plaza.ABox.EVENTS.UPDATED, this, this.updatedAdapter);
            Plaza.ABox.startObservingSpace(space.id, Plaza.ABox.EVENTS.DESTROYED, this, this.destroyedAdapter);
        }

        for(var i in this.connections) {
            var c = this.connections[i];
            c.connectionChangeAdapter(space);
        }

        this.disconnect();

        this.connectedTo = widget;
        widget.connections.push(this);
    };

    w.disconnectConnection = function(otherWidget) {
        var newConnections = [];
        for(var i in this.connections) {
            if(this.connections[i] != otherWidget) {
                newConnections.push(this.connections[i]);
            }
        }
        this.connections = newConnections;
    }

    w.disconnect = function() {
        if(this.connectedTo != null) {
            this.connectedTo.disconnectConnection(this);
        }
    };

    return w;
}();


/**
 * PlazaUI.Widgets namespace
 */
PlazaUI.Widgets = {
    makeClosable: function(widget) {
        widget.find(".widget-header").append('<span style="float: right; margin: -2px -5px 0px 3px; background-color: white; cursor: pointer;" class="remove-widget ui-icon ui-icon-circle-close"></span>');
        widget.find('.remove-widget').bind("click", function(){
            widget.remove();
        });
    }
};

/**
 * PlazaUI.Widgets.EntityDebugger
 */

PlazaUI.Widgets.EntityDebugger = function(){

    var w = PlazaUI.EntityWidget.extend();

    // Element where the widget will be inserted
    w.attachedElement = null;

    w.build = function() {
        PlazaUI.EntityWidget.build.apply(this,[])
        this.handlers = {};
        this.handlers["onUpdate"] = w.onUpdated;
        this.handlers["onDestroy"] = w.onDestroyed;
    },

    // Insertes the widget in the DOM
    w.attach = function() {
        var elem = null;
        if( arguments.length == 0) {
            elem = jQuery("body");
        } else {
            elem = arguments[0];
        };

        this.attachedElement = jQuery("<div class='plaza-widget entity-debugger-widget'>");
        this.attachedElement.append("<div class='entity-debugger-widget-header widget-header'>");
        this.attachedElement.append("<div class='entity-debugger-widget-body'>");

        if(this.entity == null) {
            this.onDestroyed(null);
        } else {
            this.onUpdated(this.entity);
        }

        elem.append(this.attachedElement);

        // UI tweaks
        if(this.entity == null) {
            this.attachedElement.css("width",300)
        } else {
            this.attachedElement.css("width",this.entity._uri.length * 8)
        }

        var attached = this.attachedElement;
        PlazaUI.Widgets.makeClosable(attached);
        attached.resizable();
        this.attachedElement.find(".entity-debugger-widget-header").bind('mousedown',function(event){
            attached.draggable();
        });
        this.attachedElement.find(".entity-debugger-widget-header").bind('mouseup',function(event){
            attached.draggable("destroy");
        });
    };

    w.onUpdated = function(entity) {
        if(this.attachedElement != null) {
            if(entity != null) {
                this.attachedElement.find(".entity-debugger-widget-header").html(entity._uri);
                this.attachedElement.find(".entity-debugger-widget-body").empty();

                for(var p in entity) {
                    var val = "<div class='entity-debugger-widget-prop'><div class='entity-debugger-widget-prop-alias'>" + p + "</div>";
                    var uri = Plaza.TBox.findPropertyUri(p);
                    if(uri != undefined) {
                        val = val + "<div class='entity-debugger-widget-prop-uri'>uri: " + uri + "</div>"
                    }
                    val = val + "<div class='entity-debugger-widget-prop-val'>value: " + entity[p] + "</div></div>"
                    var props = jQuery(val);

                    this.attachedElement.find(".entity-debugger-widget-body").append(props);
                }
            }
        }
    };

    w.onDestroyed = function(entity) {
        if(this.attachedElement != null) {
            this.attachedElement.find(".entity-debugger-widget-header").html();
            this.attachedElement.find(".entity-debugger-widget-body").empty();
        }
    };

    return w;
}();

/**
 * PlazaUI.Widgets.TripleSpacesBrowser
 */
PlazaUI.Widgets.TripleSpacesBrowser = function(){

    var w = PlazaUI.EntityWidget.extend();

    w.build = function() {
        PlazaUI.EntityWidget.build.apply(this,[])
    };

    var makeSpacePanel = function(id, space){
        var spaceId = space.id;
        var spaceSingleResource = space.singleResource;
        var spaceCollectionResource = space.collectionResource;

        var panel = jQuery("<div class='triple-space-browser-panel'><div class='triple-space-browser-panel-header'>" + spaceId + "</div><div class='triple-space-browser-panel-body'></div></div>");

        if(spaceSingleResource != null) {
            var spaceUri = Plaza.Services.servicesRegistry[spaceSingleResource];
            panel.find(".triple-space-browser-panel-body").append(jQuery("<div class='triple-space-browser-resource'><label>single resource:</label><input type='text' value='" + spaceUri + "'></input></div>"));
        }

        if(spaceCollectionResource != null) {
            var spaceUri = Plaza.Services.servicesRegistry[spaceCollectionResource];
            panel.find(".triple-space-browser-panel-body").append(jQuery("<div class='triple-space-browser-resource'><label>collection resource:</label><input type='text' value='" + spaceUri + "'></input></div>"));
        }

        var actions = "<div class='triple-space-actions'><input class='triple-space-show-triples-btn' type='button' value='show triples'></input></div>"
        panel.append(jQuery(actions));
        panel.find(".triple-space-show-triples-btn").bind("click", function() {
            var w = PlazaUI.Widgets.TriplesTable.extend();
            w.attach();
            w.setSpace(spaceId);
        });

        if(spaceCollectionResource != null) {
            var collectionResourceActions = "<div class='collection-triple-space-actions'><div class='overline'><p>collection resource actions</p></div><input type='button' value='load instances'></input></div>"
            panel.append(jQuery(collectionResourceActions));

            panel.find(".collection-triple-space-actions").bind("click",function() {
                Plaza.ABox.loadInstances(spaceId, {});
            });
        }

        return panel;
    };

    // Inserts the widget in the DOM
    w.attach = function() {
        var elem = null;
        if( arguments.length == 0) {
            elem = jQuery("body");
        } else {
            elem = arguments[0];
        };

        var attachedElement = jQuery("<div class='plaza-widget triple-spaces-browser-widget'>");
        attachedElement.append(jQuery("<div class='triple-spaces-browser-header widget-header'>Registered Triple Spaces</div>"));

        for (var i in Plaza.ABox.spacesRegistry) {
            var panel = makeSpacePanel(i,Plaza.ABox.spacesRegistry[i]);
            attachedElement.append(panel);
        }

        elem.append(attachedElement);

        // UI tweaks
        attachedElement.css("width",500)

        PlazaUI.Widgets.makeClosable(attachedElement);
        attachedElement.resizable();
        attachedElement.find(".triple-spaces-browser-header").bind('mousedown',function(event){
            attachedElement.draggable();
        });
        attachedElement.find(".triple-spaces-browser-header").bind('mouseup',function(event){
            attachedElement.draggable("destroy");
        });
    };

    return w;
}();


/**
 * PlazaUI.Widgets.Toolbox
 */

PlazaUI.Widgets.Toolbox = function(){

    var w = PlazaUI.Base.extend();

    w.build = function() {
        PlazaUI.Base.build.apply(this,[])
    };

    var _addFeature = function(container, header, cls, featureBody) {
        var body = container.find(".toolbox-widget-body")

        var elmt = "<h3 class='toolbox-widget-section-header " + cls + "'><a href='#'>" + header + "</a></h3>";
        var panel = jQuery(elmt)

        body.append(panel);

        var elmt = "<div class='toolbox-widget-section-body'></div>"
        var panel = jQuery(elmt);
        panel.append(featureBody);

        body.append(panel);
    };


    var makeSearchTriples = function() {
        var code = "<div><input type='text' class ='entity-search-fld'></input><input class='entitySearchBtn' type='button' value='search'></input></div>"
        var elem = jQuery(code)

        elem.find(".entity-search-fld").autocomplete({source: function(term,callback) {
            var matches = [];
            for(var uri in Plaza.ABox.entitiesRegistry) {
                if(uri.indexOf(term.term) != -1 || term == "") {
                    matches.push(uri);
                }
            }
            callback(matches);
        }});

        elem.find(".entitySearchBtn").bind("click", function(event) {
            var uri = elem.find(".entity-search-fld").val()
            var w = PlazaUI.Widgets.EntityDebugger.extend();
            w.setEntity(Plaza.ABox.findEntityByURI(uri));
            w.attach();
        });
        return elem;
    };

    var makeLoadSchemas = function() {
        var code = "<div><input type='text' class ='schema-uri-fld'></input><input class='schema-load-btn' type='button' value='connect'></input></div>"
        var elem = jQuery(code)

        elem.find(".schema-load-btn").bind("click", function(event) {
            var uri = elem.find(".schema-uri-fld").val();
            if(uri != "") {
                Plaza.TBox.registerSchema(uri, function(loadedUri){
                    var dialog = jQuery("<div title='Schema connection'><p>The schema located at " + loadedUri + " has been loaded successfully</p></div>");
                    jQuery("body").append(dialog);
                    dialog.dialog();
                });
            } else {
                alert("A URI must be provided to load schema information");
            }
        });
        return elem;
    };

    var makeInspectTBox = function() {
        var code = "<div class='inspect-tbox-toolbox-feature'>";
        code = code + "<div><input type='button' class='browse-services-btn' value='Browse registered triple spaces'></input></div>";
        code = code + "</div>";

        var elem = jQuery(code);

        elem.find('.browse-services-btn').bind("click", function(){
            var w = PlazaUI.Widgets.TripleSpacesBrowser.extend();
            w.attach();
        });

        return elem;
    };

    var makeConnectToService = function() {
        var code = "<div class='connection-to-service-toolbox-feature' style='height:120px'><fieldset style='text-align:right'>"
        code = code + "<div><label>Triple Space ID:</label><input type='text' class='ts-id-fld toolbox-legend-fld'></input></div>";
        code = code + "<div><label>Single Service URI:</label><input type='text' class='single-service-fld toolbox-legend-fld'></input></div>"
        code = code + "<div><label>Collection Service URI:</label><input type='text' class='collection-service-fld toolbox-legend-fld'></input></div>"
        code = code + "<input class='service-connect-btn' type='button' value='connect' style='margin-top:10px'></input></fieldset></div>";
        var elem = jQuery(code)

        elem.find(".service-connect-btn").bind("click", function(event) {
            var tsId = elem.find(".ts-id-fld").val();
            var uriSingle = elem.find(".single-service-fld").val();
            var uriCollection = elem.find(".collection-service-fld").val();

            if(tsId == "") {
                alert("A name for the triple space must be provided");
            } else {
                if(uriSingle == "" && uriCollection == "") {
                    alert("URIs for single resource, collection resource or both of them must be provided");
                } else {
                    if(Plaza.ABox.spacesRegistry[tsId] != null) {
                        alert("The name for the triple space is already in use");
                    } else {
                        var params = {}
                        if(uriCollection != "") {
                            params["singleResource"] = uriSingle;
                        }
                        if(uriSingle != "") {
                            params["collectionResource"] = uriCollection;
                        }
                        elem.find(".ts-id-fld").val("")
                        elem.find(".single-service-fld").val("");
                        elem.find(".collection-service-fld").val("");

                        Plaza.ABox.TripleSpace.connect(tsId, params, function() {
                            var dialog = jQuery("<div title='Service connection'><p>The service " + tsId + " has been registered successfully</p></div>");
                            jQuery("body").append(dialog);
                            dialog.dialog();
                        });
                    }
                }
            }
        });
        return elem;
    }

    // Insertes the widget in the DOM
    w.attach = function() {
        var elem = null;
        if( arguments.length == 0) {
            elem = jQuery("body");
        } else {
            elem = arguments[0];
        };

        this.attachedElement = jQuery("<div class='plaza-toolbox-widget plaza-widget'>");
        this.attachedElement.append("<div class='toolbox-widget-header widget-header'>( plaza )  toolbox</div>");
        this.attachedElement.append("<div class='toolbox-widget-body'>");

        _addFeature(this.attachedElement, "Schemas connection", "", makeLoadSchemas());
        _addFeature(this.attachedElement, "Services connection", "", makeConnectToService());
        _addFeature(this.attachedElement, "Search entity", "", makeSearchTriples());
        _addFeature(this.attachedElement, "Inspect tbox", "", makeInspectTBox());

        elem.append(this.attachedElement);
        this.attachedElement.find(".toolbox-widget-body").accordion({collapsible: true, active: false});

        // UI tweaks
        var attached = this.attachedElement;
        PlazaUI.Widgets.makeClosable(attached);
        this.attachedElement.css("width",600)
        this.attachedElement.find(".widget-header").bind('mousedown',function(event){
            attached.draggable();
        });
        this.attachedElement.find(".widget-header").bind('mouseup',function(event){
            attached.draggable("destroy");
        });
    };

    return w;
}();


/**
 * PlazaUI.Widgets.TriplesTable
 */

PlazaUI.Widgets.TriplesTable = function(){

    var w = PlazaUI.SpaceWidget.extend();

    // Element where the widget will be inserted
    w.attachedElement = null;

    w.build = function() {
        PlazaUI.SpaceWidget.build.apply(this,[])
        this.handlers = {};
        this.handlers["onCreate"] = w.onCreated;
        this.handlers["onUpdate"] = w.onUpdated;
        this.handlers["onDestroy"] = w.onDestroyed;
        this.rowsCounter = 0;
        // uri -> id
        this.rowsMap = {};
    };

    w.onCreated = function(data){
        var titleId = "#plaza-triples-table-"+this.__plz_id;
        jQuery(titleId).html(this.space.id);

        if(this.attachedElement != null) {
            var rowClass = "row-" + this.rowsCounter++;
            var subject = data._uri;
            for(var p in data) {
                if(p != "_uri") {
                    var predicate = Plaza.TBox.findPropertyUri(p);
                    if(predicate == null) {
                        predicate = p;
                    }
                    var object = data[p];
                    var txt = "<tr class='" + rowClass + "'><td>" + subject + "</td><td>" + predicate + "</td><td>" + object + "</td></tr>";
                    this.attachedElement.find("tbody").append(jQuery(txt));
                }
            }
        }
    };


    w.onUpdated = function(){
        if(this.attachedElement != null) {
            // @todo
        }
    };

    w.onDestroyed = function() {
        if(this.attachedElement != null) {
            // @todo
        }
    }

    // Insertes the widget in the DOM
    w.attach = function() {
        var elem = null;
        if( arguments.length == 0) {
            elem = jQuery("body");
        } else {
            elem = arguments[0];
        };

        var spaceId = "";
        if(this.space != null) {
            spaceId = this.space.id;
        }

        this.attachedElement = jQuery("<div class='plaza-triples-table-widget plaza-widget'>");
        this.attachedElement.append("<div class='toolbox-widget-header widget-header'><span id='plaza-triples-table-"+this.__plz_id +"'>" + spaceId  + "</span></div>");
        this.attachedElement.append("<div class='plaza-triples-widget-body'><table><thead><th>subject</th><th>predicate</th><th>object</th></thead><tbody></tbody></table></div>");


        elem.append(this.attachedElement);

        if(this.space != null) {
            for(var i in this.space.elements) {
                var elt = this.space.elements[i];

                this.createdAdapter(this.space.id, Plaza.ABox.EVENTS.CREATED, elt.value)
            }
        }

        // UI tweaks
        var attached = this.attachedElement;

        PlazaUI.Widgets.makeClosable(attached);

        this.attachedElement.css("width",1000)
        this.attachedElement.find(".widget-header").bind('mousedown',function(event){
            attached.draggable();
        });
        this.attachedElement.find(".widget-header").bind('mouseup',function(event){
            attached.draggable("destroy");
        });
    };

    return w;
}();
