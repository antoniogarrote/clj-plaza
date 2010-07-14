/**
 *
 *  Plaza JS UI library
 *  @author Antonio Garrote
 *  @date   26.06.2010
 *
 */

PlazaUI = {};

/**
 * PlazaUI.Utils
 */

PlazaUI.Utils = function() {
    return {
        showDialog: function(title, txt) {
            var dialog = jQuery("<div title='"+ title +"'><p>" + txt + "</p></div>");
            jQuery("body").append(dialog);
            dialog.dialog();
        },

        center: function(widget) {
            widget.css("position","absolute");
            var top = ($(window).height() - widget.height() ) / 2+$(window).scrollTop();
            if (top < 0) {
                top = 20;
            }
            widget.css("top", top  + "px");
            widget.css("left", ( $(window).width() - widget.width() ) / 2+$(window).scrollLeft() + "px");
        },

        generateSetup: function() {
            var js = "Plaza.setup(\n";
            js = js + "\t// Schemas\n"
            for (var i in Plaza.TBox.schemaList) {
                js = js + '\tPlaza.TBox.registerSchema("'+Plaza.TBox.schemaList[i]+'"),\n'
            }

            js = js + "\n\t// TripleSpaces\n"
            for(var spaceId in Plaza.ABox.spacesRegistry) {
                var space = Plaza.ABox.spacesRegistry[spaceId];
                var singleResource = Plaza.Services.servicesRegistry[space.singleResource];
                var collectionResource = Plaza.Services.servicesRegistry[space.collectionResource];

                if(singleResource == null && collectionResource != null) {
                    js = js + '\tPlaza.ABox.TripleSpace.connect("'+spaceId+'", {"collectionResource": "'+collectionResource+'"}),\n'
                } else if(singleResource != null && collectionResource == null) {
                    js = js + '\tPlaza.ABox.TripleSpace.connect("'+spaceId+'", {"singleResource": "'+singleResource+'"}),\n'
                } else {
                    js = js + '\tPlaza.ABox.TripleSpace.connect("'+spaceId+'", {"collectionResource": "'+collectionResource+'", "singleResource": "'+singleResource+'"}),\n'
                }
            }

            js = js + "\n\tfunction(){\n\t\t//application code here\n\t})";

            return js;
        }
    };
}();

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
            this.attachedElement.css("width",300);
        } else {
            this.attachedElement.css("width",this.entity._uri.length * 8);
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

        PlazaUI.Utils.center(this.attachedElement);
    };

    w.onUpdated = function(entity) {
        if(this.attachedElement != null) {
            if(entity != null) {
                this.attachedElement.find(".entity-debugger-widget-header").html(entity._uri);
                this.attachedElement.find(".entity-debugger-widget-body").empty();

                for(var p in entity) {
                    var val = "<div class='entity-debugger-widget-prop'><div class='entity-debugger-widget-prop-alias'>" + p + "</div>";
                    var uri = Plaza.TBox.findPropertyUri(p)[0];
                    if(uri != undefined) {
                        val = val + "<div class='entity-debugger-widget-prop-uri'>uri: " + uri + "</div>";
                    }
                    if(Plaza.Utils.typeOf(entity[p]) === "array") {
                        val = val + "<div class='multiple-values' style='overflow:auto'>"
                        for(var i in entity[p]) {
                            val = val + "<div class='entity-debugger-widget-prop-val'>value: " + entity[p][i] + "</div>";
                        }
                        val = val + "</div></div>";
                    } else {
                        val = val + "<div class='entity-debugger-widget-prop-val'>value: " + entity[p] + "</div></div>";
                    }
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
            var collectionResourceActions = "<div class='collection-triple-space-actions'><div class='overline'><p>collection resource actions</p></div><input class='load-instances-action' type='button' value='load instances'></input>"
            collectionResourceActions = collectionResourceActions + "<input class='create-instance-action' type='button' value='create instance'></input></div>";

            panel.append(jQuery(collectionResourceActions));

            panel.find(".load-instances-action").bind("click",function() {
                Plaza.ABox.loadInstances(spaceId, {});
            });

            panel.find(".create-instance-action").bind("click",function() {
                var f = PlazaUI.Widgets.ToolboxEntityCreatorForm.extend();
                f.attach();
                f.wrap(spaceId, "post", "collectionResource", {});
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

        PlazaUI.Utils.center(attachedElement);
    };

    return w;
}();


/**
 * PlazaUI.Widgets.Toolbox
 */

PlazaUI.Widgets.Toolbox = function(){

    var w = PlazaUI.Base.extend();

    w.build = function() {
        PlazaUI.Base.build.apply(this,[]);
    };

    var _addFeature = function(container, header, cls, featureBody) {
        var body = container.find(".toolbox-widget-body");

        var elmt = "<h3 class='toolbox-widget-section-header " + cls + "'><a href='#'>" + header + "</a></h3>";
        var panel = jQuery(elmt);

        body.append(panel);

        var elmt = "<div class='toolbox-widget-section-body'></div>";
        var panel = jQuery(elmt);
        panel.append(featureBody);

        body.append(panel);
    };


    var makeSearchTriples = function() {
        var code = "<div><label>free text:</label><input type='text' class ='entity-search-fld'></input><input class='entity-search-btn' type='button' value='search'></input></div>";

        code = code +  "<div><label>triple space:</label><select name='tspace-selector' class='tspace-selector'>";
        for (var i in Plaza.ABox.spacesRegistry) {
               code = code + "<option value='"+i+"'>"+i+"</option>";
        }
        code = code + "</select><input class='tspace-browse-btn' type='button' value='browse entities'></input></div>";


        var elem = jQuery(code);


        elem.find(".entity-search-fld").autocomplete({source: function(term,callback) {
            var matches = [];
            for(var uri in Plaza.ABox.entitiesRegistry) {
                var entity = Plaza.ABox.entitiesRegistry[uri];
                var found = false;
                for(var p in entity.value) {
                    if(!found && (""+entity.value[p]).indexOf(term.term) != -1) {
                        matches.push(uri);
                        found = true;
                    }
                }
            }
            callback(matches);
        }});

        elem.find(".entity-search-btn").bind("click", function(event) {
            var uri = elem.find(".entity-search-fld").val();
            var w = PlazaUI.Widgets.EntityDebugger.extend();
            w.setEntity(Plaza.ABox.findEntityByURI(uri));
            w.attach();
        });

        elem.find(".tspace-browse-btn").bind("click", function(event) {
            var spaceId = elem.find(".tspace-selector").val();
            var w = PlazaUI.Widgets.EntitiesBrowser.extend();
            w.setSpaceId(spaceId);
            w.attach();
            w.setWidth(600);
            w.setTitle("Entities in triple space"+spaceId);
            w.createPanels();
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

    var makeClassesSelector = function() {
        var txt = "<div class='tbox-classes-selector-group'><label>RDFS Classes</label>"
        txt = txt + "<select name='classes-selector' id='tbox-inspector-classes-selector'>";
        for(var c in Plaza.TBox.classesRegistry) {
            txt = txt + "<option value='"+c+"'>"+c+"</option>";
        }
        txt = txt + "</select><input type='button' value='show'></input></div>";

        return txt;
    };

    var makeInspectTBox = function() {
        var code = "<div class='inspect-tbox-toolbox-feature'>";
        code = code + "<div><input type='button' class='browse-services-btn' value='Browse registered triple spaces'></input></div>";
        code = code + makeClassesSelector();
        code = code + "</div>";

        var elem = jQuery(code);

        elem.find(".tbox-classes-selector-group > input").bind("click", function(){
            var clsAlias = jQuery("#tbox-inspector-classes-selector").val();
            w = PlazaUI.Widgets.RDFSClassBrowser.extend();
            w.setRDFSClass(clsAlias);
            w.attach();
            w.createPanels();
        });

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

    var makeCodeGeneration = function() {
        var code = "<div class='code-generation-toolbox-feature' style='height:120px'><fieldset style='padding:10px'>";
        code = code + "<div><label>Setup code:</label><input type='button' class='setup-code-btn' value='generate'></input></div>";
        code = code +"</div>";

        var elem = jQuery(code)

        elem.find(".setup-code-btn").bind("click", function(event) {
            var code = PlazaUI.Utils.generateSetup();
            var window = jQuery("<div class='plaza-widget'><div class='widget-header plaza-widget-header'>Setup code generation</div><div class='plaza-widget-body'><textarea style='width:100%; height:600px'>"+code+"</textarea></div></div>");
            PlazaUI.Widgets.makeClosable(window);
            window.css("width",800)
            window.css("height",600)
            window.find(".plaza-widget-header").bind('mousedown',function(event){
                window.draggable();
            });
            window.find(".plaza-widget-header").bind('mouseup',function(event){
                window.draggable("destroy");
            });
            jQuery("body").append(window);
        });
        return elem;
    };

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
        _addFeature(this.attachedElement, "Code generation", "", makeCodeGeneration());

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

        PlazaUI.Utils.center(this.attachedElement);
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
                    var predicates = Plaza.TBox.findPropertyUri(p);
                    if(predicates[0] == null) {
                        predicates = [p];
                    }
                    for(var i in predicates){
                        var predicate = predicates[i]
                        var object = data[p];
                        var txt = "<tr class='" + rowClass + "'><td>" + subject + "</td><td>" + predicate + "</td><td>" + object + "</td></tr>";
                        this.attachedElement.find("tbody").append(jQuery(txt));
                    }
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

                this.createdAdapter(this.space.id, Plaza.ABox.EVENTS.CREATED, elt.value);
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

        PlazaUI.Utils.center(this.attachedElement);
    };

    return w;
}();

/* PlazaUI.Widgets.EntityCreatorForm */

PlazaUI.Widgets.EntityCreatorForm = function(){
    var w = PlazaUI.Base.extend();

    w.build = function() {
        PlazaUI.Base.build.apply(this,[]);
        this.spaceId = null;
        this.method = null;
        this.serviceKind = null;
        this.entity = null;
        this.propertyCounter = 0;
        this.currentProperties = {};
        this.attachedElement = null;
    };

    var attributesForSpace = function(spaceId, serviceKind, method) {
        var space = Plaza.ABox.findSpace(spaceId);
        if(space != null) {
            var serviceAlias  = space[serviceKind];
            if(serviceAlias != null) {
                return Plaza.Services.inputMessagesMap(serviceAlias, method);
            } else {
                PlazaUI.Utils.showDialog("Error", "Servive cannot be found for triple space:"+spaceId)
            }
        } else {
            PlazaUI.Utils.showDialog("Error", "Triple space cannot be found with identifier:"+spaceId)
        }
    };

    w.inputTxtForProp = function(prop, entity){
        var inputId = this.propertyCounter++;
        var propId = this.__plz_id+"-"+inputId;
        value = entity[prop];
        if(value == undefined) {
            value = "";
        }

        this.currentProperties[prop] = propId;

        var dt = Plaza.XSD.DATATYPES;
        // @todo a selector for the type of the property must be added
        var range = Plaza.TBox.propertiesMap[Plaza.TBox.propertiesRegistry[prop]].range[0];
        if(range == dt.boolean) {
            var txt = "<select name='"+prop+"' id='"+propId+"'>";
            if(value === true) {
                txt = txt + "<option value='true' selected='true'>true</option>";
            } else {
                txt = txt + "<option value='true'>true</option>";
            }
            if(value === false) {
                txt = txt + "<option value='false' selected='true'>true</option>";
            } else {
                txt = txt + "<option value='false'>true</option>";
            }
            return jQuery(txt + "</select>");
        } else if(range == dt.dateTime || range == dt.date) {
            var txt = jQuery("<input type='text' id='"+propId+"'></input>");
            if (value == "") {
                value = null;
            }
            txt.datepicker({defaultDate: null, dateFormat: "yy-mm-dd"});
            return txt;
        } else {
            if(Plaza.XSD.isKnownUriDatatype(range) || prop == "restResourceId") {
                return jQuery("<input name='"+prop+"' id='"+propId+"' type='text' value='"+value+"'></input>");
            } else {
                var w = PlazaUI.Widgets.MultiEntitiesSelector.extend();
                w.setRDFSClass(range);
                w.setPropertyValues(value);
                w.attach();
                w.attachedElement.find('.entities-selector-list').attr('id', propId);
                return w.attachedElement;
            }
        }
    },

    w.addPropertyToForm = function(formBody, prop, entity) {
        var label = Plaza.Utils.humanize(prop);
        var propClass = prop
        if(prop.indexOf("http://") == 0) {
            label = prop;
            propClass = ""
        }

        var value = entity[prop];
        if(value == null) {
            value = "";
        }
        var propHtml = jQuery("<div class='property'><label class='"+ propClass  +"Label'>"+label+"</label></div>");
        propHtml.append(this.inputTxtForProp(prop,entity));

        formBody.append(propHtml);
    };

    w.retrieveFormData = function(){
        var data = {};

        for(var p in this.currentProperties) {
            var val = this.attachedElement.find("#"+this.currentProperties[p]).val();

            if(val == "") {
                val = null;
            }

            if(val != null) {
                data[p] = val;
            }
        }

        return data;
    };

    w.wrap = function(spaceId, method, serviceKind, entity) {
        this.spaceId = spaceId;
        this.method = method;
        this.serviceKind = serviceKind;
        this.entity = entity;

        var attributes = attributesForSpace(spaceId, serviceKind, method);
        this.currentProperties = {};

        if( this.attachedElement != null) {
            var body = this.attachedElement.find(".entity-creator-form");
            for(var p in attributes) {
                this.addPropertyToForm(body,p,entity);
            }

            var actions = this.attachedElement.find('.entity-creator-actions');
            var that = this;
            if(serviceKind == "collectionResource") {
                var btn = jQuery("<input class='entity-creator-form-btn' type='button' value='save'></input>");
                btn.bind("click",function() {
                    var data = that.retrieveFormData();
                    Plaza.ABox.createEntity(that.spaceId, data);
                });
                actions.append(btn);
            } else if(serviceKind == "singleResource" && method.toLowerCase() == "get") {
                var btn = jQuery("<input class='entity-creator-form-btn' type='button' value='load'></input>");
                actions.append(btn);
            } else if(serviceKind == "singleResource" && method.toLowerCase() == "put") {
                var btn = jQuery("<input class='entity-creator-form-btn' type='button' value='update'></input>");
                btn.bind("click",function() {
                    var data = that.retrieveFormData();
                    Plaza.ABox.updateEntity(entity._uri, data);
                });
                actions.append(btn);
            } else if(serviceKind == "singleResource" && method.toLowerCase() == "delete") {
                var btn = jQuery("<input class='entity-creator-form-btn' type='button' value='destroy'></input>");
                btn.bind("click",function() {
                    var data = that.retrieveFormData();
                    Plaza.ABox.destroyEntity(entity._uri);
                });
                actions.append(btn);
            }

            PlazaUI.Utils.center(this.attachedElement);
        }
    };

    w.attach = function() {
        var elem = null;
        if( arguments.length == 0) {
            elem = jQuery("body");
        } else {
            elem = arguments[0];
        };

        this.attachedElement = jQuery("<div class='plaza-widget entity-creator-widget'>");
        this.attachedElement.append(jQuery("<div class='entity-creator-widget-body'><div class='entity-creator-form'></div><div class='entity-creator-actions'></div></div>"));

        if(this.spaceId != null && this.method != null && this.serviceKind != null && this.entity != null) {
            this.wrap(this.spaceId, this.method, this.serviceKind, this.entity);
        }
        elem.append(this.attachedElement);
        PlazaUI.Utils.center(this.attachedElement);
    };

    return w;
}();


/**
 * PlazaUI.Widgets.ToolboxEntityCreatorForm
 */
PlazaUI.Widgets.ToolboxEntityCreatorForm = function(){
    var w = PlazaUI.Widgets.EntityCreatorForm.extend();

    w.build = function(){
        PlazaUI.Widgets.EntityCreatorForm.build.apply(this, []);
    };

    w.wrap = function() {
        PlazaUI.Widgets.EntityCreatorForm.wrap.apply(this,arguments);
        if(this.spaceId != null && this.method != null && this.serviceKind != null) {
            this.attachedElement.find(".toolbox-entity-creator-form-header").html("Triple space "+ this.spaceId + " " + this.method + " operation");
        }
        PlazaUI.Widgets.makeClosable(this.attachedElement);
        var attachedElement = this.attachedElement;
        this.attachedElement.resizable();
        this.attachedElement.find(".toolbox-entity-creator-form-header").bind('mousedown',function(event){
            attachedElement.draggable();
        });
        this.attachedElement.find(".toolbox-entity-creator-form-header").bind('mouseup',function(event){
            attachedElement.draggable("destroy");
        });
        PlazaUI.Utils.center(this.attachedElement);
    };

    w.attach = function() {
        PlazaUI.Widgets.EntityCreatorForm.attach.apply(this,[]);
        var body = this.attachedElement;
        body.addClass("toolbox-entity-creator-widget");
        body.find(".entity-creator-widget-body").addClass("toolbox-entity-creator-widget-body");
        body.prepend(jQuery("<div class='toolbox-entity-creator-form-header widget-header'></div>"));

        if(this.spaceId != null && this.method != null && this.serviceKind != null) {
            this.attachedElement.find(".toolbox-entity-creator-form-header").html("Triple space "+ this.spaceId + " " + this.method + " operation");
        }

        PlazaUI.Widgets.makeClosable(this.attachedElement);
        var attachedElement = this.attachedElement;
        this.attachedElement.resizable();
        this.attachedElement.find(".toolbox-entity-creator-form-header").bind('mousedown',function(event){
            attachedElement.draggable();
        });
        this.attachedElement.find(".toolbox-entity-creator-form-header").bind('mouseup',function(event){
            attachedElement.draggable("destroy");
        });
        PlazaUI.Utils.center(this.attachedElement);
    };

    return w;
}();

/**
 * PlazaUI.Widgets.PanelListFrame
 */
PlazaUI.Widgets.PanelListFrame = function(){

    var w = PlazaUI.Base.extend();

    w.build = function() {
        PlazaUI.Base.build.apply(this,[]);
        this.panels = [];
        this.attachedElement = null;
    };

    w.appendPanels = function() {
        for(var p in this.panels) {
            this.attachedElement.find(".panel-list-body").append(this.panels[p]);
        }
    };

    // Inserts the widget in the DOM
    w.attach = function() {
        var elem = null;
        if( arguments.length == 0) {
            elem = jQuery("body");
        } else {
            elem = arguments[0];
        };

        var attachedElement = jQuery("<div class='plaza-widget panel-list-widget'>");
        attachedElement.append(jQuery("<div class='panel-list-header widget-header'>Panel List</div>"));
        attachedElement.append(jQuery("<div class='panel-list-body widget-body'></div>"));

        elem.append(attachedElement);

        // UI tweaks
        attachedElement.css("width",500)

        PlazaUI.Widgets.makeClosable(attachedElement);
        attachedElement.resizable();
        attachedElement.find(".panel-list-header").bind('mousedown',function(event){
            attachedElement.draggable();
        });
        attachedElement.find(".panel-list-header").bind('mouseup',function(event){
            attachedElement.draggable("destroy");
        });

        this.attachedElement = attachedElement;
        this.attachedElement.find(".panel-list-body").css("overflow","auto");
        PlazaUI.Utils.center(this.attachedElement);
    };

    // @todo refactor to some frame parent prototype
    w.setWidth = function(width) {
        this.attachedElement.css('width', width);
    };

    w.setHeight = function(height) {
        this.attachedElement.find(".panel-list-body").css('height', height);
    };

    w.setTitle = function(title) {
        this.attachedElement.find(".panel-list-header").html(title);
    };

    w.addWidgetClass = function(cls)  {
        this.attachedElement.find(".panel-list-widget").addClass(cls);
    };

    w.addWidgetHeaderClass = function(cls)  {
        this.attachedElement.find(".panel-list-header").addClass(cls);
    };

    return w;
}();


/**
 * PlazaUI.Widgets.RDFSClassBrowser
 */
PlazaUI.Widgets.RDFSClassBrowser = function() {
    w = PlazaUI.Widgets.PanelListFrame.extend();

    w.build = function(){
        PlazaUI.Widgets.PanelListFrame.build.apply(this,[]);
        this.rdfsClass = null;
    };

    w.setRDFSClass = function(clsAlias) {
        this.rdfsClass = clsAlias;
    };

    var classDescriptionPanel = function(classAlias) {
        var panel = jQuery("<div class='panel-list-panel'><div class='panel-list-panel-header'>Class URI</div><div class='class-uri-panel-body panel-list-panel-body'>"+Plaza.TBox.classesRegistry[classAlias]+"</div></div>");
        return panel;
    };

    var makePropertyPanel = function(propertyAlias){
        var uri = Plaza.TBox.propertiesRegistry[propertyAlias];
        var def = Plaza.TBox.propertiesMap[uri];
        var domains = def.domain;
        var ranges  = def.range;

        var panelBody = "<div><label>URI<label><input type='text' value='"+uri+"'></input></div>";
        for(var domain in domains) {
            panelBody = panelBody + "<div><label>domain<label><input type='text' value='"+domain+"'></input></div>";
        }
        for(var range in rages) {
            panelBody = panelBody + "<div><label>range<label><input type='text' value='"+range+"'></input></div>";
        }
        var panel = jQuery("<div class='panel-list-panel'><div class='panel-list-panel-header'>Property "+propertyAlias+"</div><div class='panel-list-panel-body'>"+panelBody+"</div></div>");
        return panel;
    };

    w.createPanels = function() {
        var clsUri = Plaza.TBox.classesRegistry[this.rdfsClass];

        this.panels = [classDescriptionPanel(this.rdfsClass)];

        for(var p in Plaza.TBox.propertiesRegistry) {
            var pUri = Plaza.TBox.propertiesRegistry[p];
            var pDef = Plaza.TBox.propertiesMap[pUri];

            if(Plaza.Utils.includes(pDef.domain, clsUri)) {
                var panel = makePropertyPanel(p);
                this.panels.push(panel);
            }
        }

        this.appendPanels();
        this.addWidgetHeaderClass("tbox-class-panel-header");
        this.addWidgetClass("tbox-class-panel-body");
        this.setTitle("Class "+clsUri);
        PlazaUI.Widgets.makeClosable(this.attachedElement);
        this.setWidth(600);
        this.setHeight(500);
    };

    return w;
}();



/**
 * PlazaUI.Widgets.EntitiesBrowser
 */
PlazaUI.Widgets.EntitiesBrowser = function() {
    w = PlazaUI.Widgets.PanelListFrame.extend();

    w.build = function(){
        PlazaUI.Widgets.PanelListFrame.build.apply(this,[]);
        this.spaceId = null;
    };

    w.setSpaceId = function(spaceId) {
        this.spaceId = spaceId;
    };

    var makeEntityPanel = function(entity){

        var panelBody = "<div><label> URI<label><input type='text' value='"+entity.uri+"'></input></div>";
        var panelBody = "<div class='entity-actions'><input class='entity-show' type='button' value='show'><input class='entity-edit' type='button' value='edit'></input><input class='entity-delete' type='button' value='destroy'></input></div>";
        var panel = jQuery("<div class='panel-list-panel'><div class='panel-list-panel-header'>Entity "+entity.uri+"</div><div class='panel-list-panel-body'>"+panelBody+"</div></div>");

        panel.find(".entity-show").bind("click", function(){
            var w = PlazaUI.Widgets.EntityDebugger.extend();
            w.setEntity(Plaza.ABox.findEntityByURI(entity.uri));
            w.attach();
        });

        panel.find(".entity-edit").bind("click", function(){
            var f = PlazaUI.Widgets.ToolboxEntityCreatorForm.extend();
            f.attach();
            f.wrap(entity.space.id, "put", "singleResource", entity.value);
        });

        panel.find(".entity-delete").bind("click", function(){
            Plaza.ABox.destroyEntity(entity.uri);
            PlazaUI.Utils.showDialog("Notice", "Trying to destroy triples for entity:"+entity.uri+"\n Open this panel again afterwards.")
        });

        return panel;
    };

    w.createPanels = function() {
        var space = Plaza.ABox.spacesRegistry[this.spaceId];


        this.panels = [];

        for(var i in space.entities) {
            var entity = space.entities[i];
            var panel = makeEntityPanel(entity);
            this.panels.push(panel);
        }

        this.appendPanels();
        this.addWidgetHeaderClass("tbox-class-panel-header");
        this.addWidgetClass("tbox-class-panel-body");
        this.setTitle("Entities in triple space "+this.spaceId);
        PlazaUI.Widgets.makeClosable(this.attachedElement);
        this.setWidth(600);
        this.setHeight(this.panels.count * 50);
    };

    return w;
}();

/**
 * PlazaUI.Widgets.MultiEntitiesSelector
 */
PlazaUI.Widgets.MultiEntitiesSelector = function(){

    var w = PlazaUI.Base.extend();

    w.build = function() {
        PlazaUI.Base.build.apply(this,[]);
        this.RDFSclass = null;
        this.attachedElement = null;
        this.propertyValues = null;
    };

    w.setRDFSClass = function(cls) {
        this.RDFSclass = cls;
        if(this.attachedElement!=null) {
            this.addOptions();
        }
    };

    w.setPropertyValues = function(values) {
        this.propertyValues = values;
    };

    w.addOptions = function() {
        var select = this.attachedElement.find('.entities-selector-list');
        var selected = [];
        for(var uri in Plaza.ABox.entitiesRegistry) {
            var entity = Plaza.ABox.entitiesRegistry[uri];
            var val = entity.value;
            var match = false;
            for(var p in val) {
                if(val[p] == this.RDFSclass) {
                    match = true;
                    break;
                }
            }
            if(match == true) {
                selected.push(entity.uri);
            }
        }

        for(var i in selected) {
            var uri = selected[i];
            if(this.propertyValues != null) {
                if(Plaza.Utils.includes(this.propertyValues, uri)) {
                    select.append(jQuery("<option value='"+uri+"' selected='true'>"+uri+"</option>"));
                } else {
                    select.append(jQuery("<option value='"+uri+"'>"+uri+"</option>"));
                }
            } else {
                select.append(jQuery("<option value='"+uri+"'>"+uri+"</option>"));
            }
        }
    };

    // Inserts the widget in the DOM
    w.attach = function() {
        var attachedElement = jQuery("<span class='multi-entities-selector-widget'>");
        attachedElement.append(jQuery("<span class='selector'><span class='entities-selector-title'><select class='entities-selector-list' multiple='multiple'></select></span></span>"));


        // UI tweaks
        this.attachedElement = attachedElement;
        PlazaUI.Utils.center(this.attachedElement);

        if(this.RDFSclass != null) {
            this.addOptions();
        }
    };

    w.setSelectId = function(value){
        this.attachedElement.find(".entities-selector-list").attr("id", value);
    };

    // @todo refactor to some frame parent prototype
    w.setWidth = function(width) {
        this.attachedElement.css('width', width);
    };

    w.setHeight = function(height) {
        this.attachedElement.css('height', height);
    };

    return w;
}();
