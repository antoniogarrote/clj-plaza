Scrummy = {
    colors: ["#adc2eb", "#b2e0e0", "#e0c2ff", "#ccffff", "#ffffb2", "#ffb2b2", "#f5ebcc", "#e0b2c2", "#ffebeb", "#d1c2b2", "#d1d1e0", "#fdfdfe", "#c2ffc2", "#c2c2ad"],

    domain: "http://localhost:8081/",

    id: null,

    project: null,

    currentSprint: null,

    sprints: null,

    maxPriority: 0,

    storiesMap: {},

    arrayForColors: "abcedefghijklmnopqrstuvwkyz01234567890",

    center: function(widget) {
        widget.css("position","absolute");
        widget.css("top", ( $(window).height() - widget.height() ) / 2+$(window).scrollTop() + "px");
        widget.css("left", ( $(window).width() - widget.width() ) / 2+$(window).scrollLeft() + "px");
    },

    chooseColor: function(string) {
        var acum = 0;
        for(var i=0; i<string.length; i++) {
            var c = string[i];
            acum = acum + Scrummy.arrayForColors.indexOf(c);
        }
        var sel = (acum * 100) % Scrummy.colors.length;
        if(sel < 0) {
            sel = -sel;
        }

        return Scrummy.colors[sel];
    },

    parseId: function() {
        this.id =  window.location.pathname.split("/")[1];
    },


    loadSprints: function(clbk) {
        Plaza.ABox.loadInstances("sprints", {"belongsToProject": Scrummy.project._uri}, function(evt){
            if(evt.kind == "error") {
                PlazaUI.Utils.showDialog("error", "Sprints for  project "+Scrummy.id+" could not be loaded");
            } else {
                console.log("Loaded "+evt.uris.length+" sprints for project "+Scrummy.id);
                var sprints = Plaza.ABox.findSpace("sprints").entities;
                if(sprints.length == 0) {
                    console.log("No sprint, creating a new sprint");
                    // No sprints yet, I create the first sprint
                    Plaza.ABox.createEntity("sprints", {"created": Plaza.Utils.isoDate(new Date()), "belongsToProject": Scrummy.project._uri}, function(evt){
                        if(evt.kind=="success") {
                            console.log("Sprint 0 created successfully");
                            var currentSprint = Plaza.ABox.findEntityByURI(evt.uris[0]);
                            currentSprint["current"] = true;
                            Scrummy.currentSprint = currentSprint;
                            Scrummy.sprints = [currentSprint];
                            clbk(Scrummy.currentSprint);
                        } else {
                            PlazaUI.Utils.showDialog("error", "The initial sprint could not be created");
                        }
                    });

                } else {
                    // I set up the sprints and choose the current sprint
                    acum = [];
                    var older = sprints[0];
                    for(var i in sprints) {
                        acum.push(sprints[i].value);
                        if (sprints[i].value.created > older.created) {
                            older = sprints[i].value;
                        }
                    }

                    Scrummy.currentSprint = older.value;
                    Scrummy.currentSprint["current"] = true;
                    Scrummy.sprints = acum;
                    clbk(Scrummy.currentSprint);
                }
            }
        });
    },

    createTask: function() {
        var id = jQuery("#new-task-story-id").val();
        var contributor = jQuery("#new-task-contributor").val();
        var body = jQuery("#new-task-body").val();

        var storyUri = Scrummy.storiesMap[""+id];

        Plaza.ABox.createEntity("tasks", {"belongsToStory": storyUri,
                                          "title": body,
                                          "contributor": contributor,
                                          "status": "todo"});

        jQuery("#new-task-form").toggle();
        return false;
    },

    editTask: function() {
        var taskUri = jQuery("#edit-task-id").val();
        var task = Plaza.ABox.findEntityByURI(taskUri);

        var contributor = jQuery("#edit-task-contributor").val();
        var body = jQuery("#edit-task-body").val();

        task["contributor"] = contributor;
        task["title"] = body;

        Plaza.ABox.updateEntity(task._uri, task);

        var color = Scrummy.chooseColor(task.contributor);
        var panel = jQuery("#"+ task.restResourceId);
        panel.find(".task-panel-body").text(task.title);
        panel.find(".task-panel-contributor").text(task.contributor);
        panel.css("background-color",color);
        jQuery("#edit-task-form").toggle();
        return false;
    },

    makeStoryRow: function(story) {
        var id = story.priority;
        if(Scrummy.maxPriority <= story.priority) {
            Scrummy.maxPriority = story.priority + 1;
        }
        var txt = "<tr id='"+id+"'></td><td class='story-title'><div class='story-title-wrapper'>"+story.title+"<div class='story-close ui-icon ui-icon-closethick' style='display:none'></div></div></td><td class='story-td-actions'><a href='#' class='add-task-link'>+</a></td><td id='todo_"+id+"' class='story-todo'></td><td id='progress_"+id+"' class='story-progress'></td><td id='done_"+id+"' class='story-done'></td></tr>";

        Scrummy.storiesMap[""+id] = story._uri;

        var row = jQuery(txt);
        row.find(".story-title").bind("mouseover", function(){
            row.find(".story-close").show();
            row.find(".story-title-wrapper").css("border-width", "1px");
        });
        row.find(".story-title").bind("mouseout", function(){
            row.find(".story-close").hide();
            row.find(".story-title-wrapper").css("border-width", "0px");
        });
        row.find(".story-close").bind('click', function(){
            Plaza.ABox.destroyEntity(story._uri);
            row.remove();
        });
        row.find("td#todo_"+id).sortable({containment: "tr#"+id,
                                          connectWith: ["td#progress_"+id, "td#done_"+id]});
        row.find("td#progress_"+id).sortable({containment: "tr#"+id,
                                              connectWith: ["td#todo_"+id, "td#done_"+id]});
        row.find("td#done_"+id).sortable({containment: "tr#"+id,
                                          connectWith: ["td#progress_"+id, "td#todo_"+id]});

        row.find(".story-todo").droppable({drop:function(event, ui) {
            var storyPanel = ui.draggable;
            var storyUri = ui.draggable.find(".task-uri").val();
            var story = Plaza.ABox.findEntityByURI(storyUri);
            if(story.status != "todo") {
                story.status = "todo";
                Plaza.ABox.updateEntity(story._uri, story);
                var storyClone = storyPanel.clone(true);
            }
        }});
        row.find(".story-progress").droppable({drop:function(event, ui) {
            var storyPanel = ui.draggable;
            var storyUri = ui.draggable.find(".task-uri").val();
            var story = Plaza.ABox.findEntityByURI(storyUri);
            if(story.status != "progress") {
                story.status = "progress";
                Plaza.ABox.updateEntity(story._uri, story);
            }
        }});
        row.find(".story-done").droppable({drop:function(event, ui) {
            var storyPanel = ui.draggable;
            var storyUri = ui.draggable.find(".task-uri").val();
            var story = Plaza.ABox.findEntityByURI(storyUri);
            if(story.status != "done") {
                story.status = "done";
                Plaza.ABox.updateEntity(story._uri, story);
            }
        }});
        row.find(".add-task-link").bind("click",function(){
            jQuery("#new-task-story-id").val(""+id);
            jQuery("#new-task-contributor").val("");
            jQuery("#new-task-body").val("");
            jQuery("#new-task-form").toggle();
            Scrummy.center(jQuery("#new-task-form"));
            return false;
        });
        return row;
    },

    insertStory: function(story) {
        var rows = jQuery('#wall-body').find("tr");
        if(rows.length == 0) {
            jQuery('#wall-body').append(Scrummy.makeStoryRow(story));
        } else {
            for(var i=0; i<rows.length; i++) {
                var id = parseInt(rows[i].id);
                if(id>story.priority) {
                    Scrummy.makeStoryRow(story).insertBefore(jQuery('#wall-body').find("tr#"+id));
                    break;
                } else {
                    if(i == (rows.length -1)) {
                        jQuery('#wall-body').append(Scrummy.makeStoryRow(story));
                    }
                }
            }
        }
        // Let's load the tasks for this story
        Plaza.ABox.loadInstances("tasks",{"belongsToStory": story._uri});
    },

    insertTask: function(task) {
        var storyId = null;
        for(var i in Scrummy.storiesMap) {
            if(Scrummy.storiesMap[i] == task.belongsToStory) {
                storyId = i;
                break;
            }
        }

        var taskId = task.restResourceId;
        var txt = "<div class='task-panel' id='"+taskId+"'><input class='story-id' type='hidden' value='"+storyId+"'/>"
        txt = txt + "<input class='task-uri' type='hidden' value='"+task._uri+"'/>";
        txt = txt + "<div class='task-panel-body'>"+task.title+"</div><div class='task-panel-contributor'>"+task.contributor+"</div>"
        txt = txt + "<div class='task-buttons' style='display:none'>edit</div>"
        txt = txt + "<div class='task-close ui-icon ui-icon-closethick' style='display:none'>edit</div>"
        txt = txt + "</div>"
        var storyTr = jQuery("tr#"+storyId);
        var panel = jQuery(txt)

        panel.find(".task-buttons").bind("click",function(){
            var editForm = jQuery("#edit-task-form");
            editForm.find("#edit-task-body").val(task.title);
            editForm.find("#edit-task-contributor").val(task.contributor);
            editForm.find("#edit-task-id").val(task._uri);
            editForm.toggle();
            Scrummy.center(editForm);
        });

        panel.find(".task-close").bind("click", function(){
            panel.remove();
            Plaza.ABox.destroyEntity(task._uri);
        });

        var color = Scrummy.chooseColor(task.contributor);
        panel.css("backgroundColor",color);

        panel.bind("mouseover", function(){
            panel.find(".task-buttons").show();
            panel.find(".task-close").show();
        });
        panel.bind("mouseout", function(){
            panel.find(".task-buttons").hide();
            panel.find(".task-close").hide();
        });
        panel.find(".task-buttons")
        if(task.status == "todo") {
            storyTr.find("td.story-todo").append(panel);
        }else {
            if(task.status == "progress") {
                storyTr.find("td.story-progress").append(panel);
            } else {
                storyTr.find("td.story-done").append(panel);
            }
        }
    },

    registerObservers: function() {
        Plaza.ABox.startObservingSpace("stories", Plaza.ABox.EVENTS.CREATED, this,
                                       function(_space, _event, entity) {
                                           console.log("Created story: " + entity._uri);
                                           Scrummy.insertStory(entity);
                                       });
        Plaza.ABox.startObservingSpace("tasks", Plaza.ABox.EVENTS.CREATED, this,
                                       function(_space, _event, entity) {
                                           console.log("Created task: " + entity._uri);
                                           Scrummy.insertTask(entity);
                                       });
    },

    bindButtons: function() {
        jQuery('#new-story').bind("click", function() {
            jQuery("#new-story-body").text("")
            jQuery("#new-story-form").toggle();
            Scrummy.center(jQuery("#new-story-form"));
        });

        jQuery('#send-new-story').bind("click", function(evt){
            var title = jQuery('#new-story-body').val();
            Plaza.ABox.createEntity("stories", {"belongsToSprint": Scrummy.currentSprint._uri,
                                                "title": title,
                                                "priority": Scrummy.maxPriority++});
            jQuery('#new-story-body').val("");
            jQuery(this).parent().parent().toggle();
            return false;
        });
        jQuery('#cancel-new-story').bind("click", function(){jQuery(this).parent().parent().toggle()});

        jQuery('#send-new-task').bind("click", Scrummy.createTask);
        jQuery('#cancel-new-task').bind("click", function(){jQuery(this).parent().parent().toggle()});

        jQuery('#send-edit-task').bind("click", Scrummy.editTask);
        jQuery('#cancel-edit-task').bind("click", function(){jQuery(this).parent().parent().toggle()});
    }
};
