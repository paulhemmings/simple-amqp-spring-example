(function(app){

    var compiledTemplates = {},
        templateServerUrl = "public/partials/",

    storeTemplate = function(id, html) {
        compiledTemplates[id] = Handlebars.compile(html);
        return compiledTemplates[id];
    },

    compileTemplates = function() {
        $("*[type='text/x-handlebars-template']").each(function() {
            storeTemplate($(this).attr('id'), $(this).html());
        });
    },

    renderTemplate = function(id, container, data, append) {
        var deferred = new $.Deferred();
        getTemplatePromise(id)
            .done(function(template) {
                var html = template(data);
                if(append || false) {
                    $(container).append(html);
                } else {
                    $(container).html(html);
                }
                deferred.resolve();
            });
        return deferred.promise();
    },

    getTemplatePromise = function(id) {
        var deferred = new $.Deferred();

        if(compiledTemplates[id]) {
            deferred.resolve(compiledTemplates[id]);
        } else {
            loadTemplate(id, function(template) {
                deferred.resolve(template);
            });
        }

        return deferred.promise();
    },

    getTemplate = function(id, callback) {
        if(compiledTemplates[id]) {
            callback(compiledTemplates[id]);
        } else {
            loadTemplate(id, callback);
        }
    },

    loadTemplate = function(id, callback) {
        var self = this;
        $.ajax(templateServerUrl + id + ".handlebars")
            .done(function(data) {
                callback(self.storeTemplate(id, data));
            })
            .fail(function() {
                console.error('failed to load template ' + id);
            });
    },

    registerHelpers = function() {

        Handlebars.registerHelper('contains', function(context, options) {
            var values = Array.prototype.slice.call(arguments,1);
            if(arguments[0].indexOf(arguments[1]) > -1) {
                return options.fn(this);
            } else {
                return options.inverse(this);
            }
        });

        Handlebars.registerHelper('renderPartial', function(partialPath) {

            if (!partialPath) {
                console.error('No partial name given.');
            }

            var values = Array.prototype.slice.call(arguments,1);

            var partial = Handlebars.partials[partialPath];
            if (!partial) {
                return '';
            }
            var context = $.extend(this, values.pop().hash);
            return new Handlebars.SafeString( partial(context) );
        });

    },

    init = function(serviceUrl) {
        templateServerUrl = serviceUrl || templateServerUrl;
        compileTemplates();
        registerHelpers();
    };

    app.TemplateLayer = {
        'init': init,
        'renderTemplate': renderTemplate
    }

})(MainApp);