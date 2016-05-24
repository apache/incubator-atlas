/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var git = require('git-rev');
module.exports = function(grunt) {
    var classPathSep = (process.platform === "win32") ? ';' : ':',
        gitHash = '',
        pkg = grunt.file.readJSON('package.json'),
        distPath = 'dist',
        publicPath = 'public',
        libPath = distPath + '/js/libs',
        isDashboardDirectory = grunt.file.isDir('public'),
        modulesPath = 'public/';
    if (!isDashboardDirectory) {
        modulesPath = '../public/'
    }

    var proxySnippet = require('grunt-connect-proxy/lib/utils').proxyRequest;


    grunt.initConfig({
        watch: {
            js: {
                files: ['public/**/*.js'],
                tasks: ['copy:dist']
            },
            html: {
                files: ['public/**/*.html'],
                tasks: ['copy:dist']
            },
            css: {
                files: ['public/**/*.scss', 'public/**/*.css'],
                tasks: ['sass', 'copy:dist']
            },
            image: {
                files: ['public/**/*.{ico,gif,png}'],
                tasks: ['copy:dist']
            }
        },
        connect: {
            server: {
                options: {
                    port: 9999,
                    base: distPath,
                    // change this to '0.0.0.0' to access the server from outside
                    hostname: '0.0.0.0',
                    middleware: function(connect, options, defaultMiddleware) {
                        var proxy = require('grunt-connect-proxy/lib/utils').proxyRequest;
                        return [
                            // Include the proxy first
                            proxy
                        ].concat(defaultMiddleware);

                    }

                },
                proxies: [{
                    context: '/api', // the context of the data service
                    host: '127.0.0.1', // wherever the data service is running
                    port: 21000, // the port that the data service is running on
                    ws: true,
                    changeOrigin: false,
                    https: false,
                    xforward: false,
                    //xforward: false
                }],
            },
        },
        devUpdate: {
            main: {
                options: {
                    updateType: 'force'
                }
            }
        },
        compress: {
            release: {
                options: {
                    archive: function() {
                        return [pkg.name, pkg.version, gitHash].join('_') + '.tgz';
                    }
                },
                src: ['node_modules/**', 'package.json', 'server.js', 'server/**', 'public/**', '!public/js/**', '!public/modules/**/*.js']
            }
        },
        npmcopy: {
            // Javascript 
            js: {
                options: {
                    destPrefix: libPath
                },
                files: {
                    'jquery/js': 'jquery/dist/jquery.min.js',
                    'requirejs': 'requirejs/require.js',
                    'requirejs-text': 'requirejs-text/text.js',
                    'underscore': 'underscore/underscore-min.js',
                    'bootstrap/js': 'bootstrap/dist/js/bootstrap.min.js',
                    'backbone': 'backbone/backbone-min.js',
                    'backbone-babysitter': 'backbone.babysitter/lib/backbone.babysitter.min.js',
                    'backbone-marionette': 'backbone.marionette/lib/backbone.marionette.min.js',
                    'backbone-paginator': 'backbone.paginator/lib/backbone.paginator.min.js',
                    'backbone-wreqr': 'backbone.wreqr/lib/backbone.wreqr.min.js',
                    'backgrid/js': 'backgrid/lib/backgrid.js',
                    'backgrid-filter/js': 'backgrid-filter/backgrid-filter.min.js',
                    'backgrid-orderable-columns/js': 'backgrid-orderable-columns/backgrid-orderable-columns.js',
                    'backgrid-paginator/js': 'backgrid-paginator/backgrid-paginator.min.js',
                    'backgrid-sizeable-columns/js': 'backgrid-sizeable-columns/backgrid-sizeable-columns.js',
                    'jquery-asBreadcrumbs/js': 'jquery-asBreadcrumbs/dist/jquery-asBreadcrumbs.min.js',
                    'd3': 'd3/d3.min.js',
                    'd3/': 'd3-tip/index.js',
                    'noty/js': 'noty/js/noty/packaged/jquery.noty.packaged.min.js',
                    'dagre-d3': 'dagre-d3/dist/dagre-d3.min.js',
                    'jstree': 'jstree/dist/jstree.min.js',
                    'select2': 'select2/dist/js/select2.min.js'
                }
            },
            css: {
                options: {
                    destPrefix: libPath
                },
                files: {
                    'bootstrap/css': 'bootstrap/dist/css/bootstrap.min.css',
                    'bootstrap/fonts': 'bootstrap/dist/fonts',
                    'backgrid/css': 'backgrid/lib/backgrid.css',
                    'backgrid-filter/css': 'backgrid-filter/backgrid-filter.min.css',
                    'backgrid-orderable-columns/css': 'backgrid-orderable-columns/backgrid-orderable-columns.css',
                    'backgrid-paginator/css': 'backgrid-paginator/backgrid-paginator.css',
                    'backgrid-sizeable-columns/css': 'backgrid-sizeable-columns/backgrid-sizeable-columns.css',
                    'jquery-asBreadcrumbs/css': 'jquery-asBreadcrumbs/css/asBreadcrumbs.css',
                    'select2/css': 'select2/dist/css/select2.min.css'
                }

            }
        },
        sass: {
            dist: {
                files: {
                    'dist/css/style.css': 'dist/css/scss/style.scss'
                }
            }
        },
        copy: {
            dist: {
                expand: true,
                cwd: modulesPath,
                src: ['**', '!**/*.sass'],
                dest: distPath
            }
        },
        clean: {
            build: [distPath, libPath],
            options: {
                force: true
            }
        }
    });

    grunt.loadNpmTasks('grunt-connect-proxy');
    grunt.loadNpmTasks('grunt-contrib-connect');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-npmcopy');


    require('load-grunt-tasks')(grunt);

    grunt.registerTask('default', [
        'devUpdate',
        'npmcopy:js',
        'npmcopy:css'
    ]);

    grunt.registerTask('server', ['clean', 'copy:dist', 'concurrent', 'watch']);

    grunt.registerTask('dev', [
        'clean',
        'npmcopy:js',
        'npmcopy:css',
        'copy:dist',
        'sass',
        'configureProxies:server',
        'connect:server',
        /* 'concurrent',*/
        'watch',
        /*'connect:livereload'*/
    ]);

    grunt.registerTask('build', [
        'npmcopy:js',
        'npmcopy:css',
        'copy:dist',
        'sass'
    ]);

    grunt.registerTask('minify', 'Minify the all js', function() {
        var done = this.async();
        grunt.task.run(['shell:min']);
        done();
    });
    grunt.registerTask('release', 'Create release package', function() {
        var done = this.async();
        git.short(function(str) {
            gitHash = str;
            grunt.task.run(['minify', 'compress:release']);
            done();
        });
    });
};
