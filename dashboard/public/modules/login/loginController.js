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

angular.module('dgc.login').controller('LoginController', ['$scope', 'Global', '$location', '$http', '$state', '$rootScope', '$cookieStore',
    function($scope, Global, $location, $http, $state, $rootScope, $cookieStore) {
        $scope.loggUser = function(form) {
            if (form.$valid) {
                if ($scope.user.username === 'admin' && $scope.user.password === 'admin') {
                    var userToken = {};
                    userToken.timeOutLimit = $scope.user.timeOut;
                    userToken.user = $scope.user.username;
                    userToken.timeOut = new Date().getTime();
                    var userSession = {
                        sessionId: "user12345",
                        timeOut: userToken.timeOut
                    };
                    Global.setUserSession(userSession, userToken);
                    $rootScope.username = Global.getUserSession().user;
                    var lastRoute = $cookieStore.get('LastRoute'),
                        lastParamObj = $cookieStore.get('LastRouteParam');
                    if (lastRoute !== undefined && lastRoute !== 'login' && lastRoute !== '') {
                        $state.go(lastRoute, lastParamObj);
                        $cookieStore.remove('LastRoute');
                        $cookieStore.remove('LastRouteParam');
                    } else {
                        $state.go('search');
                    }
                } else {
                    $scope.showLoginVal = {
                        show: true,
                        userPassInvalid: true
                    };
                }

            } else {
                $scope.showLoginVal = {
                    show: true,
                    userPassInvalid: true
                };
            }
        };
    }
]);
