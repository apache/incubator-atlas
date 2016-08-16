#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import sys
from os import environ
from mock import patch
from mock import call
import unittest
import logging
import atlas_config as mc
import atlas_start as atlas
import platform

IS_WINDOWS = platform.system() == "Windows"
logger = logging.getLogger()

class TestMetadata(unittest.TestCase):
  @patch.object(mc,"runProcess")
  @patch.object(mc,"configure_hbase")
  @patch.object(mc,"getConfig")
  @patch.object(mc,"grep")
  @patch.object(mc,"exist_pid")
  @patch.object(mc,"writePid")
  @patch.object(mc, "executeEnvSh")
  @patch.object(mc,"atlasDir")
  @patch.object(mc, "expandWebApp")
  @patch("os.path.exists")
  @patch.object(mc, "java")
  @patch.object(mc, "is_hbase_local")
  @patch.object(mc, "is_solr_local")

  def test_main_embedded(self, is_solr_local_mock, is_hbase_local_mock, java_mock, exists_mock, expandWebApp_mock,
                atlasDir_mock, executeEnvSh_mock, writePid_mock, exist_pid_mock, grep_mock, getConfig_mock,
                configure_hbase_mock, runProcess_mock):
    sys.argv = []
    exists_mock.return_value = True
    expandWebApp_mock.return_value = "webapp"
    atlasDir_mock.return_value = "atlas_home"
    is_hbase_local_mock.return_value = True
    is_solr_local_mock.return_value = True

    exist_pid_mock(789)
    exist_pid_mock.assert_called_with(789)
    grep_mock.return_value = "hbase"
    getConfig_mock.return_value = "localhost:9838"

    atlas.main()
    self.assertTrue(configure_hbase_mock.called)

    if IS_WINDOWS:
      calls = [call(['atlas_home\\hbase\\bin\\start-hbase.cmd', '--config', 'atlas_home\\hbase\\conf'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'start', '-z', 'localhost:9838', '-p', '9838'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'create', '-c', 'vertex_index', '-d', 'atlas_home\\solr\\server\\solr\\configsets\\basic_configs\\conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'create', '-c', 'edge_index', '-d', 'atlas_home\\solr\\server\\solr\\configsets\\basic_configs\\conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'create', '-c', 'fulltext_index', '-d', 'atlas_home\\solr\\server\\solr\\configsets\\basic_configs\\conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home\\logs', False, True)]

      runProcess_mock.assert_has_calls(calls)
    else:
      calls = [call(['atlas_home/hbase/bin/hbase-daemon.sh', '--config', 'atlas_home/hbase/conf', 'start', 'master'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'start', '-z', 'localhost:9838', '-p', '9838'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'create', '-c', 'vertex_index', '-d', 'atlas_home/solr/server/solr/configsets/basic_configs/conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'create', '-c', 'edge_index', '-d', 'atlas_home/solr/server/solr/configsets/basic_configs/conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'create', '-c', 'fulltext_index', '-d', 'atlas_home/solr/server/solr/configsets/basic_configs/conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home/logs', False, True)]

      runProcess_mock.assert_has_calls(calls)

    self.assertTrue(java_mock.called)
    if IS_WINDOWS:
      
      java_mock.assert_called_with(
        'org.apache.atlas.Atlas',
        ['-app', 'atlas_home\\server\\webapp\\atlas'],
        'atlas_home\\conf;atlas_home\\server\\webapp\\atlas\\WEB-INF\\classes;atlas_home\\server\\webapp\\atlas\\WEB-INF\\lib\\*;atlas_home\\libext\\*;atlas_home\\hbase\\conf',
        ['-Datlas.log.dir=atlas_home\\logs', '-Datlas.log.file=application.log', '-Datlas.home=atlas_home', '-Datlas.conf=atlas_home\\conf', '-Xmx1024m', '-XX:MaxPermSize=512m', '-Dlog4j.configuration=atlas-log4j.xml', '-Djava.net.preferIPv4Stack=true'], 'atlas_home\\logs')
      
    else:
      java_mock.assert_called_with(
        'org.apache.atlas.Atlas',
        ['-app', 'atlas_home/server/webapp/atlas'],
        'atlas_home/conf:atlas_home/server/webapp/atlas/WEB-INF/classes:atlas_home/server/webapp/atlas/WEB-INF/lib/*:atlas_home/libext/*:atlas_home/hbase/conf',
        ['-Datlas.log.dir=atlas_home/logs', '-Datlas.log.file=application.log', '-Datlas.home=atlas_home', '-Datlas.conf=atlas_home/conf', '-Xmx1024m', '-XX:MaxPermSize=512m', '-Dlog4j.configuration=atlas-log4j.xml', '-Djava.net.preferIPv4Stack=true'],  'atlas_home/logs')

    pass

  @patch.object(mc,"runProcess")
  @patch.object(mc,"configure_hbase")
  @patch.object(mc,"getConfig")
  @patch.object(mc,"grep")
  @patch.object(mc,"exist_pid")
  @patch.object(mc,"writePid")
  @patch.object(mc, "executeEnvSh")
  @patch.object(mc,"atlasDir")
  @patch.object(mc, "expandWebApp")
  @patch("os.path.exists")
  @patch.object(mc, "java")
  @patch.object(mc, "is_hbase_local")
  @patch.object(mc, "is_solr_local")
  def test_main_default(self, is_solr_local_mock, is_hbase_local_mock, java_mock, exists_mock, expandWebApp_mock,
                         atlasDir_mock, executeEnvSh_mock, writePid_mock, exist_pid_mock, grep_mock, getConfig_mock,
                         configure_hbase_mock, runProcess_mock):
    sys.argv = []
    exists_mock.return_value = True
    expandWebApp_mock.return_value = "webapp"
    atlasDir_mock.return_value = "atlas_home"
    is_hbase_local_mock.return_value = False
    is_solr_local_mock.return_value = False

    exist_pid_mock(789)
    exist_pid_mock.assert_called_with(789)
    grep_mock.return_value = "hbase"
    getConfig_mock.return_value = "localhost:9838"

    atlas.main()
    self.assertFalse(configure_hbase_mock.called)

    if IS_WINDOWS:
      calls = [call(['atlas_home\\hbase\\bin\\start-hbase.cmd', '--config', 'atlas_home\\hbase\\conf'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'start', '-z', 'localhost:9838', '-p', '9838'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'create', '-c', 'vertex_index', '-d', 'atlas_home\\solr\\server\\solr\\configsets\\basic_configs\\conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'create', '-c', 'edge_index', '-d', 'atlas_home\\solr\\server\\solr\\configsets\\basic_configs\\conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home\\logs', False, True),
               call(['atlas_home\\solr\\bin\\solr.cmd', 'create', '-c', 'fulltext_index', '-d', 'atlas_home\\solr\\server\\solr\\configsets\\basic_configs\\conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home\\logs', False, True)]

      runProcess_mock.assert_not_called(calls)
    else:
      calls = [call(['atlas_home/hbase/bin/hbase-daemon.sh', '--config', 'atlas_home/hbase/conf', 'start', 'master'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'start', '-z', 'localhost:9838', '-p', '9838'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'create', '-c', 'vertex_index', '-d', 'atlas_home/solr/server/solr/configsets/basic_configs/conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'create', '-c', 'edge_index', '-d', 'atlas_home/solr/server/solr/configsets/basic_configs/conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home/logs', False, True),
               call(['atlas_home/solr/bin/solr', 'create', '-c', 'fulltext_index', '-d', 'atlas_home/solr/server/solr/configsets/basic_configs/conf', '-shards', '1', '-replicationFactor', '1'], 'atlas_home/logs', False, True)]

      runProcess_mock.assert_not_called(calls)

    self.assertTrue(java_mock.called)
    if IS_WINDOWS:

      java_mock.assert_called_with(
        'org.apache.atlas.Atlas',
        ['-app', 'atlas_home\\server\\webapp\\atlas'],
        'atlas_home\\conf;atlas_home\\server\\webapp\\atlas\\WEB-INF\\classes;atlas_home\\server\\webapp\\atlas\\WEB-INF\\lib\\*;atlas_home\\libext\\*;atlas_home\\hbase\\conf',
        ['-Datlas.log.dir=atlas_home\\logs', '-Datlas.log.file=application.log', '-Datlas.home=atlas_home', '-Datlas.conf=atlas_home\\conf', '-Xmx1024m', '-XX:MaxPermSize=512m', '-Dlog4j.configuration=atlas-log4j.xml', '-Djava.net.preferIPv4Stack=true'], 'atlas_home\\logs')

    else:
      java_mock.assert_called_with(
        'org.apache.atlas.Atlas',
        ['-app', 'atlas_home/server/webapp/atlas'],
        'atlas_home/conf:atlas_home/server/webapp/atlas/WEB-INF/classes:atlas_home/server/webapp/atlas/WEB-INF/lib/*:atlas_home/libext/*:atlas_home/hbase/conf',
        ['-Datlas.log.dir=atlas_home/logs', '-Datlas.log.file=application.log', '-Datlas.home=atlas_home', '-Datlas.conf=atlas_home/conf', '-Xmx1024m', '-XX:MaxPermSize=512m', '-Dlog4j.configuration=atlas-log4j.xml', '-Djava.net.preferIPv4Stack=true'],  'atlas_home/logs')

    pass

  def test_jar_java_lookups_fail(self):
    java_home = environ['JAVA_HOME']
    del environ['JAVA_HOME']
    orig_path = environ['PATH']
    environ['PATH'] = "/dev/null"

    self.assertRaises(EnvironmentError, mc.jar, "foo")
    self.assertRaises(EnvironmentError, mc.java, "empty", "empty", "empty", "empty")

    environ['JAVA_HOME'] = java_home
    environ['PATH'] = orig_path

  @patch.object(mc, "runProcess")
  @patch.object(mc, "which", return_value="foo")
  def test_jar_java_lookups_succeed_from_path(self, which_mock, runProcess_mock):
    java_home = environ['JAVA_HOME']
    del environ['JAVA_HOME']

    mc.jar("foo")
    mc.java("empty", "empty", "empty", "empty")

    environ['JAVA_HOME'] = java_home

if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()
