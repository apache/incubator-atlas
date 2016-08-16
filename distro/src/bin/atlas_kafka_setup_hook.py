#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
import atlas_client_cmdline as cmdline
import atlas_config as mc

def main():
    conf_dir = cmdline.setup_conf_dir()
    jvm_opts_list = cmdline.setup_jvm_opts_list(conf_dir, 'atlas_kafka_setup_hook.log')
    atlas_classpath = cmdline.get_atlas_hook_classpath(conf_dir)
    topics_array = mc.get_topics_to_create(conf_dir)
    process = mc.java("org.apache.atlas.hook.AtlasTopicCreator", topics_array, atlas_classpath, jvm_opts_list)
    return process.wait()

if __name__ == '__main__':
    try:
        returncode = main()
    except Exception as e:
        print "Exception in setting up Kafka topics for Atlas: %s" % str(e)
        returncode = -1

    sys.exit(returncode)