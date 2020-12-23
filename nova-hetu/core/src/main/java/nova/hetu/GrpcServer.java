/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nova.hetu;

import nova.hetu.cluster.Cluster;
import nova.hetu.cluster.ClusterConfig;
import nova.hetu.executor.ShuffleService;
import org.apache.log4j.Logger;

import java.io.IOException;

public class GrpcServer
{
    private static Logger log = Logger.getLogger(GrpcServer.class);
    private static Cluster cluster;

    private GrpcServer() {}

    public static void main(String[] args)
    {
        start();
    }

    public static void start()
    {
        addTestMasters();
        int i = 0;
        while (i < 10) {
            try {
                cluster = Cluster.Builder()
                        .addService(new ShuffleService()).build();
                cluster.start();
                break;
            }
            catch (IOException e) { //port is occupied
                log.debug("Port: " + ClusterConfig.config.local.port + " is in use");
                ClusterConfig.config.local.port++; //increment the port
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            i++;
        }
    }

    private static void addTestMasters()
    {
        ClusterConfig.config.addMaster(new ClusterConfig.EndPoint(ClusterConfig.config.local.ip, ClusterConfig.config.local.port + 1));
        ClusterConfig.config.addMaster(new ClusterConfig.EndPoint(ClusterConfig.config.local.ip, ClusterConfig.config.local.port + 2));
    }

    public static void shutdown()
    {
        cluster.shutdown();
    }
}
