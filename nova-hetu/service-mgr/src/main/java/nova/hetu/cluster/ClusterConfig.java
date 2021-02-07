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
package nova.hetu.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;

/**
 * Configuration of the cluster, obtained via a plugin,
 * which can retrieve from zk, etcd, consul
 * The ClusterConfig class is static, it can be refreshed using the plugin from the predefined location
 */
public class ClusterConfig
{
    public static ClusterConfig config = new ClusterConfig();
    public HashSet<EndPoint> masters = new HashSet<>();
    public EndPoint local;

    private ClusterConfig()
    {
        try {
            local = new EndPoint(InetAddress.getLocalHost().getHostAddress(), (short) ('H' * 'E' * 'T' * 'U') /** 16544 */);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void addMaster(EndPoint ep)
    {
        masters.add(ep);
    }

    public static final class EndPoint
    {
        public EndPoint(String ip, int port)
        {
            this.ip = ip;
            this.port = port;
        }

        public String ip;
        public int port;
    }
}
