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

import io.airlift.configuration.Config;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ShuffleServiceConfig
{
    private boolean enabled = true;
    private String host;
    private int port = 16544;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("shuffle-service.enabled")
    public ShuffleServiceConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public String getHost()
    {
        if (host == null) {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            }
            catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return this.host;
    }

    @Config("shuffle-service.host")
    public ShuffleServiceConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    @Config("shuffle-service.port")
    public ShuffleServiceConfig setPort(int port)
    {
        this.port = port;
        return this;
    }
}
