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
package nova.hetu.shuffle;

import java.net.URI;

public class ProducerInfo
{
    private final String host;
    private final int port;
    private final String producerId;

    public ProducerInfo(URI producerLocation)
    {
        this.host = producerLocation.getHost();
        this.port = producerLocation.getPort();
        String[] paths = producerLocation.getPath().split("/");
        this.producerId = String.format("%s-%s", paths[3], paths[5]);
    }

    public ProducerInfo(String host, int port, String producerId)
    {
        this.host = host;
        this.port = port;
        this.producerId = producerId;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public String getProducerId()
    {
        return producerId;
    }
}
