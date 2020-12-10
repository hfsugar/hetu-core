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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestClusterService
{
    private TestClusterService() {}

    public static void main(String[] args)
            throws UnknownHostException
    {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(ClusterConfig.config.local.ip, ClusterConfig.config.local.port).usePlaintext().build();

        ClusterGrpc.ClusterBlockingStub stub = ClusterGrpc.newBlockingStub(channel);
        ClusterOuterClass.ClusterInfo info = stub.join(ClusterOuterClass.Node.newBuilder().setIp(InetAddress.getLocalHost().getHostAddress()).setPort(58092).build());
        System.out.println(info);
    }
}
