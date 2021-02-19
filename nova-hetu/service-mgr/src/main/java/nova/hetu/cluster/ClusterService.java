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

import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

public class ClusterService
        extends ClusterGrpc.ClusterImplBase
{
    private static Logger log = Logger.getLogger(ClusterService.class);

    ClusterOuterClass.ClusterInfo.Builder clusterInfo = ClusterOuterClass.ClusterInfo.newBuilder().addNodes(ClusterOuterClass.Node.newBuilder().setIp(ClusterConfig.config.local.ip).setPort(ClusterConfig.config.local.port));

    @Override
    public void join(ClusterOuterClass.Node request, StreamObserver<ClusterOuterClass.ClusterInfo> responseObserver)
    {
        responseObserver.onNext(join(request));
        responseObserver.onCompleted();
    }

    private ClusterOuterClass.ClusterInfo join(ClusterOuterClass.Node request)
    {
        ClusterOuterClass.ClusterInfo newNode = ClusterOuterClass.ClusterInfo.newBuilder().addNodes(request).build();
        clusterInfo.mergeFrom(newNode);
        log.debug(request.getIp() + ":" + request.getPort() + " joined the cluster");
        return clusterInfo.build();
    }
}
