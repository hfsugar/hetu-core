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

import com.google.common.util.concurrent.Futures;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusRuntimeException;
import io.grpc.services.HealthStatusManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static nova.hetu.cluster.ClusterConfig.config;

/**
 * A singleton ensures a consistent view of the cluster
 */
public class Cluster
{
    private static Logger log = Logger.getLogger(Cluster.class);

    private Server server;

    private Cluster(Server server)
    {
        this.server = server;
    }

    /**
     * During start up, the cluster will start the services and then join a cluster
     */
    public void start()
            throws IOException, InterruptedException
    {
        server.start();
        log.info("Listening on port " + server.getPort());
        log.info("\t Services:");
        List<ServerServiceDefinition> services = server.getServices();
        for (ServerServiceDefinition service : services) {
            log.info("\t\t " + service.getServiceDescriptor().getName());
        }

        //joinCluster();
        addShutdownHook(server);
        //server.awaitTermination();
        log.info("gRpc server started properly");
    }

    private void joinCluster()
    {
        Futures.submit(() -> {
            for (ClusterConfig.EndPoint master : config.masters) {
                try {
                    if (!isLocal(master.ip, master.port)) {
                        log.debug("Joining: " + master.ip + ":" + master.port);
                        //FIXME: TLS disabled
                        ManagedChannel channel = ManagedChannelBuilder.forAddress(master.ip, master.port).usePlaintext().build();
                        ClusterGrpc.ClusterBlockingStub stub = ClusterGrpc.newBlockingStub(channel);
                        stub.join(ClusterOuterClass.Node.newBuilder().setIp(config.local.ip).setPort(config.local.port).build());
                    }
                }
                catch (StatusRuntimeException e) {
                    log.debug("Service not available at " + master.ip + ":" + master.port);
                }
            }
        }, Executors.newSingleThreadExecutor());
    }

    /**
     * a cluster is local only if the ip and port are the same
     *
     * @param ip
     * @param port
     * @return
     * @throws UnknownHostException
     */
    private boolean isLocal(String ip, int port)
    {
        return ip.equals(config.local.ip)
                && port == config.local.port;
    }

    /**
     * A shutdown hook to terminate the service gracefully
     *
     * @param server
     */
    private static void addShutdownHook(Server server)
    {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                // Start graceful shutdown
                server.shutdown();
                try {
                    // Wait for RPCs to complete processing
                    if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
                        // That was plenty of time. Let's cancel the remaining RPCs
                        server.shutdownNow();
                        // shutdownNow isn't instantaneous, so give a bit of time to clean resources up
                        // gracefully. Normally this will be well under a second.
                        server.awaitTermination(5, TimeUnit.SECONDS);
                    }
                }
                catch (InterruptedException ex) {
                    server.shutdownNow();
                }
            }
        });
    }

    public static Builder Builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        ServerBuilder serverBuilder;

        public Builder()
        {
            serverBuilder = ServerBuilder.forPort(config.local.port)
                    .addService(new HealthStatusManager().getHealthService())
                    .addService(new ClusterService());
        }

        public Builder addService(BindableService service)
        {
            serverBuilder.addService(service);
            return this;
        }

        public Cluster build()
        {
            return new Cluster(serverBuilder.build());
        }
    }
}
