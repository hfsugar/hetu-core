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
package io.prestosql.operator;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.net.URI;

/**
 * Responsible for streaming back the pages produced by the locations (e.g. splits)
 */
public interface ExchangeClient
        extends Closeable
{
    ExchangeClientStatus getStatistics();

    void addLocation(URI location);

    void setNoMoreLocation();

    /**
     * FIXME: Should be consolidate with a better API so that the WorkProcessor logic for late materialization is isolated
     * this is currently used by MergeOperator only
     *
     * @return
     */
    default WorkProcessor<Page> pages()
    {
        return WorkProcessor.create(() -> {
            Page page = pollPage();
            if (page == null) {
                if (isFinished()) {
                    return WorkProcessor.ProcessState.finished();
                }

                ListenableFuture<?> blocked = isBlocked();
                if (!blocked.isDone()) {
                    return WorkProcessor.ProcessState.blocked(blocked);
                }

                return WorkProcessor.ProcessState.yield();
            }

            return WorkProcessor.ProcessState.ofResult(page);
        });
    }

    /**
     * Returns the currently returned pages
     *
     * @return
     */
    @Nullable
    Page pollPage();

    boolean isFinished();

    boolean isClosed();

    ListenableFuture<?> isBlocked();

    void close();
}
