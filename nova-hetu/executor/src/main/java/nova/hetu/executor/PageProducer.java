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
package nova.hetu.executor;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.spi.Page;

public class PageProducer
{
    /**
     * Creates a Page Producer for sending pages to a taskid of a specific partition, when the output is not parititoned
     * the default partitionid is 0
     * @param taskid
     * @param partitionId
     * @param serde
     * @return
     */
    public static PageProducer create(String taskid, String partitionId, PagesSerde serde /** can we hide this? */)
    {
        return new PageProducer(ShuffleService.getStream(taskid, partitionId, serde));
    }

    private ShuffleService.Stream out;

    PageProducer(ShuffleService.Stream out)
    {
        this.out = out;
    }

    public void send(Page page)
            throws InterruptedException
    {
        out.write(page);
    }

    public void close()
            throws Exception
    {
        out.close();
    }

    public boolean isClosed()
    {
        return out.isClosed();
    }
}
