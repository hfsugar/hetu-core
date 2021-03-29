/*
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

import io.prestosql.Session;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DoubleArrayBlock;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.SystemSessionProperties.getOmniPageThreshold;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public final class HashAggregationOmniWorkV2
{

    private final VecType[] outputTypes;
    private final int[] outputLayout;
    private final VecType[] inputTypes;
    private final long stageID;
    private final OmniRuntime omniRuntime;
    private final long omniOperatorID;
    private boolean finished;
    private Vec[] result;
    //    private Page page;
    List<Vec> multiPageVecList = new ArrayList<>();
    int pageCount;
    private final int PAGE_THRESHOLD;

    public HashAggregationOmniWorkV2(Optional<Session> session, OmniRuntime omniRuntime, long stageID, long omniOperatorID, VecType[] inputTypes, VecType[] outputTypes, int[] outputLayout)
    {
        requireNonNull(omniRuntime, "omniRuntime is null");
        if (session.isPresent()) {
            PAGE_THRESHOLD = getOmniPageThreshold(session.get());
        }
        else {
            PAGE_THRESHOLD = 100;
        }
        this.omniRuntime = omniRuntime;
        this.stageID = stageID;
        this.omniOperatorID = omniOperatorID;
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
        this.outputLayout = outputLayout;
    }
    public boolean process(Page page)
    {
        requireNonNull(page, "page is null");

        if (page != null && page.getChannelCount() != 0) {
            pageCount++;
        }

        if (inputTypes.length != page.getChannelCount()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Agg omni operator input type channels and data channels not match");
        }
        for (int i = 0; i < page.getChannelCount(); i++) {
            Vec vec = page.getBlock(i).getVec();
            if (vec == null || vec.getData() == null) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "omni Vec or vec.getData() is null,may have been released");
            }
            if (vec.size() != page.getPositionCount()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Vec size not equal the page position count");
            }
            multiPageVecList.add(vec);
        }

        if (pageCount >= PAGE_THRESHOLD) {
            omniRuntime.executeAggIntermediate(stageID, omniOperatorID, multiPageVecList, inputTypes.length);
        }
        else {
            finished = true;
            return true;
        }

        closeVec();

        multiPageVecList.clear();
        pageCount = 0;
        finished = true;
        return true;
    }

    private void closeVec()
    {
        for (Vec vec : multiPageVecList) {
            vec.close();
        }
    }

    public Page getResult()
    {
        checkState(finished, "process has not finished");

        result = omniRuntime.executeAggFinal(omniOperatorID, outputTypes);
//        System.out.println("Vec invoke close count: "+Vec.getCount()+" totalSize: "+Vec.getTotalSize());
        return toResult(result);
    }

    public Page toResult(Vec[] omniExecutionResult)
    {
        int positionCount = omniExecutionResult[0].size();
        int chanelCount = omniExecutionResult.length;

        boolean[] valueIsNull = new boolean[positionCount];
        for (int i = 0; i < positionCount; i++) {
            valueIsNull[i] = false;
        }
        Block[] blocks = new Block[chanelCount];

        for (int i = 0; i < chanelCount; i++) {
            if (omniExecutionResult[outputLayout[i]] instanceof DoubleVec) {
                blocks[i] = new DoubleArrayBlock(positionCount, Optional.of(valueIsNull), ((DoubleVec) omniExecutionResult[outputLayout[i]]));
            }
            else if (omniExecutionResult[outputLayout[i]] instanceof LongVec) {
                blocks[i] = new LongArrayBlock(positionCount, Optional.of(valueIsNull), (LongVec) omniExecutionResult[outputLayout[i]]);
            }
            else if (omniExecutionResult[outputLayout[i]] instanceof IntVec){
                blocks[i] = new IntArrayBlock(positionCount, Optional.of(valueIsNull), (IntVec) omniExecutionResult[outputLayout[i]]);
            }
        }

        Page page = new Page(blocks);

        return page;
    }

    public boolean isFinished()
    {
        return finished;
    }

    public int getPageCount(){
        return this.pageCount;
    }

    public void processRemaining()
    {

        omniRuntime.executeAggIntermediate(stageID, omniOperatorID, multiPageVecList, inputTypes.length);

        closeVec();
        finished = true;
        multiPageVecList.clear();
        pageCount=0;
    }
}
