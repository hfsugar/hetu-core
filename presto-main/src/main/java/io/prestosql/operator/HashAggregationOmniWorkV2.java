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

import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DoubleArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class HashAggregationOmniWorkV2<O>
        implements Work<Page>
{

    private final VecType[] omniGroupByTypes;
    private final VecType[] omniAggregationTypes;
    private final VecType[] omniAggReturnTypes;
    OmniRuntime omniRuntime;
    String omniOperatorID;
    private boolean finished;
    private Vec[] result;
    private Page page;
    private boolean flag;

    public HashAggregationOmniWorkV2(Page page, OmniRuntime omniRuntime, String omniOperatorID, VecType[] omniGroupByTypes, VecType[] omniAggregationTypes, VecType[] omniAggReturnTypes)
    {
        this.page = page;
        this.omniRuntime = omniRuntime;
        this.omniOperatorID = omniOperatorID;
        this.omniGroupByTypes = omniGroupByTypes;
        this.omniAggregationTypes = omniAggregationTypes;
        this.omniAggReturnTypes = omniAggReturnTypes;
    }

    @Override
    public boolean process()
    {
        VecType[] inputTypes = ArrayUtils.addAll(omniGroupByTypes, omniAggregationTypes);

        Vec[] inputData = new Vec[inputTypes.length];
        for (int i = 0; i < inputTypes.length; i++) {
            inputData[i] = page.getBlock(i).getValues();
        }

        int rowNum = page.getPositionCount();

        if (inputTypes.length!=inputData.length) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,"Agg omni operator input type and data not map");
        }

//        Block b0 = page.getBlock(0);
//        Block b1 = page.getBlock(1);
//        for (int i = 0; i < rowNum; i++) {
//            if((Long)b1.get(i)!=70 &&(Long)b1.get(i)!=79){
//                flag = true;
//                System.out.println("input stage column 1 has random value");
//            }
//        }
//        for (int i = 0; i < rowNum; i++) {
//            if((Long)b0.get(i)!=65 &&(Long)b0.get(i)!=78 &&(Long)b0.get(i)!=82){
//                flag = true;
//                System.out.println("input stage column 0 has random value");
//            }
//        }
//
//        for (int i = 0; i < rowNum; i++) {
//            if(((LongVec)inputData[1]).get(i)!=70 &&((LongVec)inputData[1]).get(i)!=79){
//                flag = true;
//                System.out.println("omniinput stage column 1 has random value");
//            }
//        }
//        for (int i = 0; i < rowNum; i++) {
//            if(((LongVec)inputData[0]).get(i)!=65 &&((LongVec)inputData[0]).get(i)!=78 &&((LongVec)inputData[0]).get(i)!=82){
//                flag = true;
//                System.out.println("omniinput stage column 0 has random value");
//            }
//        }

        omniRuntime.executeAggIntermediate(omniOperatorID, inputData, inputTypes, rowNum);

        finished = true;
        return true;
    }

    @Override
    public Page getResult()
    {
        checkState(finished, "process has not finished");

        VecType[] outputTypes = ArrayUtils.addAll(omniGroupByTypes, omniAggReturnTypes);

        result = omniRuntime.executeAggFinal(omniOperatorID,outputTypes);

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
            if (omniExecutionResult[i] instanceof DoubleVec) {
                blocks[i] = new DoubleArrayBlock(positionCount, Optional.of(valueIsNull), ((DoubleVec) omniExecutionResult[i]));
            }
            else if(omniExecutionResult[i] instanceof LongVec){
                blocks[i] = new LongArrayBlock(positionCount, Optional.of(valueIsNull), (LongVec) omniExecutionResult[i]);
            }
        }

        Page page = new Page(blocks);

        return page;
    }

    public boolean isFinished()
    {
        return finished;
    }

    public void updatePages(Page page)
    {
        this.page = page;
    }
}
