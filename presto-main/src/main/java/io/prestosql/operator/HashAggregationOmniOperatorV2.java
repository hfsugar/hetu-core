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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.execution.Lifespan;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.AggType;
import nova.hetu.omnicache.vector.VecType;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashAggregationOmniOperatorV2
        implements Operator
{
    private static final Logger log = Logger.get(HashAggregationOmniOperatorV2.class);
    private final long stageID;
    private VecType[] inputTypes;

    private int[] outputLayout;
    private OperatorContext operatorContext;
    private final long omniOperatorID;
    private final OmniRuntime omniRuntime;
    VecType[] outputTypes;

    private boolean finishing;
    private boolean finished;

    HashAggregationOmniWorkV2 omniWork;

    public HashAggregationOmniOperatorV2(OperatorContext operatorContext, OmniRuntime omniRuntime, long stageID, long omniOperatorID, VecType[] inputTypes, VecType[] outputTypes, int[] outputLayout)
    {
        this.operatorContext = operatorContext;
        this.omniRuntime = omniRuntime;
        this.stageID = stageID;
        this.omniOperatorID = omniOperatorID;
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
        this.outputLayout = outputLayout;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return this.operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close()
            throws Exception
    {

    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }
        if (omniWork != null && !omniWork.isFinished()) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");

        if (omniWork == null) {
            omniWork = new HashAggregationOmniWorkV2(omniRuntime, stageID, omniOperatorID, inputTypes, outputTypes, outputLayout);
        }
        omniWork.process(page);
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        if (finishing) {
            if (omniWork == null) {
                finished = true;
                return null;
            }
            if (omniWork.pageCount>0) {
                omniWork.processRemaining();
            }
            if (omniWork.isFinished()) {
                finished = true;
                return omniWork.getResult();
            }
        }

        return null;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return null;
    }

    @Override
    public void finishMemoryRevoke()
    {

    }

    public static class HashAggregationOmniOperatorFactory
            implements OperatorFactory
    {
        private final OmniRuntime omniRuntime;
        private final long stageID;
        private int[] outputLayout;
        private List<VecType[]> inAndOutputTypes;
        private AggType[] omniAggregator;
        private int omniTotalChannels;
        private int[] omniGrouByChannels;
        private int[] omniAggregationChannels;
        int operatorId;
        PlanNodeId planNodeId;
        VecType[] omniGroupByTypes;
        VecType[] omniAggregationTypes;
        VecType[] omniAggReturnTypes;

        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, long stageID, int omniTotalChannels, int[] omniGrouByChannels, VecType[] omniGroupByTypes, int[] omniAggregationChannels, VecType[] omniAggregationTypes, AggType[] omniAggregator, VecType[] omniAggReturnTypes, List<VecType[]> inAndOutputTypes, int[] outputLayout)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.stageID = stageID;
            this.omniGroupByTypes = omniGroupByTypes;
            this.omniAggregationTypes = omniAggregationTypes;
            this.omniAggReturnTypes = omniAggReturnTypes;
            this.omniTotalChannels = omniTotalChannels;
            this.omniGrouByChannels = omniGrouByChannels;
            this.omniAggregator = omniAggregator;
            this.omniAggregationChannels = omniAggregationChannels;
            this.inAndOutputTypes = inAndOutputTypes;
            this.outputLayout = outputLayout;
            this.omniRuntime = new OmniRuntime();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOmniOperatorV2.class.getSimpleName());

            long omniOperatorID = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
            omniRuntime.prepareAgg(stageID, omniOperatorID, omniTotalChannels, omniGrouByChannels, omniGroupByTypes, omniAggregationChannels, omniAggregationTypes, omniAggregator, omniAggReturnTypes, inAndOutputTypes.get(0));
            HashAggregationOmniOperatorV2 hashAggregationOperator = new HashAggregationOmniOperatorV2(operatorContext, omniRuntime, stageID, omniOperatorID, inAndOutputTypes.get(0), inAndOutputTypes.get(1), outputLayout);
            return hashAggregationOperator;
        }

        @Override
        public void noMoreOperators()
        {

        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {

        }

        @Override
        public OperatorFactory duplicate()
        {
            return null;
        }
    }
}
