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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.AggType;
import nova.hetu.omnicache.vector.VecType;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class HashAggregationOmniOperatorV2
        implements Operator
{
    private static final Logger log = Logger.get(HashAggregationOmniOperatorV2.class);
    private VecType[] inputTypes;

    private int[] outputLayout;
    private OperatorContext operatorContext;
    private String omniOperatorID;
    private OmniRuntime omniRuntime;
    VecType[] outputTypes;


    private boolean finishing;
    private boolean finished;

    HashAggregationOmniWorkV2 omniWork;

    public HashAggregationOmniOperatorV2(OperatorContext operatorContext, OmniRuntime omniRuntime, String omniOperatorID,  VecType[] inputTypes,  VecType[] outputTypes, int[] outputLayout)
    {
        this.operatorContext = operatorContext;
        this.omniRuntime = omniRuntime;
        this.omniOperatorID = omniOperatorID;
        this.inputTypes = inputTypes;
        this.outputTypes= outputTypes;
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
            omniWork = new HashAggregationOmniWorkV2(page, omniRuntime, omniOperatorID, inputTypes, outputTypes, outputLayout);
        }
        else {
            omniWork.updatePages(page);
        }
        omniWork.process();
//        page.release();
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
            if (omniWork != null && omniWork.isFinished()) {
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
        private List<Symbol> groupBySymbols;
        private ImmutableMap.Builder<Symbol, Integer> outputMappings;
        private AggType[] omniAggregator;
        private int omniTotalChannels;
        private int[] omniGrouByChannels;
        private int[] omniAggregationChannels;
        private List<Type> groupByTypes;
        private List<Symbol> aggregationOutputSymbols;
        private List<AccumulatorFactory> accumulatorFactories;
        private Map<Symbol, AggregationNode.Aggregation> aggregations;
        private List<Integer> groupByChannels;
        int operatorId;
        PlanNodeId planNodeId;
        VecType[] omniGroupByTypes;
        VecType[] omniAggregationTypes;
        VecType[] omniAggReturnTypes;

        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, int omniTotalChannels, int[] omniGrouByChannels, VecType[] omniGroupByTypes, List<Symbol> groupBySymbols, int[] omniAggregationChannels, VecType[] omniAggregationTypes, List<Symbol> aggregationOutputSymbols, AggType[] omniAggregator, VecType[] omniAggReturnTypes, ImmutableMap.Builder<Symbol, Integer> outputMappings)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.omniGroupByTypes = omniGroupByTypes;
            this.groupBySymbols = groupBySymbols;
            this.omniAggregationTypes = omniAggregationTypes;
            this.omniAggReturnTypes = omniAggReturnTypes;
            this.omniTotalChannels = omniTotalChannels;
            this.omniGrouByChannels = omniGrouByChannels;
            this.aggregationOutputSymbols = aggregationOutputSymbols;
            this.omniAggregator = omniAggregator;
            this.omniAggregationChannels = omniAggregationChannels;
            this.outputMappings = outputMappings;
        }

        public HashAggregationOmniOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Integer> groupByChannels, List<Type> groupByTypes, List<Symbol> groupBySymbols, List<Symbol> aggregationOutputSymbols, Map<Symbol, AggregationNode.Aggregation> aggregations, List<AccumulatorFactory> accumulatorFactories, ImmutableMap.Builder<Symbol, Integer> outputMappings)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.groupByChannels = groupByChannels;
            this.groupByTypes = groupByTypes;
            this.groupBySymbols = groupBySymbols;
            this.aggregationOutputSymbols = aggregationOutputSymbols;
            this.aggregations = aggregations;
            this.accumulatorFactories = accumulatorFactories;
            this.outputMappings = outputMappings;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOmniOperatorV2.class.getSimpleName());

            String omniOperatorID = UUID.randomUUID().toString();

            if (omniTotalChannels != 0 && omniGrouByChannels.length != 0 && omniAggregationTypes.length != 0 && omniAggregationChannels.length != 0 && omniAggregationTypes.length != 0 && omniAggregator.length != 0 && omniAggReturnTypes.length != 0) {
                OmniRuntime omniRuntime = new OmniRuntime();
                omniRuntime.prepareAgg(omniOperatorID, omniTotalChannels, omniGrouByChannels, omniGroupByTypes, omniAggregationChannels, omniAggregationTypes, omniAggregator, omniAggReturnTypes);

                List<VecType[]> inAndOutputTypes = getInputVecTypes(omniGrouByChannels, omniGroupByTypes, omniAggregationTypes, omniAggReturnTypes);
                int[] outputLayout = getOutputLayout(omniTotalChannels, omniGrouByChannels, omniAggregationChannels);

                HashAggregationOmniOperatorV2 hashAggregationOperator = new HashAggregationOmniOperatorV2(operatorContext, omniRuntime, omniOperatorID, inAndOutputTypes.get(0),inAndOutputTypes.get(1), outputLayout);
                return hashAggregationOperator;
            }

            int groupBySize = groupByChannels.size();
            int aggregationSize = aggregationOutputSymbols.size();
            int omniTotalChannels = groupBySize + aggregationSize;
            int[] omniGrouByChannels = new int[groupBySize];
            VecType[] omniGroupByTypes = new VecType[groupBySize];
            for (int i = 0; i < groupBySize; i++) {
                omniGrouByChannels[i] = groupByChannels.get(i);
                omniGroupByTypes[i] = toVecType(groupByTypes.get(0).getTypeSignature().getBase());
            }

            int[] omniAggregationChannels = new int[aggregationSize];
            VecType[] omniAggregationTypes = new VecType[aggregationSize];
            AggType[] omniAggregator = new AggType[aggregationSize];
            VecType[] omniAggReturnTypes = new VecType[aggregationSize];
            for (int i = 0; i < aggregationSize; i++) {

                Signature signature = aggregations.get(aggregationOutputSymbols.get(i)).getSignature();

                omniAggregationChannels[i] = accumulatorFactories.get(i).getInputChannels().get(0);

                //aggreagtion types
                omniAggregationTypes[i] = toVecType(signature.getArgumentTypes().get(0).getBase());

                //return types
                omniAggReturnTypes[i] = toVecType(signature.getReturnType().getBase());

                //aggregator type, eg:sum,avg...
                switch (signature.getName()) {
                    case "sum":
                        omniAggregator[i] = AggType.SUM;
                        break;
                    default:
                        throw new UnsupportedOperationException("unsupported Aggregator type by OmniRuntime: " + groupByTypes.get(0).getTypeSignature().getBase());
                }
            }

            OmniRuntime omniRuntime = new OmniRuntime();
            omniRuntime.prepareAgg(omniOperatorID, omniTotalChannels, omniGrouByChannels, omniGroupByTypes, omniAggregationChannels, omniAggregationTypes, omniAggregator, omniAggReturnTypes);

            List<VecType[]> inAndOutputTypes = getInputVecTypes(omniGrouByChannels, omniGroupByTypes, omniAggregationTypes, omniAggReturnTypes);

            int[] outputLayout = getOutputLayout(omniTotalChannels, omniGrouByChannels, omniAggregationChannels);

            HashAggregationOmniOperatorV2 hashAggregationOperator = new HashAggregationOmniOperatorV2(operatorContext, omniRuntime, omniOperatorID,inAndOutputTypes.get(0),inAndOutputTypes.get(1), outputLayout);
            return hashAggregationOperator;
        }

        private List<VecType[]> getInputVecTypes(int[] omniGrouByChannels, VecType[] omniGroupByTypes, VecType[] omniAggregationTypes, VecType[] omniAggReturnTypes)
        {
            List<VecType[]> inAndOutputVecTypes = new ArrayList<>();
            boolean groupByFirst = false;
            for (int omniGrouByChannel : omniGrouByChannels) {
                if (omniGrouByChannel == 0) {
                    groupByFirst = true;
                    break;
                }
            }
            if (groupByFirst) {
                inAndOutputVecTypes.add(ArrayUtils.addAll(omniGroupByTypes, omniAggregationTypes));
                inAndOutputVecTypes.add(ArrayUtils.addAll(omniGroupByTypes, omniAggReturnTypes));
            }
            else {
                inAndOutputVecTypes.add(ArrayUtils.addAll(omniAggregationTypes, omniGroupByTypes));
                inAndOutputVecTypes.add(ArrayUtils.addAll(omniAggReturnTypes, omniGroupByTypes));
            }
            return inAndOutputVecTypes;
        }

        private int[] getOutputLayout(int omniTotalChannels, int[] omniGrouByChannels, int[] omniAggregationChannels)
        {
            ImmutableList<Symbol> symbolImmutableList = outputMappings.build().keySet().asList();
            int[] outputLayout = new int[omniTotalChannels];
            for (int i = 0; i < omniTotalChannels; i++) {
                Symbol symbol = symbolImmutableList.get(i);
                int resultIndex = -1;
                for (int j = 0; j < groupBySymbols.size(); j++) {
                    if (groupBySymbols.get(j).equals(symbol)) {
                        resultIndex = omniGrouByChannels[j];
                        break;
                    }
                }
                if (resultIndex == -1) {
                    for (int j = 0; j < aggregationOutputSymbols.size(); j++) {
                        if (aggregationOutputSymbols.get(j).equals(symbol)) {
                            resultIndex = omniAggregationChannels[j];
                            break;
                        }
                    }
                }

                if (resultIndex == -1) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "HashAgg Omni operator may have wrong output layout");
                }
                outputLayout[i] = resultIndex;
            }
            return outputLayout;
        }

        private VecType toVecType(String signatureBaseType)
        {
            switch (signatureBaseType) {
                case "bigint":
                    return VecType.LONG;
                case "int":
                    return VecType.INT;
                default:
                    throw new UnsupportedOperationException("unsupported omni data type by OmniRuntime: " + signatureBaseType);
            }
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
