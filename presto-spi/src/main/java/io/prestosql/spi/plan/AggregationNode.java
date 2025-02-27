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
package io.prestosql.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

@Immutable
public class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<Symbol, Aggregation> aggregations;
    private final GroupingSetDescriptor groupingSets;
    private final List<Symbol> preGroupedSymbols;
    private final Step step;
    private final Optional<Symbol> hashSymbol;
    private final Optional<Symbol> groupIdSymbol;
    private final List<Symbol> outputs;
    private AggregationType aggregationType;
    private Optional<Symbol> finalizeSymbol;

    @JsonCreator
    public AggregationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("aggregations") Map<Symbol, Aggregation> aggregations,
            @JsonProperty("groupingSets") GroupingSetDescriptor groupingSets,
            @JsonProperty("preGroupedSymbols") List<Symbol> preGroupedSymbols,
            @JsonProperty("step") Step step,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("groupIdSymbol") Optional<Symbol> groupIdSymbol,
            @JsonProperty("aggregationType") AggregationType aggregationType,
            @JsonProperty("finalizeSymbol") Optional<Symbol> finalizeSymbol)
    {
        super(id);

        this.source = source;
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));

        requireNonNull(groupingSets, "groupingSets is null");
        groupIdSymbol.ifPresent(symbol -> checkArgument(groupingSets.getGroupingKeys().contains(symbol), "Grouping columns does not contain groupId column"));
        this.groupingSets = groupingSets;

        this.groupIdSymbol = requireNonNull(groupIdSymbol);

        boolean noOrderBy = aggregations.values().stream()
                .map(Aggregation::getOrderingScheme)
                .noneMatch(Optional::isPresent);
        checkArgument(noOrderBy || step == SINGLE, "ORDER BY does not support distributed aggregation");

        this.step = step;
        this.hashSymbol = hashSymbol;

        requireNonNull(preGroupedSymbols, "preGroupedSymbols is null");
        checkArgument(preGroupedSymbols.isEmpty() || groupingSets.getGroupingKeys().containsAll(preGroupedSymbols), "Pre-grouped symbols must be a subset of the grouping keys");
        this.preGroupedSymbols = ImmutableList.copyOf(preGroupedSymbols);

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        outputs.addAll(groupingSets.getGroupingKeys());
        hashSymbol.ifPresent(outputs::add);
        outputs.addAll(aggregations.keySet());

        if (aggregationType == AggregationType.SORT_BASED) {
            if (step.equals(Step.PARTIAL)) {
                if (finalizeSymbol.isPresent()) {
                    List<Symbol> symbolList = new ArrayList<>(Arrays.asList(finalizeSymbol.get()));
                    outputs.addAll(symbolList);
                }
            }
        }

        this.outputs = outputs.build();
        this.aggregationType = aggregationType;
        this.finalizeSymbol = finalizeSymbol;
    }

    public List<Symbol> getGroupingKeys()
    {
        return groupingSets.getGroupingKeys();
    }

    @JsonProperty("groupingSets")
    public GroupingSetDescriptor getGroupingSets()
    {
        return groupingSets;
    }

    /**
     * @return whether this node should produce default output in case of no input pages.
     * For example for query:
     * <p>
     * SELECT count(*) FROM nation WHERE nationkey < 0
     * <p>
     * A default output of "0" is expected to be produced by FINAL aggregation operator.
     */
    public boolean hasDefaultOutput()
    {
        return hasEmptyGroupingSet() && (step.isOutputPartial() || step.equals(SINGLE));
    }

    public boolean hasEmptyGroupingSet()
    {
        return !groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public boolean hasNonEmptyGroupingSet()
    {
        return groupingSets.getGroupingSetCount() > groupingSets.getGlobalGroupingSets().size();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty
    public Map<Symbol, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty("preGroupedSymbols")
    public List<Symbol> getPreGroupedSymbols()
    {
        return preGroupedSymbols;
    }

    public int getGroupingSetCount()
    {
        return groupingSets.getGroupingSetCount();
    }

    public Set<Integer> getGlobalGroupingSets()
    {
        return groupingSets.getGlobalGroupingSets();
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    @JsonProperty("hashSymbol")
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty("groupIdSymbol")
    public Optional<Symbol> getGroupIdSymbol()
    {
        return groupIdSymbol;
    }

    public boolean hasOrderings()
    {
        return aggregations.values().stream()
                .map(Aggregation::getOrderingScheme)
                .anyMatch(Optional::isPresent);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new AggregationNode(getId(), Iterables.getOnlyElement(newChildren), aggregations, groupingSets, preGroupedSymbols, step, hashSymbol, groupIdSymbol,
                aggregationType, finalizeSymbol);
    }

    public boolean producesDistinctRows()
    {
        return aggregations.isEmpty() &&
                !groupingSets.getGroupingKeys().isEmpty() &&
                outputs.size() == groupingSets.getGroupingKeys().size() &&
                outputs.containsAll(new HashSet<>(groupingSets.getGroupingKeys()));
    }

    public boolean isStreamable()
    {
        return !preGroupedSymbols.isEmpty() && groupingSets.getGroupingSetCount() == 1 && groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public static GroupingSetDescriptor globalAggregation()
    {
        return singleGroupingSet(ImmutableList.of());
    }

    public static GroupingSetDescriptor singleGroupingSet(List<Symbol> groupingKeys)
    {
        Set<Integer> globalGroupingSets;
        if (groupingKeys.isEmpty()) {
            globalGroupingSets = ImmutableSet.of(0);
        }
        else {
            globalGroupingSets = ImmutableSet.of();
        }

        return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
    }

    public static GroupingSetDescriptor groupingSets(List<Symbol> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
    {
        return new GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
    }

    @JsonProperty("aggregationType")
    public AggregationType getAggregationType()
    {
        return aggregationType;
    }

    public void setAggregationType(AggregationType aggregationType)
    {
        this.aggregationType = aggregationType;
    }

    @JsonProperty("finalizeSymbol")
    public Optional<Symbol> getFinalizeSymbol()
    {
        return finalizeSymbol;
    }

    public void setFinalizeSymbol(Optional<Symbol> finalizeSymbol)
    {
        this.finalizeSymbol = finalizeSymbol;
    }

    public static class GroupingSetDescriptor
    {
        private final List<Symbol> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        @JsonCreator
        public GroupingSetDescriptor(
                @JsonProperty("groupingKeys") List<Symbol> groupingKeys,
                @JsonProperty("groupingSetCount") int groupingSetCount,
                @JsonProperty("globalGroupingSets") Set<Integer> globalGroupingSets)
        {
            requireNonNull(globalGroupingSets, "globalGroupingSets is null");
            checkArgument(globalGroupingSets.size() <= groupingSetCount, "list of empty global grouping sets must be no larger than grouping set count");
            requireNonNull(groupingKeys, "groupingKeys is null");
            if (groupingKeys.isEmpty()) {
                checkArgument(!globalGroupingSets.isEmpty(), "no grouping keys implies at least one global grouping set, but none provided");
            }

            this.groupingKeys = ImmutableList.copyOf(groupingKeys);
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = ImmutableSet.copyOf(globalGroupingSets);
        }

        @JsonProperty
        public List<Symbol> getGroupingKeys()
        {
            return groupingKeys;
        }

        @JsonProperty
        public int getGroupingSetCount()
        {
            return groupingSetCount;
        }

        @JsonProperty
        public Set<Integer> getGlobalGroupingSets()
        {
            return globalGroupingSets;
        }
    }

    public enum Step
    {
        PARTIAL(true, true),
        FINAL(false, false),
        INTERMEDIATE(false, true),
        SINGLE(true, false);

        private final boolean inputRaw;
        private final boolean outputPartial;

        Step(boolean inputRaw, boolean outputPartial)
        {
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        public boolean isInputRaw()
        {
            return inputRaw;
        }

        public boolean isOutputPartial()
        {
            return outputPartial;
        }

        public static Step partialOutput(Step step)
        {
            if (step.isInputRaw()) {
                return Step.PARTIAL;
            }
            else {
                return Step.INTERMEDIATE;
            }
        }

        public static Step partialInput(Step step)
        {
            if (step.isOutputPartial()) {
                return Step.INTERMEDIATE;
            }
            else {
                return Step.FINAL;
            }
        }
    }

    public static class Aggregation
    {
        private final CallExpression functionCall;
        private final List<RowExpression> arguments;
        private final boolean distinct;
        private final Optional<Symbol> filter;
        private final Optional<OrderingScheme> orderingScheme;
        private final Optional<Symbol> mask;

        @JsonCreator
        public Aggregation(
                @JsonProperty("call") CallExpression functionCall,
                @JsonProperty("arguments") List<RowExpression> arguments,
                @JsonProperty("distinct") boolean distinct,
                @JsonProperty("filter") Optional<Symbol> filter,
                @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme,
                @JsonProperty("mask") Optional<Symbol> mask)
        {
            this.functionCall = requireNonNull(functionCall, "functionCall is null");
            this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
//            for (RowExpression argument : arguments) {
//                checkArgument(isExpression(argument) && (castToExpression(argument) instanceof SymbolReference || castToExpression(argument) instanceof LambdaExpression),
//                        "argument must be symbol or lambda expression: %s", argument.getClass().getSimpleName());
//            }
            this.distinct = distinct;
            this.filter = requireNonNull(filter, "filter is null");
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
            this.mask = requireNonNull(mask, "mask is null");
        }

        @JsonProperty
        public CallExpression getFunctionCall()
        {
            return functionCall;
        }

        @JsonProperty
        public FunctionHandle getFunctionHandle()
        {
            return functionCall.getFunctionHandle();
        }

        @JsonProperty
        public List<RowExpression> getArguments()
        {
            return arguments;
        }

        @JsonProperty
        public boolean isDistinct()
        {
            return distinct;
        }

        @JsonProperty
        public Optional<Symbol> getFilter()
        {
            return filter;
        }

        @JsonProperty
        public Optional<OrderingScheme> getOrderingScheme()
        {
            return orderingScheme;
        }

        @JsonProperty
        public Optional<Symbol> getMask()
        {
            return mask;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Aggregation that = (Aggregation) o;
            return distinct == that.distinct &&
                    Objects.equals(functionCall, that.functionCall) &&
                    Objects.equals(arguments, that.arguments) &&
                    Objects.equals(filter, that.filter) &&
                    Objects.equals(orderingScheme, that.orderingScheme) &&
                    Objects.equals(mask, that.mask);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionCall, arguments, distinct, filter, orderingScheme, mask);
        }
    }

    public enum AggregationType
    {
        HASH,
        SORT_BASED
    }
}
