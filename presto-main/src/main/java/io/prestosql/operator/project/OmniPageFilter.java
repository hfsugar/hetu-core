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
package io.prestosql.operator.project;

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.RowExpression;
import nova.hetu.omnicache.runtime.FilterContext;
import nova.hetu.omnicache.runtime.OmniFilter;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.Vec;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class OmniPageFilter
        implements PageFilter
{
    private static final int ITEM_SELECTED_FLAG = 1;
    private final RowExpression filterExpression;
    private final InputChannels inputChannels;
    private final OmniFilter omniFilter;
    private FilterContext omniFilterHandler;

    public OmniPageFilter(RowExpression rowExpression, InputChannels inputChannels, OmniFilter filter, List<Type> inputTypes)
    {
        this.filterExpression = requireNonNull(rowExpression, "filterExpression is null");
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.omniFilter = requireNonNull(filter, "filter is null");

        int[] vecTypes = new int[inputTypes.size()];
        for (int idx = 0; idx < inputTypes.size(); idx++) {
            String type = inputTypes.get(idx).getTypeSignature().getBase();
            switch (type) {
                case "bigint":
                    vecTypes[idx] = 2;
                    break;
                case "integer":
                case "date":
                    vecTypes[idx] = 1;
                    break;
                case "double":
                    vecTypes[idx] = 3;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type for omni filter");
            }
        }
        this.omniFilterHandler = omniFilter.compile(filterExpression.toString(), vecTypes);
        System.out.println("Compile filter");
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return this.inputChannels;
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        // TODO: get types and compile in constructor
        Vec[] inputs = new Vec[page.getChannelCount()];
        for (int idx = 0; idx < page.getChannelCount(); idx++) {
            inputs[idx] = page.getBlock(idx).getVec();
        }
//        if (omniFilterHandler == null) {
//            int[] vecTypes = new int[page.getChannelCount()];
//            for (int idx = 0; idx < page.getChannelCount(); idx++) {
//                vecTypes[idx] = page.getBlock(idx).getVec().getType().getValue();
//            }
//            omniFilterHandler = omniFilter.compile(filterExpression.toString(), vecTypes);
//        }
        //todo: Future we need direct result selected position offset
        IntVec result = omniFilter.execute(omniFilterHandler, inputs, page.getPositionCount());
        return positionsArrayToSelectedPositions(result, page.getPositionCount());
    }

    private SelectedPositions positionsArrayToSelectedPositions(IntVec selectedPositions, int size)
    {
        int selectedCount = 0;
        for (int i = 0; i < size; i++) {
            int selectedPosition = selectedPositions.get(i);
            if (ITEM_SELECTED_FLAG == selectedPosition) {
                selectedCount++;
            }
        }
        if (selectedCount == 0 || selectedCount == size) {
            return SelectedPositions.positionsRange(0, selectedCount);
        }

        int[] positions = new int[selectedCount];
        int index = 0;
        for (int position = 0; position < size; position++) {
            if (ITEM_SELECTED_FLAG == selectedPositions.get(position)) {
                positions[index] = position;
                index++;
            }
        }
        return SelectedPositions.positionsList(positions, 0, selectedCount);
    }
}
