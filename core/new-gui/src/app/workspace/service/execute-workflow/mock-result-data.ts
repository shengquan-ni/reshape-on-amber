import { SuccessExecutionResult, ErrorExecutionResult, ResultObject } from '../../types/execute-workflow.interface';
import { Point, OperatorPredicate } from '../../types/workflow-common.interface';

export const mockResultData: ResultObject[] = [{
  chartType: undefined,
  operatorID: 'operator-1234',
  totalRowCount: 6,
  table: [
    {
      'id': 1,
      'layer': 'Disk Space and I/O Managers',
      'duty': 'Manage space on disk (pages), including extents',
      'slides': 'slide 2'
    },
    {
      'id': 2,
      'layer': 'Buffer Manager',
      'duty': 'DB-oriented page replacement schemes',
      'slides': 'slide 3'
    },
    {
      'id': 3,
      'layer': 'System Catalog',
      'duty': 'Info about physical data, tables, indexes',
      'slides': 'slides 4 and 5'
    },
    {
      'id': 4,
      'layer': 'Access methods',
      'duty': 'Index structures for access based on field values.',
      'slides': 'B+ tree: slides 6 and 7. Hashing: slide 8. Indexing Performance: slide 9.'
    },
    {
      'id': 5,
      'layer': 'Plan Executor + Relational Operators',
      'duty': 'Runtime side of query processing',
      'slides': 'Sorting: slide 10. Selection+Projection: slide 11. Join: slide 12. Set operations: slide 13.'
    },
    {
      'id': 6,
      'layer': 'Query Optimizer',
      'duty': 'Rewrite query logically. Perform cost-based optimization',
      'slides': 'Cost estimation: slide 14. SystemR Optimizer: slide 15'
    }
  ]
}
];

// execution results for pre-amber engine (deprecated)
export const mockExecutionResult: SuccessExecutionResult = {
  code: 0,
  resultID: '1',
  result: mockResultData
};


export const mockExecutionEmptyResult: SuccessExecutionResult = {
  code: 0,
  resultID: '2',
  result: []
};


export const mockExecutionErrorResult: ErrorExecutionResult = {
  code: 1,
  message: 'custom error happening'
};

export const mockResultOperator: OperatorPredicate = {
  operatorID: mockResultData[0].operatorID,
  operatorType: 'ViewResults',
  operatorProperties: {},
  inputPorts: [],
  outputPorts: [],
  showAdvanced: false
};

export const mockResultPoint: Point = {
  x: 1,
  y: 1
};
