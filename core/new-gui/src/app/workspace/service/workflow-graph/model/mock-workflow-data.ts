import { Point, OperatorPredicate, OperatorLink, Breakpoint } from './../../../types/workflow-common.interface';

/**
 * Provides mock data related operators and links:
 *
 * Operators:
 *  - 1: ScanSource
 *  - 2: NlpSentiment
 *  - 3: ViewResults
 *  - 4: MultiInputOutputOperator
 *
 * Links:
 *  - link-1: ScanSource -> ViewResults
 *  - link-2: ScanSource -> NlpSentiment
 *  - link-3: NlpSentiment -> ScanSource
 *
 * Invalid links:
 *  - link-4: (no source port) -> NlpSentiment
 *  - link-5: (NlpSentiment) -> (no target port)
 *
 */

export const mockPoint: Point = {
  x: 100, y: 100
};

export const mockScanPredicate: OperatorPredicate = {
  operatorID: '1',
  operatorType: 'ScanSource',
  operatorProperties: {
  },
  inputPorts: [],
  outputPorts: [{portID: 'output-0'}],
  showAdvanced: true
};

export const mockSentimentPredicate: OperatorPredicate = {
  operatorID: '2',
  operatorType: 'NlpSentiment',
  operatorProperties: {
  },
  inputPorts: [{portID: 'input-0'}],
  outputPorts: [{portID: 'output-0'}],
  showAdvanced: true
};

export const mockResultPredicate: OperatorPredicate = {
  operatorID: '3',
  operatorType: 'ViewResults',
  operatorProperties: {
  },
  inputPorts: [{portID: 'input-0'}],
  outputPorts: [],
  showAdvanced: true
};

export const mockMultiInputOutputPredicate: OperatorPredicate = {
  operatorID: '4',
  operatorType: 'MultiInputOutput',
  operatorProperties: {
  },
  inputPorts: [{portID: 'input-0'}, {portID: 'input-1'}, {portID: 'input-2'}],
  outputPorts: [{portID: 'output-0'}, {portID: 'output-1'}, {portID: 'output-2'}],
  showAdvanced: true
};

export const mockScanResultLink: OperatorLink = {
  linkID: 'link-1',
  source: {
    operatorID: mockScanPredicate.operatorID,
    portID: mockScanPredicate.outputPorts[0].portID
  },
  target: {
    operatorID: mockResultPredicate.operatorID,
    portID: mockResultPredicate.inputPorts[0].portID
  }
};

export const mockScanSentimentLink: OperatorLink = {
  linkID: 'link-2',
  source: {
    operatorID: mockScanPredicate.operatorID,
    portID: mockScanPredicate.outputPorts[0].portID
  },
  target: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.inputPorts[0].portID
  }
};

export const mockSentimentResultLink: OperatorLink = {
  linkID: 'link-3',
  source: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.outputPorts[0].portID
  },
  target: {
    operatorID: mockResultPredicate.operatorID,
    portID: mockResultPredicate.inputPorts[0].portID
  }
};


export const mockFalseResultSentimentLink: OperatorLink = {
  linkID: 'link-4',
  source: {
    operatorID: mockResultPredicate.operatorID,
    portID: undefined as any
  },
  target: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.inputPorts[0].portID
  }
};

export const mockFalseSentimentScanLink: OperatorLink = {
  linkID: 'link-5',
  source: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.outputPorts[0].portID
  },
  target: {
    operatorID: mockScanPredicate.operatorID,
    portID: undefined as any
  }
};
