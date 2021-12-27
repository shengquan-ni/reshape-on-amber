import { JSONSchema4, JSONSchema7 } from 'json-schema';
import { ResultObject } from './execute-workflow.interface';
/**
 * This file contains multiple type declarations related to workflow-graph.
 * These type declarations should be identical to the backend API.
 */

export interface Point extends Readonly<{
  x: number;
  y: number;
}> { }

export interface OperatorPort extends Readonly<{
  operatorID: string;
  portID: string;
}> { }

export interface OperatorPredicate extends Readonly<{
  operatorID: string;
  operatorType: string;
  operatorProperties: Readonly<{[key: string]: any}>;
  inputPorts: {portID: string, displayName?: string}[];
  outputPorts: {portID: string, displayName?: string}[];
  showAdvanced: boolean;
}> { }

export interface OperatorLink extends Readonly<{
  linkID: string;
  source: OperatorPort;
  target: OperatorPort;
}> { }

export interface BreakpointSchema extends Readonly<{
  jsonSchema: Readonly<JSONSchema7>;
}> {}

type ConditionBreakpoint = Readonly<{
  column: number;
  condition: '=' | '>' | '>=' | '<' | '<=' | '!=' | 'contains' | 'does not contain';
  value: string;
}>;

type CountBreakpoint = Readonly<{
  count: number;
}>;

export type Breakpoint = ConditionBreakpoint | CountBreakpoint;

export type BreakpointRequest =
  Readonly<{type: 'ConditionBreakpoint'} & ConditionBreakpoint>
  | Readonly<{type: 'CountBreakpoint'} & CountBreakpoint>;

export type BreakpointFaultedTuple = Readonly<{
  tuple: ReadonlyArray<string>;
  id: number;
  isInput: boolean;
}>;

export type BreakpointFault = Readonly<{
  actorPath: string;
  faultedTuple: BreakpointFaultedTuple;
  messages: ReadonlyArray<string>;
}>;

export type BreakpointTriggerInfo = Readonly<{
  report: ReadonlyArray<BreakpointFault>;
  operatorID: string;
}>;
