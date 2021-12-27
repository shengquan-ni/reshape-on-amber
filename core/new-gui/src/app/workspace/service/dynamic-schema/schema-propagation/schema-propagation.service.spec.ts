import { HttpClient } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, inject, TestBed, tick } from '@angular/core/testing';
import { LoggerModule } from 'ngx-logger';
import { environment } from '../../../../../environments/environment';
import { AppSettings } from '../../../../common/app-setting';
import { OperatorPredicate } from '../../../types/workflow-common.interface';
import { JointUIService } from '../../joint-ui/joint-ui.service';
import {
  mockAggregationSchema, mockKeywordSearchSchema, mockNlpSentimentSchema
} from '../../operator-metadata/mock-operator-metadata.data';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';

import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';
import { UndoRedoService } from '../../undo-redo/undo-redo.service';
import { mockPoint, mockScanPredicate, mockScanSentimentLink, mockSentimentPredicate } from '../../workflow-graph/model/mock-workflow-data';
import { WorkflowActionService } from '../../workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../../workflow-graph/util/workflow-util.service';
import { DynamicSchemaService } from '../dynamic-schema.service';
import {
  mockEmptySchemaPropagationResponse, mockSchemaPropagationOperatorID, mockSchemaPropagationResponse
} from './mock-schema-propagation.data';
import { SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS, SCHEMA_PROPAGATION_ENDPOINT, SchemaPropagationService } from './schema-propagation.service';

/* tslint:disable: no-non-null-assertion */
describe('SchemaPropagationService', () => {

  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
        LoggerModule.forRoot(undefined)
      ],
      providers: [
        {provide: OperatorMetadataService, useClass: StubOperatorMetadataService},
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        DynamicSchemaService,
        SchemaPropagationService
      ]
    });

    httpClient = TestBed.inject(HttpClient);
    httpTestingController = TestBed.inject(HttpTestingController);
    environment.schemaPropagationEnabled = true;
  });

  it('should be created', inject([SchemaPropagationService], (service: SchemaPropagationService) => {
    expect(service).toBeTruthy();
  }));

  it('should invoke schema propagation API when a link is added/deleted', fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
    workflowActionService.addLink(mockScanSentimentLink);

    // since resetAttributeOfOperator is called every time when the schema changes, and resetAttributeOfOperator
    //  will change operator property, 2 requests will be sent to auto-complete API endpoint every time
    // EDIT: resetAttributeOfOperator was disabled due to it acting like an independent undo-redo action
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === 'POST');
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1.flush(mockSchemaPropagationResponse);

    // const req2 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // expect(req2.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // req2.flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    workflowActionService.deleteLinkWithID(mockScanSentimentLink.linkID);

    const req3 = httpTestingController.match(
      request => request.method === 'POST'
    );
    expect(req3[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req3[0].flush(mockEmptySchemaPropagationResponse);

    // const req4 = httpTestingController.match(
    //   request => request.method === 'POST'
    // );
    // expect(req4[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // req4[0].flush(mockEmptySchemaPropagationResponse);

    httpTestingController.verify();

  }));

  it('should invoke schema propagation API when a operator property is changed', fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, {tableName: 'test'});

    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method).toEqual('POST');
    req1.flush(mockSchemaPropagationResponse);
    httpTestingController.verify();

  }));

  it('should handle error responses from server gracefully', fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, {tableName: 'test'});
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);

    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method).toEqual('POST');

    // return error response from server
    req1.error(new ErrorEvent('network error'));
    httpTestingController.verify();

    // verify that after the error response, schema propagation service still reacts to events normally
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, {tableName: 'newTable'});
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);

    const req2 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req2.request.method).toEqual('POST');
    req2.flush(mockSchemaPropagationResponse);
    httpTestingController.verify();

  }));

  it('should modify `attribute` of operator schema', fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);

    const mockOperator = {
      ...mockSentimentPredicate,
      operatorID: mockSchemaPropagationOperatorID
    };

    workflowActionService.addOperator(mockOperator, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockOperator.operatorID, {testAttr: 'test'});

    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    // flush mock response
    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === 'POST');
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    req1.flush(mockSchemaPropagationResponse);

    // const req2 = httpTestingController.match(
    //   request => request.method === 'POST'
    // );
    // expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const attributeInSchema = schema.jsonSchema!.properties!['attribute'];
    const expectedEnum = mockSchemaPropagationResponse.result[mockOperator.operatorID][0]?.map(attr => attr.attributeName);

    expect(attributeInSchema).toEqual({
      ...(mockNlpSentimentSchema.jsonSchema.properties!['attribute'] as object),
      enum: expectedEnum,
      uniqueItems: true
    });

  }));

  it('should restore `attribute` to original schema if input attributes no longer exists', fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);

    const mockOperator = {
      ...mockSentimentPredicate,
      operatorID: mockSchemaPropagationOperatorID
    };

    workflowActionService.addOperator(mockOperator, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockOperator.operatorID, {testAttr: 'test'});

    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);

    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === 'POST');
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);

    // flush mock response
    req1.flush(mockSchemaPropagationResponse);

    // const req2 = httpTestingController.match(
    //   request => request.method === 'POST'
    // );
    // expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const attributeInSchema = schema.jsonSchema!.properties!['attribute'];
    const expectedEnum = mockSchemaPropagationResponse.result[mockOperator.operatorID][0]?.map(attr => attr.attributeName);

    expect(attributeInSchema).toEqual({
      ...(mockNlpSentimentSchema.jsonSchema.properties!['attribute'] as object),
      enum: expectedEnum,
      uniqueItems: true
    });

    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockOperator.operatorID, {testAttr: 'test'});

    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req3 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req3.request.method === 'POST');
    expect(req3.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);

    // flush mock response, however, this time response is empty, which means input attrs no longer exists
    req3.flush(mockEmptySchemaPropagationResponse);

    // const req4 = httpTestingController.match(
    //   request => request.method === 'POST'
    // );
    // expect(req4[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // req4[0].flush(mockEmptySchemaPropagationResponse);

    httpTestingController.verify();
    // verify that schema is restored to original value
    const restoredSchema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const restoredAttributeInSchema = restoredSchema.jsonSchema!.properties!['attribute'];
    expect(restoredAttributeInSchema).toEqual({
      ...(mockNlpSentimentSchema.jsonSchema.properties!['attribute'] as object),
      enum: undefined, uniqueItems: undefined
    });

  }));

  it('should modify `attributes` of operator schema', fakeAsync(() => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);

    const mockKeywordSearchOperator: OperatorPredicate = {
      operatorID: mockSchemaPropagationOperatorID,
      operatorType: mockKeywordSearchSchema.operatorType,
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [],
      showAdvanced: true
    };

    workflowActionService.addOperator(mockKeywordSearchOperator, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockKeywordSearchOperator.operatorID, {testAttr: 'test'});

    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);
    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === 'POST');
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // flush mock response
    req1.flush(mockSchemaPropagationResponse);

    // const req2 = httpTestingController.match(
    //   request => request.method === 'POST'
    // );
    // expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const attributeInSchema = schema.jsonSchema!.properties!['attributes'];
    const expectedEnum = mockSchemaPropagationResponse.result[mockKeywordSearchOperator.operatorID][0]?.map(attr => attr.attributeName);

    expect(attributeInSchema).toEqual({
      type: 'array',
      title: 'attributes',
      uniqueItems: true,
      autofill: 'attributeNameList',
      autofillAttributeOnPort: 0,
      items: {
        type: 'string',
        enum: expectedEnum
      }
    });

  }));

  it('should modify nested deep `attribute` of operator schema', fakeAsync(() => {

    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const dynamicSchemaService: DynamicSchemaService = TestBed.inject(DynamicSchemaService);
    const schemaPropagationService: SchemaPropagationService = TestBed.inject(SchemaPropagationService);

    // to match the operator ID of mockSchemaPropagationResponse
    const mockAggregationPredicate: OperatorPredicate = {
      operatorID: mockSchemaPropagationOperatorID,
      operatorType: mockAggregationSchema.operatorType,
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [],
      showAdvanced: true
    };

    workflowActionService.addOperator(mockAggregationPredicate, mockPoint);
    // change operator property to trigger invoking schema propagation API
    workflowActionService.setOperatorProperty(mockAggregationPredicate.operatorID, {testAttr: 'test'});
    tick(SCHEMA_PROPAGATION_DEBOUNCE_TIME_MS);

    const req1 = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    expect(req1.request.method === 'POST');
    expect(req1.request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);

    // flush mock response
    req1.flush(mockSchemaPropagationResponse);

    // const req2 = httpTestingController.match(
    //   request => request.method === 'POST'
    // );
    // expect(req2[0].request.url).toEqual(`${AppSettings.getApiEndpoint()}/${SCHEMA_PROPAGATION_ENDPOINT}`);
    // req2[0].flush(mockSchemaPropagationResponse);

    httpTestingController.verify();

    const schema = dynamicSchemaService.getDynamicSchema(mockSentimentPredicate.operatorID);
    const expectedEnum = mockSchemaPropagationResponse.result[mockAggregationPredicate.operatorID][0]?.map(attr => attr.attributeName);

    expect(schema.jsonSchema!.properties).toEqual({
      listOfAggregations: {
        title: 'list of aggregations',
        type: 'array',
        items: {
          type: 'object',
          properties: {
            attribute: {
              type: 'string',
              title: 'attribute',
              autofill: 'attributeName',
              autofillAttributeOnPort: 0,
              enum: expectedEnum,
              uniqueItems: true
            },
            aggregator: {
              title: 'aggregator',
              type: 'string',
              enum: ['min', 'max', 'average', 'sum', 'count'],
              uniqueItems: true
            },
            resultAttribute: {type: 'string', title: 'result attribute'}
          }
        }
      }
    });

  }));

});
