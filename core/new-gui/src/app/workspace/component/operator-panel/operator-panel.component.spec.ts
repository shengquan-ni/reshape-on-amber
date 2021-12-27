import { mockScanSourceSchema } from './../../service/operator-metadata/mock-operator-metadata.data';
import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
import { DragDropService } from './../../service/drag-drop/drag-drop.service';
import { async, ComponentFixture, TestBed, tick, fakeAsync } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import '../../../common/rxjs-operators';
import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';

import { OperatorPanelComponent } from './operator-panel.component';
import { OperatorLabelComponent } from './operator-label/operator-label.component';
import { OperatorMetadataService, EMPTY_OPERATOR_METADATA } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { TourService } from 'ngx-tour-ng-bootstrap';
import { GroupInfo, OperatorSchema } from '../../types/operator-schema.interface';
import { RouterTestingModule } from '@angular/router/testing';
import { TourNgBootstrapModule } from 'ngx-tour-ng-bootstrap';
import { FormsModule, ReactiveFormsModule} from '@angular/forms';

import {
  mockOperatorMetaData, mockOperatorGroup, mockOperatorSchemaList
} from '../../service/operator-metadata/mock-operator-metadata.data';

import * as c from './operator-panel.component';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';
import { WorkflowUtilService } from '../../service/workflow-graph/util/workflow-util.service';
import { NzDropDownModule } from 'ng-zorro-antd/dropdown';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';


describe('OperatorPanelComponent', () => {
  let component: OperatorPanelComponent;
  let fixture: ComponentFixture<OperatorPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorPanelComponent, OperatorLabelComponent],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        DragDropService,
        WorkflowActionService,
        UndoRedoService,
        WorkflowUtilService,
        JointUIService,
        TourService
      ],
      imports: [
        NzDropDownModule,
        NzCollapseModule,
        BrowserAnimationsModule,
        RouterTestingModule.withRoutes([]),
        TourNgBootstrapModule.forRoot()
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorPanelComponent);
    fixture.detectChanges();
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should sort group names correctly based on order', () => {
    const groups = mockOperatorGroup;

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['Source', 'Analysis', 'View Results']);

  });

  it('should sort group names correctly based on order relatively. ex: (100, 1) -> (1, 100)', () => {
    const groups: GroupInfo[] = [
      { groupName: 'group_1', groupOrder: 1 },
      { groupName: 'group_2', groupOrder: 100 }
    ];

    const result = c.getGroupNamesSorted(groups);

    expect(result).toEqual(['group_1', 'group_2']);

  });

  it('should sort group names correctly from an empty list', () => {
    const groups: GroupInfo[] = [];
    const result = c.getGroupNamesSorted(groups);
    expect(result).toEqual([]);

  });

  it('should generate a map from operator groups to a list operators correctly', () => {
    const opMetadata = mockOperatorMetaData;

    const result = c.getOperatorGroupMap(opMetadata);

    const sourceOperators = opMetadata.operators.filter(op => op.additionalMetadata.operatorGroupName === 'Source');
    const analysisOperators = opMetadata.operators.filter(op => op.additionalMetadata.operatorGroupName === 'Analysis');
    const resultOperators = opMetadata.operators.filter(op => op.additionalMetadata.operatorGroupName === 'View Results');

    const expectedResult = new Map<string, OperatorSchema[]>();
    expectedResult.set('Source', sourceOperators);
    expectedResult.set('Analysis', analysisOperators);
    expectedResult.set('View Results', resultOperators);

    expect(result).toEqual(expectedResult);

  });

  it('should generate a map from operator groups to a list operators correctly from an empty list', () => {
    const opMetadata = EMPTY_OPERATOR_METADATA;
    const result = c.getOperatorGroupMap(opMetadata);
    const expectedResult = new Map<string, OperatorSchema[]>();

    expect(result).toEqual(expectedResult);

  });

  it('should receive operator metadata from service', () => {
    // if the length of our schema list is equal to the length of mock data
    // we assume the mock data has been received
    expect(component.operatorSchemaList.length).toEqual(mockOperatorSchemaList.length);
    expect(component.groupNamesOrdered.length).toEqual(mockOperatorGroup.length);
  });

  it('should have all group names shown in the UI side panel', () => {
    const groupNamesInUI = fixture.debugElement
    .queryAll(By.css('.texera-workspace-operator-panel-subgroup'))
    .map(el => (el.nativeElement.querySelector('.ant-collapse-header').innerText));

    expect(groupNamesInUI).toEqual(
      mockOperatorGroup.map(group => group.groupName));
  });

  it('should create child operator label component for all operators', () => {
    const operatorLabels = fixture.debugElement
      .queryAll(By.directive(OperatorLabelComponent))
      .map(debugEl => <OperatorLabelComponent>debugEl.componentInstance)
      .map(operatorLabel => operatorLabel.operator);

    expect(operatorLabels.length).toEqual(mockOperatorMetaData.operators.length);
  });

  it('should search an operator by its user friendly name', () => {
    let searchResults: OperatorSchema[] = [];
    component.operatorSearchResults.subscribe(res => searchResults = res);

    component.operatorSearchFormControl.setValue('Source: Scan');

    expect(searchResults.length === 1);
    expect(searchResults[0] === mockScanSourceSchema);
    fixture.detectChanges();
  });

  it('should support fuzzy search on operator user friendly name', () => {
    let searchResults: OperatorSchema[] = [];
    component.operatorSearchResults.subscribe(res => searchResults = res);

    component.operatorSearchFormControl.setValue('scan');

    expect(searchResults.length === 1);
    expect(searchResults[0] === mockScanSourceSchema);
  });

  it('should clear the search box when an operator from search box is dropped', () => {
    component.operatorSearchFormControl.setValue('scan');
    expect(component.operatorSearchFormControl.value).toEqual('scan');

    const dragDropService = TestBed.get(DragDropService);
    dragDropService.operatorDroppedSubject.next({
      operatorType: 'ScanSource',
      offset: {x: 1, y: 1},
      dragElementID: OperatorLabelComponent.operatorLabelSearchBoxPrefix + 'ScanSource'
    });

    expect(component.operatorSearchFormControl.value).toBeFalsy();
  });

});

