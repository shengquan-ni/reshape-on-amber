import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { CustomNgMaterialModule } from '../../../../../common/custom-ng-material.module';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';

import { FileUploadModule } from 'ng2-file-upload';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { UserFileService } from '../../../../../common/service/user/user-file/user-file.service';
import { NgbdModalFileAddComponent } from './ngbd-modal-file-add.component';
import { UserService } from '../../../../../common/service/user/user.service';
import { UserFileUploadService } from '../../../../../common/service/user/user-file/user-file-upload.service';

describe('NgbdModalFileAddComponent', () => {
  let component: NgbdModalFileAddComponent;
  let fixture: ComponentFixture<NgbdModalFileAddComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalFileAddComponent],
      providers: [
        UserService,
        UserFileService,
        UserFileUploadService,
        NgbActiveModal
      ],
      imports: [
        CustomNgMaterialModule,
        NgbModule,
        FormsModule,
        FileUploadModule,
        ReactiveFormsModule,
        HttpClientTestingModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalFileAddComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
