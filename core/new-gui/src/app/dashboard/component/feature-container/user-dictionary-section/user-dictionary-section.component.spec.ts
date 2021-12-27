import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserDictionarySectionComponent } from './user-dictionary-section.component';

import { UserDictionaryService } from '../../../../common/service/user/user-dictionary/user-dictionary.service';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { CustomNgMaterialModule } from '../../../../common/custom-ng-material.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('UserDictionarySectionComponent', () => {
  let component: UserDictionarySectionComponent;
  let fixture: ComponentFixture<UserDictionarySectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [UserDictionarySectionComponent],
      providers: [
        UserDictionaryService,
        NgbActiveModal
      ],
      imports: [
        CustomNgMaterialModule,
        NgbModule,
        FormsModule,
        HttpClientTestingModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserDictionarySectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

});
