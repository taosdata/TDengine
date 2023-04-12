import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LogContentComponent } from './log-content.component';

describe('LogContentComponent', () => {
  let component: LogContentComponent;
  let fixture: ComponentFixture<LogContentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ LogContentComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(LogContentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
