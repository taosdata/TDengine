import { Component, OnInit } from '@angular/core';
import { ExceptionSummary } from '../models/exception-summary';
import { ApiHttpService } from '../services/api-http-service';

@Component({
  selector: 'app-exceptions',
  templateUrl: './exceptions.component.html',
  styleUrls: ['./exceptions.component.css']
})
export class ExceptionsComponent implements OnInit {
  public exceptions: ExceptionSummary[]
  constructor(private apiHttpService: ApiHttpService) {
    this.exceptions = []; 
  }

  ngOnInit(): void {
    this.apiHttpService.getExceptions()
    .subscribe((data: any) => {
      this.exceptions = data
      console.log(data);
    });
  }

}
