import { Component, OnInit } from '@angular/core';
import { ApiHttpService } from '../services/api-http-service';

@Component({
  selector: 'app-logs',
  templateUrl: './logs.component.html',
  styleUrls: ['./logs.component.css']
})
export class LogsComponent implements OnInit {
  public logFiles: string[] | null;
  constructor(private apiHttpService: ApiHttpService) {
    this.logFiles = null; 
  }

  ngOnInit(): void {
    this.apiHttpService.getLogFiles()
    .subscribe((data: any) => {
      this.logFiles = data
      console.log(data);
    });
  }

}
